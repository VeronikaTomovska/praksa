import pandas as pd
from sqlalchemy import create_engine
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter
import itertools
import re
import os
import config  # <-- vse občutljive podatke vzamemo iz config.py

# --- Parametri iz config.py ---
ime_uporabnika = config.ime_uporabnika
geslo = config.geslo
ip_naslov = config.ip_naslov
port = config.port
ime_baze = config.ime_baze
izhodna_mapa = config.izhodna_mapa

# Ustvari izhodno mapo, če ne obstaja
os.makedirs(izhodna_mapa, exist_ok=True)

# --- Povezava do baze ---
povezovalni_niz = f'mysql+mysqlconnector://{ime_uporabnika}:{geslo}@{ip_naslov}:{port}/{ime_baze}'
engine = create_engine(povezovalni_niz)

print(f"\nPovezano z bazo: {ime_baze}")

# --- Nalaganje podatkov ---
sql_query = "SELECT * FROM abandoned_cart_mails;"
df = pd.read_sql(sql_query, engine)
engine.dispose()

print("Tabela naložena. Stolpci:", list(df.columns))
print("Število vrstic:", len(df))

# --- Izvoz celotne tabele ---
df.to_excel(os.path.join(izhodna_mapa, "abandoned_cart_mails_export.xlsx"), index=False)
print("✔ Izvožen Excel: abandoned_cart_mails_export.xlsx")

# --- ČASOVNA ANALIZA ---
# Pretvorba Unix timestamp -> datetime
df['time'] = pd.to_datetime(df['time'], unit='s', errors='coerce')

# Število e-mailov na dan
daily_counts = df.groupby(df['time'].dt.date).size().reset_index(name='count')

# Izvoz časovnega trenda
daily_counts.to_excel(os.path.join(izhodna_mapa, "mail_trend.xlsx"), index=False)
print("✔ Izvožen Excel: mail_trend.xlsx (časovni trend)")

# --- RELACIJE NA GRAF ---
possible_pairs = [
    ('sender', 'recipient'),
    ('from_email', 'to_email'),
    ('email_from', 'email_to'),
    ('user_id', 'product_id'),
    ('customer_id', 'product_id'),
    ('customer_email', 'product_sku')
]

found_pair = None
for a, b in possible_pairs:
    if a in df.columns and b in df.columns:
        found_pair = (a, b)
        break

# --- IZGRADNJA GRAFA ---
if found_pair is not None:
    a, b = found_pair
    print(f"Uporabili bomo relacijski par: {a} -> {b}")
    edges_df = df[[a, b]].dropna().astype(str)
    G = nx.from_pandas_edgelist(edges_df, a, b, create_using=nx.DiGraph())
    graph_type = 'edge_list_relational'

else:
    print("Relacijskega para NI — delamo co-occurrence graf iz subject + title.")

    if 'text' not in df.columns:
        df['text'] = df['subject'].fillna('') + " " + df['title'].fillna('')

    def tokenize(s):
        s = str(s).lower()
        s = re.sub(r'[^a-z0-9\s]', ' ', s)
        return [w for w in s.split() if len(w) > 2]

    pair_counts = Counter()

    for doc in df['text']:
        toks = list(dict.fromkeys(tokenize(doc)))
        for u, v in itertools.combinations(sorted(toks), 2):
            pair_counts[(u, v)] += 1

    # Filter najmočnejših povezav
    edges_list = [(u, v, w) for (u, v), w in pair_counts.items() if w >= 2]
    edges_df = pd.DataFrame(edges_list, columns=['source', 'target', 'weight'])

    G = nx.Graph()
    for _, r in edges_df.iterrows():
        G.add_edge(r['source'], r['target'], weight=r['weight'])

    graph_type = 'cooccurrence'

print("Število vozlišč v grafu:", G.number_of_nodes())
print("Število robov v grafu:", G.number_of_edges())

# --- CENTRALNOST ---
deg_cent = nx.degree_centrality(G)

if G.number_of_nodes() <= 2000:
    btw_cent = nx.betweenness_centrality(G)
else:
    btw_cent = {n: 0 for n in G.nodes()}

# --- IZVOZ VOZLIŠČ ---
nodes_out = pd.DataFrame([
    (n, deg_cent.get(n, 0), btw_cent.get(n, 0)) for n in G.nodes()
], columns=['node', 'degree_centrality', 'betweenness']).sort_values('degree_centrality', ascending=False)

nodes_out.head(50).to_excel(os.path.join(izhodna_mapa, "graf_top_nodes.xlsx"), index=False)
print("✔ Izvožen Excel: graf_top_nodes.xlsx")

# --- IZVOZ ROBOV ---
edges_df.to_excel(os.path.join(izhodna_mapa, "graf_edges.xlsx"), index=False)
print("✔ Izvožen Excel: graf_edges.xlsx")

# --- VIZUALIZACIJA GRAFA ---
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G, k=0.2, seed=42)

node_sizes = [3000 * deg_cent.get(n, 0.001) for n in G.nodes()]
edge_widths = [G[u][v].get('weight', 1) for u, v in G.edges()]

nx.draw_networkx_nodes(G, pos, node_size=node_sizes, alpha=0.85)
nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.6)
nx.draw_networkx_labels(G, pos, font_size=8)

plt.title(f"Graf analiza – {graph_type}")
plt.axis('off')
plt.tight_layout()
plt.savefig(os.path.join(izhodna_mapa, "graf_vizualizacija.png"))
plt.show()

print("\n==========================================")
print("Analiza je uspesno narejena ")
print(f" Rezultati so shranjeni v: {izhodna_mapa}")
print("=")

