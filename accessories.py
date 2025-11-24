import pandas as pd
from sqlalchemy import create_engine
import networkx as nx
import matplotlib.pyplot as plt
import os
import config   # <-- vse občutljive podatke vzamemo iz config.py

# --- Baza iz config.py ---
ime_uporabnika = config.ime_uporabnika
geslo = config.geslo
ip_naslov = config.ip_naslov
port = config.port
ime_baze = config.ime_baze

izhodna_mapa = config.izhodna_mapa

os.makedirs(izhodna_mapa, exist_ok=True)

# --- Povezava do baze ---
engine = create_engine(
    f"mysql+mysqlconnector://{ime_uporabnika}:{geslo}@{ip_naslov}:{port}/{ime_baze}"
)

print("\nPovezano z bazo:", ime_baze)

# --- Preberi vse tabele ---
tables_df = pd.read_sql("SHOW TABLES;", engine)
tables = tables_df.iloc[:, 0].tolist()

print("Najdene tabele:", tables)

dfs = {}
relations = []

# --- Preberi vse tabele ---
for tabela in tables:
    print(f"\n>>> Obdelujem tabelo: {tabela}")

    df = pd.read_sql(f"SELECT * FROM {tabela};", engine)
    dfs[tabela] = df

    # --- Izvoz podatkov ---
    df.to_excel(os.path.join(izhodna_mapa, f"{tabela}_export.xlsx"), index=False)

    # --- Statistika ---
    stat = df.describe(include="all")
    stat.to_excel(os.path.join(izhodna_mapa, f"{tabela}_statistics.xlsx"))

    # --- Manjkajoče vrednosti ---
    missing = df.isnull().sum().reset_index()
    missing.columns = ["column", "missing"]
    missing.to_excel(os.path.join(izhodna_mapa, f"{tabela}_missing.xlsx"), index=False)

    # Iskanje možnih relacij
    for col in df.columns:
        if col.endswith("_id") or col == "id":
            relations.append((tabela, col))

print("\n✔ Osnovna analiza končana za vse tabele.\n")

# --- ERD ---
print("Gradim ERD...")

G = nx.DiGraph()

for t, col in relations:
    for t2, df2 in dfs.items():
        if col in df2.columns and t != t2:
            G.add_edge(t, t2, label=col)

plt.figure(figsize=(12, 10))
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, with_labels=True, node_size=2700, font_size=10, arrowsize=18)

plt.title("ERD – Povezave med tabelami")
plt.tight_layout()
plt.savefig(os.path.join(izhodna_mapa, "ERD_graf.png"))
plt.show()

print("✔ ERD ustvarjen.\n")

# --- Analiza accessories ---
if "accessories" in dfs:
    print("Delam analizo za accessories...")

    df = dfs["accessories"].copy()

    # Pretvorba numeric values
    df['regular_price'] = pd.to_numeric(df['regular_price'], errors='coerce')
    df['reduced_price'] = pd.to_numeric(df['reduced_price'], errors='coerce')

    # Popust
    df['discount'] = df['regular_price'] - df['reduced_price']
    df['discount_percent'] = (df['discount'] / df['regular_price']) * 100

    # Izvoz
    df.to_excel(os.path.join(izhodna_mapa, "accessories_analysis.xlsx"), index=False)

    # Graf
    plt.figure(figsize=(10, 6))
    plt.bar(df['name'], df['discount_percent'])
    plt.xticks(rotation=45)
    plt.ylabel("Popust (%)")
    plt.title("Popusti po izdelkih – Accessories")
    plt.tight_layout()
    plt.savefig(os.path.join(izhodna_mapa, "accessories_popusti.png"))
    plt.show()

    print("✔ Analiza za accessories končana.\n")
else:
    print("Tabela 'accessories' ne obstaja, preskakujem analizo.\n")

# --- Izvoz accessories za PowerBI ---
powerbi_excel = os.path.join(izhodna_mapa, "powerbi_accessories.xlsx")

print("Izvažam samo ACCESSORIES tabelo v Excel datoteko...")

with pd.ExcelWriter(powerbi_excel, engine='openpyxl') as writer:
    if "accessories" in dfs:
        df = dfs["accessories"]
        df.to_excel(writer, sheet_name="accessories", index=False)
    else:
        print("Tabela 'accessories' ne obstaja, ne morem izvoziti.")

print("powerbi_accessories.xlsx je ustvarjen")
print("tabela 'accessories' je v Excel")

print("==========================================")
print(" analiza je uspešno narejena ")
print(" Rezultati so shranjeni v:", izhodna_mapa)
print("==========================================")

