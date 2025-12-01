import pandas as pd
from sqlalchemy import create_engine
import config
import os

# Parametri iz config.py 
ime_uporabnika = config.ime_uporabnika
geslo = config.geslo
ip_naslov = config.ip_naslov
port = config.port
ime_baze = config.ime_baze
izhodna_mapa = config.izhodna_mapa  

connection_string = f"mysql+pymysql://{ime_uporabnika}:{geslo}@{ip_naslov}:{port}/{ime_baze}"
engine = create_engine(connection_string)

print(f"Povezano z bazo: {ime_baze}")

query = "SELECT * FROM customers"
df = pd.read_sql(query, engine)

print("Naložena tabela customers. Vrstice:", len(df))

os.makedirs(izhodna_mapa, exist_ok=True)
output_file = os.path.join(izhodna_mapa, "customers_export.xlsx")

df.to_excel(output_file, index=False)

print(f"Izvožen Excel: {output_file}")
print("Končano")
