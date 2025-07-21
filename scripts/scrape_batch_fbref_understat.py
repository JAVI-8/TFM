
import os
from scrape_fbref_defense import build_defense_url, extract_fbref_defensive_table
from scrape_fbref_multi import get_fbref_tables_from_url, build_fbref_url
from scrape_understat import get_understat_data
from limpiar_datos import limpiar
# Definir ligas y temporadas
ligas = [
    {"name": "Premier League", "slug": "premier-league", "code": 9, "understat": "EPL"},
    {"name": "La Liga", "slug": "laliga", "code": 12, "understat": "La_liga"},
    {"name": "Serie A", "slug": "serie-a", "code": 11, "understat": "Serie_A"},
    {"name": "Bundesliga", "slug": "bundesliga", "code": 20, "understat": "Bundesliga"}
]

temporadas = ["2022-2023", "2023-2024", "2024-2025"]

# Tablas de FBref
table_ids = ["stats_standard"]

#carpetas para guardar
os.makedirs("data/fbref", exist_ok=True)
os.makedirs("data/understat", exist_ok=True)

#Recorrer
for liga in ligas:
    for temporada in temporadas:
        print(f"\nProcesando: {liga['name']} - {temporada}")

        # --- FBref ---
        try:
            fbref_url = build_fbref_url(liga["code"], temporada, liga["name"])
            df_fbref = get_fbref_tables_from_url(fbref_url, table_ids)
            nombre_archivo_fbref = f"data/fbref/{liga['slug']}_{temporada.replace('-', '_')}.csv"
            df_fbref.to_csv(nombre_archivo_fbref, index=False)
            print(f"FBref guardado correctamente en {nombre_archivo_fbref}")
        except Exception as e:
            print(f"FBref error en {liga['name']} {temporada}: {e}")
        #defensive stats
        try:
            fbref_D_url = build_defense_url(liga["code"], temporada, liga["name"])
            df_fbref_D = extract_fbref_defensive_table(fbref_D_url)
            nombre_archivo_D_fbref = f"data/fbref_defense/{liga['slug']}_{temporada.replace('-', '_')}_defense.csv"
            df_fbref_D.to_csv(nombre_archivo_D_fbref, index=False)
            print(f"FBref defensive guardado correctamente en {nombre_archivo_D_fbref}")
        except Exception as e:
            print(f"FBref defensive error en {liga['name']} {temporada}: {e}")
        
        # --- Understat --- (requiere temporada en año simple)
        try:
            año_understat = temporada.split("-")[0]
            df_understat = get_understat_data(liga["understat"], año_understat)
            nombre_archivo_understat = f"data/understat/{liga['slug']}_{temporada.replace('-', '_')}.csv"
            df_understat.to_csv(nombre_archivo_understat, index=False)
            print(f"Understat guardado correctamente en {nombre_archivo_understat}")
        except Exception as e:
            print(f"Understat error en {liga['name']} {temporada}: {e}")

limpiar()