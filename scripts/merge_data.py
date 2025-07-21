import os
import pandas as pd

# Carpeta de datos
FOLDER_FBREF = "data/fbref"
FOLDER_UNDERSTAT = "data/understat"
FOLDER_SALIDA = "data/unificados"
os.makedirs(FOLDER_SALIDA, exist_ok=True)

# Columnas clave para intentar el merge
CLAVE = ["nombre", "club_actual"]

# Recorremos todos los archivos disponibles
for archivo_fbref in os.listdir(FOLDER_FBREF):
    if not archivo_fbref.endswith(".csv"):
        continue

    temporada_id = archivo_fbref.replace(".csv", "")  # ejemplo: premier-league_2021_2022
    liga, año = temporada_id.rsplit("_", 1)

    archivo_understat = f"{liga}_{año}.csv"
    ruta_fbref = os.path.join(FOLDER_FBREF, archivo_fbref)
    ruta_understat = os.path.join(FOLDER_UNDERSTAT, archivo_understat)

    if not os.path.exists(ruta_understat):
        print(f"❌ No se encontró understat para {temporada_id}")
        continue

    try:
        df_fbref = pd.read_csv(ruta_fbref)
        df_understat = pd.read_csv(ruta_understat)

        # Preprocesamiento básico de nombres (ajustable)
        df_fbref['nombre'] = df_fbref['Player'].str.strip().str.lower()
        df_fbref['club_actual'] = df_fbref.get('Squad', '')

        df_understat['nombre'] = df_understat['nombre'].str.strip().str.lower()
        df_understat['club_actual'] = df_understat['club_actual'].str.strip()

        # Unificación
        df_merged = pd.merge(df_fbref, df_understat, on=CLAVE, how='inner', suffixes=('_fbref', '_understat'))

        salida = os.path.join(FOLDER_SALIDA, f"{liga}_{año}_merged.csv")
        df_merged.to_csv(salida, index=False)
        print(f"✅ Unificado: {salida} - {len(df_merged)} jugadores")

    except Exception as e:
        print(f"❌ Error unificando {temporada_id}: {e}")
