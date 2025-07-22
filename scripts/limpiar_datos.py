import os
import pandas as pd
from pathlib import Path

# Carpeta de origen y destino
FOLDER_ORIGEN = Path("data")
FOLDER_DESTINO = Path("data/limpios")
FOLDER_DESTINO.mkdir(parents=True, exist_ok=True)

SUBCARPETAS = ["fbref", "fbref_defense", "understat", "mercado"]

# Columnas clave a conservar por carpeta
COLUMNAS_UTILES = {
    "fbref": [
        "Unnamed: 1_level_0_Player", "Unnamed: 3_level_0_Pos", "Unnamed: 6_level_0_Born", "Playing Time_MP", "Playing Time_Starts", "Playing Time_Min", "Performance_Gls", "Performance_Ast", "Performance_CrdY", "Performance_CrdR", "Progression_PrgC","Progression_PrgP","Progression_PrgR"
    ],
    "fbref_defense": [
       "Unnamed: 1_level_0_Player", "Tackles_Def 3rd", "Tackles_Mid 3rd","Tackles_Att 3rd", "Challenges_Tkl%", "Tackles_Tkl", "Tackles_TklW", "Int", "Blocks_Blocks", "Blocks_Sh", "Blocks_Pass", "Clr", "Err"
    ],
    "understat": [
        "nombre", "xG", "xA", "npxG", "shsots", "key_passes", "xGChain", "xGBuildup"
    ],
    "mercado": [
        "player_name", "current_club", "player_height_mtrs", "player_market_value_euro"
    ]
}

# Estandarizar nombres y columnas comunes
def estandarizar_columnas(df, carpeta):
    if carpeta == "fbref":
        df = df.rename(columns={"Unnamed: 1_level_0_Player": "nombre", "Unnamed: 3_level_0_Pos": "Posicion", "Unnamed: 6_level_0_Born": "nacido", "Playing Time_Min": "minutos_jugados", "Performance_Gls": "goles", "Performance_Ast": "asistencias", "Playing Time_MP": "partidos_jugados", "Playing Time_Starts": "titularidades", "Performance_CrdY": "amarillas", "Performance_CrdR": "rojas", "Progression_PrgC": "carreras_progresivas","Progression_PrgP": "pases_progresivos","Progression_PrgR": "Pases_progresivos_recibidos"})
    elif carpeta == "fbref_defense":
        df = df.rename(columns={"Unnamed: 1_level_0_Player": "nombre", "Tackles_Def 3rd": "entradas_1/3", "Tackles_Mid 3rd": "entradas_2/3","Tackles_Att 3rd": "entradas_3/3" , "Challenges_Tkl%": "duelos_ganados", "Tackles_Tkl": "entradas", "Tackles_TklW": "entradas_ganadas", "Int": "intercepciones", "Blocks_Blocks": "bloqueos", "Blocks_Sh": "bloqueos_tiro", "Blocks_Pass": "bloqueos_pase", "Clr": "despejes", "Err": "errores_defensivos"})
    elif carpeta == "understat":
        df = df.rename(columns={"xG": "goles_esperados", "xA": "asistencias_esperadas", "shots": "remates", "npg": "goles_noPen", "npxG": "goles_esperados_noPen"})
    elif carpeta == "mercado":
        df = df.rename(columns={ "player_name": "nombre", "current_club": "club_actual", "player_height_mtrs": "altura", "player_market_value_euro": "valor"})
    if "nombre" in df.columns:
        df["nombre"] = df["nombre"].astype(str).str.lower().str.strip()
    if "club_actual" in df.columns:
        df["club_actual"] = df["club_actual"].astype(str).str.strip()
    return df

def aniadir_col(carpeta, archivo, df):
    nombre = archivo.stem
    partes = nombre.split("_")
    
    if carpeta == "mercado":
        df["temporada"] = partes[3]
        df["liga"] = partes[2]
    else:
        temporada = f"{partes[1]}-{partes[2]}"
        df["temporada"] = temporada
        df["liga"] = partes[0]
    return df

def aplanar_columnas(df):
    df = df[df[('Unnamed: 1_level_0', 'Player')] != 'Player']
    df.columns = ['_'.join(filter(None, col)).strip() for col in df.columns.values]
    return df

def limpiar():  
    for carpeta in SUBCARPETAS:
        dfs=[]
        carpeta_origen = FOLDER_ORIGEN / carpeta
        carpeta_destino = FOLDER_DESTINO / carpeta
        carpeta_destino.mkdir(parents=True, exist_ok=True)
        for archivo in carpeta_origen.glob("*.csv"):
            try:
                print(f"Procesando {archivo}")
                try:
                    if carpeta == "fbref" or carpeta == "fbref_defense":
                        df = pd.read_csv(archivo, header=[0, 1])
                        df = aplanar_columnas(df)
                    else:
                        df = pd.read_csv(archivo)
                except Exception:
                    df = pd.read_excel(archivo, engine="openpyxl")

                columnas_utiles = COLUMNAS_UTILES.get(carpeta, df.columns.tolist())
                columnas_disponibles = [col for col in columnas_utiles if col in df.columns]
                df = df[columnas_disponibles]
                df = df.loc[:, ~df.columns.duplicated()]
                df = estandarizar_columnas(df, carpeta)
                df = aniadir_col(carpeta, archivo, df)
                dfs.append(df)
                salida = carpeta_destino / archivo.name
                df.to_csv(salida, index=False)
                print(f"Guardado en {salida}")
            except Exception as e:
                print(f"Error con {archivo}: {e}")
        if dfs:
            archivo_salida = carpeta_destino / f"{carpeta}.csv"
            combinado = pd.concat(dfs, ignore_index=True)
            combinado.to_csv(archivo_salida, index=False)
            print(f"Guardado en: {archivo_salida} ({len(combinado)} filas)")
        else:
            print(f"No se encontraron archivos válidos en {carpeta_origen}")
limpiar()