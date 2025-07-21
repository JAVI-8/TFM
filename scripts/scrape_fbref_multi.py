import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import os
from io import StringIO

def build_fbref_url(competition_id: int, season_label: str, league_name: str) -> str:
    slug = league_name.lower().replace(" ", "-")
    return f"https://fbref.com/en/comps/{competition_id}/{season_label}/stats/players/{season_label}-{slug}-Stats"

def get_fbref_tables_from_url(url: str, table_ids: list) -> pd.DataFrame:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extraer y parsear comentarios HTML que puedan contener tablas ocultas
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if any(tid in comment for tid in table_ids):
            soup.append(BeautifulSoup(comment, 'html.parser'))

    tables = {}
    for table_id in table_ids:
        table = soup.find('table', {'id': table_id})
        if table:
            df = pd.read_html(StringIO(str(table)))[0]  # evitar warning de pandas
            if 'Rk' in df.columns:
                df = df[df['Rk'].apply(lambda x: str(x).isdigit())]  # Eliminar filas de encabezados repetidos
                df = df.drop(columns=['Rk'], errors='ignore')
            tables[table_id] = df.set_index('Player') if 'Player' in df.columns else df
        else:
            print(f"Tabla '{table_id}' no encontrada.")

    if not tables:
        raise ValueError("No se encontró ninguna de las tablas especificadas. Verifica la URL y los IDs de tabla.")

    # Unir tablas por jugador (índice Player)
    combined_df = pd.concat(tables.values(), axis=1, join='outer')
    combined_df = combined_df.reset_index()
    return combined_df

