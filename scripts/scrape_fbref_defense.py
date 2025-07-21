import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import os
from io import StringIO

def extract_fbref_defensive_table(url: str) -> pd.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extraer tablas ocultas en comentarios HTML
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if "stats_defense" in comment:
            soup.append(BeautifulSoup(comment, "html.parser"))

    table = soup.find("table", id="stats_defense")
    if not table:
        raise ValueError("No se encontró la tabla 'stats_defense'.")

    df = pd.read_html(StringIO(str(table)))[0]

    # Si existe 'Rk', filtrar filas válidas
    if 'Rk' in df.columns:
        df = df[df['Rk'].apply(lambda x: str(x).isdigit())]  # eliminar encabezados duplicados
        df = df.drop(columns=['Rk'], errors='ignore')

    return df.reset_index(drop=True)

def build_defense_url(competition_id: int, season_label: str, league_name: str) -> str:
    slug = league_name.lower().replace(" ", "-")
    return f"https://fbref.com/en/comps/{competition_id}/{season_label}/defense/{season_label}-{slug}-Stats"