import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import os

def get_understat_data(league: str, season: str) -> pd.DataFrame:
    url = f"https://understat.com/league/{league}/{season}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'lxml')

    # Buscar el script con los datos de jugadores
    script = soup.find("script", string=re.compile("playersData"))
    if not script:
        raise ValueError("No se encontró el script con playersData")

    # Extraer el JSON usando una expresión más flexible
    json_data = re.search(r"playersData\s*=\s*JSON\.parse\('(.*?)'\);", script.string, re.DOTALL)
    if not json_data:
        raise ValueError("No se pudo extraer el JSON embebido.")

    # Decodificar y cargar JSON
    cleaned_json = json_data.group(1).encode('utf-8').decode('unicode_escape')
    players_data = json.loads(cleaned_json)

    # Normalizar
    df = pd.json_normalize(players_data)

    # Renombrar algunas columnas si existen
    rename_map = {
        'player_name': 'nombre',
        'team_title': 'club_actual',
        'time': 'minutos_jugados'
    }
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    return df
