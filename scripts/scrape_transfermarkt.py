import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_club_urls(league_url):
    response = requests.get(league_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    
    base_url = "https://www.transfermarkt.es"
    team_urls = []

    team_table = soup.find("table", class_="items")
    if team_table:
        for a in team_table.select("td.hauptlink a.vereinprofil_tooltip"):
            href = a.get("href")
            if href and "/startseite/verein/" in href:
                team_url = base_url + href.split("?")[0]
                team_urls.append(team_url)
    return list(set(team_urls))  # eliminar duplicados

def get_players_from_team(team_url):
    print(f"Scrapeando: {team_url}")
    team_url = team_url.replace("/startseite/", "/kader/")
    
    response = requests.get(team_url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")

    team_name = soup.select_one("h1[data-test='headline']")
    team_name = team_name.text.strip() if team_name else "Desconocido"

    players = []
    table = soup.find("table", class_="items")
    if not table:
        return players

    for row in table.select("tbody tr"):
        name_tag = row.select_one("td.posrela a.spielprofil_tooltip")
        value_tag = row.select_one("td.rechts.hauptlink")

        if name_tag and value_tag:
            players.append({
                "Nombre": name_tag.text.strip(),
                "Valor mercado": value_tag.text.strip(),
                "Equipo": team_name
            })

    print(f"→ {len(players)} jugadores encontrados en {team_name}")
    return players

def scrape_league_market_values(league_url):
    all_players = []
    team_urls = get_club_urls(league_url)
    print(f"🟢 Equipos encontrados: {len(team_urls)}")
    
    for url in team_urls:
        try:
            players = get_players_from_team(url)
            all_players.extend(players)
            time.sleep(1.5)
        except Exception as e:
            print(f"Error en {url}: {e}")
            continue
    
    return pd.DataFrame(all_players)

# URL de LaLiga
laliga_url = "https://www.transfermarkt.es/laliga/startseite/wettbewerb/ES1"
df = scrape_league_market_values(laliga_url)

# Guardar
if not df.empty:
    df.to_csv("jugadores_laliga_valores.csv", index=False)
    print("✅ CSV guardado como 'jugadores_laliga_valores.csv'")
else:
    print("⚠️ No se extrajeron datos.")
