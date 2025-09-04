import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
from functools import reduce
import os


SAVE_PATH = "/Users/morfje/Documents/MLMORF"

def scrape_fbref_team_season(team_url, output_csv):
    # Regex om de basis van de URL te valideren en info uit te halen
    match = re.search(
        r"/squads/([a-z0-9]+)/([\d-]+)/matchlogs/all_comps/[a-z_]+/([^/]+-Match-Logs-All-Competitions)",
        team_url
    )
    if not match:
        st.error("❌ Ongeldige URL. Zorg dat je een FBref 'match logs' URL gebruikt.")
        return None

    team_id, season, team_name_file = match.groups()
    team_name = team_name_file.replace("-Match-Logs-All-Competitions", "")
    base_url = f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/"

    tabs = ["shooting", "passing", "passing_types", "defense", "possession", "misc", "keeper", "keeper_adv"]
    merge_keys = ["Date", "Comp", "Opponent", "Venue", "Result"]
    dfs = []

    progress = st.progress(0)
    for i, tab in enumerate(tabs, 1):
        url = f"{base_url}{tab}/{team_name}-Match-Logs-All-Competitions"
        st.write(f"📥 Scraping {tab}...")
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table")
        if table is None:
            st.warning(f"⚠️ Geen tabel gevonden voor {tab}, skip...")
            progress.progress(i / len(tabs))
            continue

        df = pd.read_html(StringIO(str(table)))[0]

        # Flatten MultiIndex kolommen
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(c) for c in col if c]).strip("_") for col in df.columns]

        # Zorg dat merge_keys bestaan
        for key in merge_keys:
            if key not in df.columns:
                df[key] = None

        # Parse datum
        date_col = [c for c in df.columns if "Date" in c][0]
        df["Date"] = pd.to_datetime(df[date_col], errors="coerce")

        # Maak kolommen uniek per tabblad
        df = df.rename(columns={c: f"{c}_{tab}" for c in df.columns if c not in merge_keys})

        dfs.append(df)
        progress.progress(i / len(tabs))

    if not dfs:
        st.error("❌ Geen data gevonden voor dit team/seizoen.")
        return None

    # Merge alle tabbladen op merge_keys
    df_final = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how="outer"), dfs)
    df_final = df_final.sort_values("Date", ascending=True)

    # Zorg dat de map bestaat
    os.makedirs(SAVE_PATH, exist_ok=True)

    # CSV opslaan
    df_final.to_csv(output_csv, index=False)
    st.success(f"✅ Data van het hele seizoen opgeslagen in {output_csv}")
    return df_final


# ---------------- Streamlit UI ----------------
st.title("⚽ FBref Team Scraper")

url = st.text_input("👉 Plak hier de FBref team-URL:")

if st.button("Scrape team data"):
    if url.strip():
        team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "")
        output_file = os.path.join(SAVE_PATH, f"{team_name}_season_stats.csv")
        df = scrape_fbref_team_season(url, output_file)

        if df is not None:
            st.dataframe(df.head(20))  # toon eerste 20 rijen
            st.download_button(
                "⬇️ Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"{team_name}_season_stats.csv",
                mime="text/csv"
            )
    else:
        st.warning("⚠️ Geef eerst een geldige FBref-URL in.")
