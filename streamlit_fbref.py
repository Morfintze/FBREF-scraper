import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
from functools import reduce
import os

# Maak een submap voor opslag
SAVE_PATH = "data"
os.makedirs(SAVE_PATH, exist_ok=True)


@st.cache_data
def scrape_fbref_team_season(team_url):
    # Regex om de basis van de URL te valideren en info uit te halen
    match = re.search(
        r"/squads/([a-z0-9]+)/([\d-]+)/matchlogs/all_comps/[a-z_]+/([^/]+-Match-Logs-All-Competitions)",
        team_url,
    )
    if not match:
        return None, "❌ Ongeldige URL. Zorg dat je een FBref 'match logs' URL gebruikt."

    team_id, season, team_name_file = match.groups()
    team_name = team_name_file.replace("-Match-Logs-All-Competitions", "")
    base_url = f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/"

    tabs = [
        "shooting",
        "passing",
        "passing_types",
        "defense",
        "possession",
        "misc",
        "keeper",
        "keeper_adv",
    ]
    merge_keys = ["Date", "Comp", "Opponent", "Venue", "Result"]
    dfs = []

    for tab in tabs:
        url = f"{base_url}{tab}/{team_name}-Match-Logs-All-Competitions"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table")
        if table is None:
            continue

        df = pd.read_html(StringIO(str(table)))[0]

        # Flatten MultiIndex kolommen
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(c) for c in col if c]).strip("_") for col in df.columns
            ]

        # Zorg dat merge_keys bestaan
        for key in merge_keys:
            if key not in df.columns:
                df[key] = None

        # Parse datum
        date_col = [c for c in df.columns if "Date" in c][0]
        df["Date"] = pd.to_datetime(df[date_col], errors="coerce")

        # Maak kolommen uniek per tabblad
        df = df.rename(
            columns={c: f"{c}_{tab}" for c in df.columns if c not in merge_keys}
        )

        dfs.append(df)

    if not dfs:
        return None, "❌ Geen data gevonden voor dit team/seizoen."

    # Merge alle tabbladen op merge_keys
    df_final = reduce(
        lambda left, right: pd.merge(left, right, on=merge_keys, how="outer"), dfs
    )
    df_final = df_final.sort_values("Date", ascending=True)

    # CSV-bestand maken
    output_file = os.path.join(SAVE_PATH, f"{team_name}_season_stats.csv")
    df_final.to_csv(output_file, index=False)

    return df_final, output_file


# ---------------- Streamlit UI ----------------
st.title("⚽ FBref Team Scraper")

url = st.text_input("👉 Plak hier de FBref team-URL:")

if st.button("Scrape team data"):
    if url.strip():
        df, file_path_or_msg = scrape_fbref_team_season(url)

        if df is not None:
            st.success("✅ Data succesvol opgehaald!")
            st.dataframe(df.head(20))

            # Download knop
            st.download_button(
                "⬇️ Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=os.path.basename(file_path_or_msg),
                mime="text/csv",
            )

            # Link naar requirements.txt
            st.markdown(
                "[📄 requirements.txt bekijken](https://github.com/<jouw-gebruikersnaam>/fbref-scraper/blob/main/requirements.txt)"
            )

        else:
            st.error(file_path_or_msg)
    else:
        st.warning("⚠️ Geef eerst een geldige FBref-URL in.")
