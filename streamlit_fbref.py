import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
from functools import reduce

def simplify_column_name(col_name, tab_name):
    """Vereenvoudig kolomnamen door overbodige prefixes en suffixes te verwijderen."""
    # Verwijder teamnaam prefix
    if 'For Chicago Fire' in col_name:
        col_name = col_name.replace('For Chicago Fire_', '')
    
    # Verwijder tabblad suffix
    if f'_{tab_name}' in col_name:
        col_name = col_name.replace(f'_{tab_name}', '')
    
    # Verwijder "Unnamed" en "Match Report" kolommen
    if 'Unnamed' in col_name or 'Match Report' in col_name:
        return None
    
    # Verwijder overige onnodige prefixes
    prefixes = ['Standard_', 'Expected_', 'Performance_', 'Pass Types_', 
                'Corner Kicks_', 'Outcomes_', 'Tackles_', 'Challenges_', 
                'Blocks_', 'Take-Ons_', 'Carries_', 'Receiving_', 'Aerial Duels_',
                'Penalty Kicks_', 'Launched_', 'Passes_', 'Goal Kicks_', 'Crosses_',
                'Sweeper_']
    
    for prefix in prefixes:
        if col_name.startswith(prefix):
            col_name = col_name.replace(prefix, '')
    
    # Vervang spaties en speciale karakters
    col_name = col_name.replace(' ', '_').replace('/', '_').replace('%', 'Pct').replace('-', '_')
    
    return col_name

def scrape_fbref_team_season(team_url):
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

    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, tab in enumerate(tabs, 1):
        status_text.text(f"📥 Scraping {tab}... ({i}/{len(tabs)})")
        url = f"{base_url}{tab}/{team_name}-Match-Logs-All-Competitions"
        
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()
            
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table")
            if table is None:
                st.warning(f"⚠️ Geen tabel gevonden voor {tab}, skip...")
                progress_bar.progress(i / len(tabs))
                continue

            df = pd.read_html(StringIO(str(table)))[0]

            # Flatten MultiIndex kolommen
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join([str(c) for c in col if c]).strip("_") for col in df.columns]

            # Vereenvoudig kolomnamen
            new_columns = []
            columns_to_drop = []
            for col in df.columns:
                simple_col = simplify_column_name(col, tab)
                if simple_col:  # Alleen toevoegen als niet None
                    new_columns.append(simple_col)
                else:
                    columns_to_drop.append(col)
            
            # Verwijder ongewenste kolommen
            df = df.drop(columns=columns_to_drop)
            df.columns = new_columns

            # Zorg dat merge_keys bestaan
            for key in merge_keys:
                if key not in df.columns:
                    df[key] = None

            # Parse datum
            date_col = [c for c in df.columns if "Date" in c]
            if date_col:
                df["Date"] = pd.to_datetime(df[date_col[0]], errors="coerce")
            else:
                df["Date"] = pd.NaT

            # Maak kolommen uniek per tabblad (alleen voor niet-merge_keys)
            columns_to_rename = [c for c in df.columns if c not in merge_keys]
            df = df.rename(columns={c: f"{c}_{tab}" for c in columns_to_rename})

            dfs.append(df)
            
        except Exception as e:
            st.warning(f"⚠️ Fout bij scrapen van {tab}: {str(e)}")
        
        progress_bar.progress(i / len(tabs))

    status_text.empty()
    progress_bar.empty()

    if not dfs:
        st.error("❌ Geen data gevonden voor dit team/seizoen.")
        return None

    # Merge alle tabbladen op merge_keys
    df_final = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how="outer"), dfs)
    df_final = df_final.sort_values("Date", ascending=True)

    return df_final

# ---------------- Streamlit UI ----------------
st.title("⚽ FBref Team Scraper")

url = st.text_input("👉 Plak hier de FBref team-URL:", 
                   placeholder="Bijv: https://fbref.com/en/squads/.../matchlogs/...")

if st.button("Scrape team data"):
    if url.strip():
        with st.spinner("Bezig met scrapen..."):
            df = scrape_fbref_team_season(url)

        if df is not None:
            st.success(f"✅ {len(df)} wedstrijden succesvol gescraped!")
            
            # Toon voorbeeld van de data
            st.subheader("Voorbeeld van de data:")
            st.dataframe(df.head(10))
            
            # Download knop
            csv_data = df.to_csv(index=False).encode("utf-8")
            team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "")
            
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_data,
                file_name=f"{team_name}_season_stats.csv",
                mime="text/csv"
            )
    else:
        st.warning("⚠️ Geef eerst een geldige FBref-URL in.")
