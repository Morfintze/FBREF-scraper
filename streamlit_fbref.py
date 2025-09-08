import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
from functools import reduce
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Session met retry strategie
def create_session():
    """Maak een requests session met retry strategie en connection pooling."""
    session = requests.Session()
    
    # Retry strategie
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Headers die een echte browser nabootsen
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    
    return session

def smart_delay(request_count=0, base_delay=2):
    """Intelligente delay tussen requests."""
    # Exponentiële backoff voor meerdere requests
    if request_count > 0:
        delay = base_delay * (1.5 ** min(request_count, 5))
    else:
        delay = base_delay
    
    # Voeg wat randomness toe (human-like behavior)
    delay += random.uniform(0.5, 1.5)
    
    time.sleep(delay)

@st.cache_data(ttl=3600)  # Cache voor 1 uur
def cached_get_page(url, session=None):
    """Cached versie van het ophalen van pagina's."""
    if session is None:
        session = create_session()
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        st.warning(f"⚠️ Request error voor {url}: {str(e)}")
        return None

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

def remove_duplicate_columns(df):
    """Verwijder duplicate kolommen uit de DataFrame."""
    # Zoek duplicate kolomnamen
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    
    if duplicate_cols:
        st.info(f"ℹ️ {len(duplicate_cols)} duplicate kolommen gevonden en verwijderd")
        
        # Behoud alleen de eerste instantie van elke duplicate kolom
        df = df.loc[:, ~df.columns.duplicated()]
    
    return df

def scrape_fbref_team_season(team_url, delay_between_requests=3, selected_tabs=None):
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

    all_tabs = {
        "shooting": "🎯 Shooting",
        "passing": "📌 Passing", 
        "passing_types": "📊 Pass Types",
        "defense": "🛡️ Defense",
        "possession": "⚽ Possession",
        "misc": "📈 Miscellaneous",
        "keeper": "🧤 Goalkeeper",
        "keeper_adv": "🧤 GK Advanced"
    }
    
    # Gebruik geselecteerde tabs of alle tabs
    if selected_tabs is None:
        tabs_to_scrape = list(all_tabs.keys())
    else:
        tabs_to_scrape = selected_tabs
    
    merge_keys = ["Date", "Comp", "Opponent", "Venue", "Result"]
    dfs = []

    # Maak een session die hergebruikt wordt
    session = create_session()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, tab in enumerate(tabs_to_scrape, 1):
        status_text.text(f"📥 Scraping {all_tabs.get(tab, tab)}... ({i}/{len(tabs_to_scrape)})")
        url = f"{base_url}{tab}/{team_name}-Match-Logs-All-Competitions"
        
        try:
            # Gebruik cached GET
            html_content = cached_get_page(url, session)
            if html_content is None:
                st.warning(f"⚠️ Kon pagina niet ophalen voor {tab}, skip...")
                progress_bar.progress(i / len(tabs_to_scrape))
                continue
            
            soup = BeautifulSoup(html_content, "html.parser")
            table = soup.find("table")
            if table is None:
                st.warning(f"⚠️ Geen tabel gevonden voor {tab}, skip...")
                progress_bar.progress(i / len(tabs_to_scrape))
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
            st.success(f"✅ {tab} succesvol gescraped")
            
            # Delay tussen requests (behalve laatste)
            if i < len(tabs_to_scrape):
                smart_delay(i-1, delay_between_requests)
            
        except Exception as e:
            st.warning(f"⚠️ Fout bij scrapen van {tab}: {str(e)}")
        
        progress_bar.progress(i / len(tabs_to_scrape))

    status_text.empty()
    progress_bar.empty()

    if not dfs:
        st.error("❌ Geen data gevonden voor dit team/seizoen.")
        return None

    # Merge alle tabbladen op merge_keys
    df_final = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how="outer"), dfs)
    df_final = df_final.sort_values("Date", ascending=True)
    
    # Verwijder duplicate kolommen
    df_final = remove_duplicate_columns(df_final)

    return df_final

# ---------------- Streamlit UI ----------------
st.title("⚽ FBref Team Scraper (Rate Limit Friendly)")

st.info("ℹ️ **Tips voor minder rate limiting:**\n"
        "• Selecteer alleen de data categorieën die je nodig hebt\n"
        "• Gebruik langere delays tussen requests\n"
        "• Data wordt 1 uur gecached om herhaalde requests te voorkomen")

# URL input
url = st.text_input("👉 Plak hier de FBref team-URL:", 
                   placeholder="Bijv: https://fbref.com/en/squads/.../matchlogs/...")

# Tab selectie
st.subheader("📊 Selecteer data categorieën:")
col1, col2 = st.columns(2)

tab_options = {
    "shooting": "🎯 Shooting Stats",
    "passing": "📌 Passing Stats", 
    "passing_types": "📊 Pass Types",
    "defense": "🛡️ Defense Stats",
    "possession": "⚽ Possession Stats",
    "misc": "📈 Miscellaneous Stats",
    "keeper": "🧤 Goalkeeper Basic",
    "keeper_adv": "🧤 Goalkeeper Advanced"
}

with col1:
    selected_basic = []
    if st.checkbox("🎯 Shooting Stats", value=True):
        selected_basic.append("shooting")
    if st.checkbox("📌 Passing Stats", value=True):
        selected_basic.append("passing")
    if st.checkbox("📊 Pass Types"):
        selected_basic.append("passing_types")
    if st.checkbox("🛡️ Defense Stats", value=True):
        selected_basic.append("defense")

with col2:
    selected_advanced = []
    if st.checkbox("⚽ Possession Stats"):
        selected_advanced.append("possession")
    if st.checkbox("📈 Miscellaneous Stats"):
        selected_basic.append("misc")
    if st.checkbox("🧤 Goalkeeper Basic"):
        selected_advanced.append("keeper")
    if st.checkbox("🧤 Goalkeeper Advanced"):
        selected_advanced.append("keeper_adv")

selected_tabs = selected_basic + selected_advanced

# Delay instelling
delay = st.slider("⏱️ Delay tussen requests (seconden):", 
                 min_value=1, max_value=10, value=3,
                 help="Hogere waarde = minder kans op rate limiting, maar langzamer")

if st.button("🚀 Scrape team data", disabled=len(selected_tabs) == 0):
    if not url.strip():
        st.warning("⚠️ Geef eerst een geldige FBref-URL in.")
    elif len(selected_tabs) == 0:
        st.warning("⚠️ Selecteer minstens één data categorie.")
    else:
        estimated_time = len(selected_tabs) * delay
        st.info(f"⏱️ Geschatte tijd: ~{estimated_time} seconden voor {len(selected_tabs)} categorieën")
        
        with st.spinner("Bezig met scrapen..."):
            df = scrape_fbref_team_season(url, delay, selected_tabs)

        if df is not None:
            st.success(f"✅ {len(df)} wedstrijden succesvol gescraped!")
            
            # Toon voorbeeld van de data
            st.subheader("📋 Voorbeeld van de data:")
            
            # Controleer op duplicate kolommen voordat we weergeven
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()]
            
            try:
                st.dataframe(df.head(10))
                
                # Data info
                st.subheader("📊 Data overzicht:")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Aantal wedstrijden", len(df))
                with col2:
                    st.metric("Aantal kolommen", len(df.columns))
                with col3:
                    st.metric("Data categorieën", len(selected_tabs))
                
            except Exception as e:
                st.error(f"❌ Fout bij weergeven van data: {str(e)}")
                st.write("Kolomnamen in de data:")
                st.write(list(df.columns))
            
            # Download knop
            csv_data = df.to_csv(index=False).encode("utf-8")
            team_name = url.split("/")[-1].replace("-Match-Logs-All-Competitions", "")
            
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_data,
                file_name=f"{team_name}_season_stats.csv",
                mime="text/csv"
            )

# Tips sectie
with st.expander("💡 Tips voor optimaal gebruik"):
    st.markdown("""
    **Rate Limiting Vermijden:**
    - Selecteer alleen de data die je echt nodig hebt
    - Gebruik delays van 3-5 seconden tussen requests
    - Scrape niet te vaak achter elkaar
    - Data wordt automatisch gecached voor 1 uur
    
    **Best Practices:**
    - Test eerst met één team voordat je meerdere teams scrapet  
    - Download en bewaar je data lokaal
    - Gebruik de data respectvol volgens FBref's terms of service
    """)
