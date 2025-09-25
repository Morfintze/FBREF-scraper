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
import base64
from urllib.parse import urljoin, urlparse
import json

# Pool van realistische User-Agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36'
]

def get_random_headers():
    """Genereer realistische, randomized headers."""
    user_agent = random.choice(USER_AGENTS)
    
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': random.choice([
            'en-US,en;q=0.9',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.9,nl;q=0.8',
        ]),
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    # Voeg soms extra headers toe
    if random.choice([True, False]):
        headers['Pragma'] = 'no-cache'
    
    return headers

def create_enhanced_session():
    """Maak een geavanceerde session met anti-detection maatregelen."""
    session = requests.Session()
    
    # Retry strategie - minder agressief
    retry_strategy = Retry(
        total=2,  # Minder retries om niet verdacht te worden
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set initial headers
    session.headers.update(get_random_headers())
    
    return session

def smart_delay(request_count=0, base_delay=5):
    """Nog intelligentere delay met meer variatie."""
    # Exponentiële backoff met meer randomness
    if request_count > 0:
        delay = base_delay * (1.8 ** min(request_count, 4))
    else:
        delay = base_delay
    
    # Veel meer randomness - lijkt menselijker
    delay += random.uniform(2, 6)
    
    # Soms extra lange pauzes (alsof je de pagina aan het lezen bent)
    if random.random() < 0.3:  # 30% kans
        delay += random.uniform(5, 15)
    
    # Laat user weten wat er gebeurt
    if delay > 10:
        st.info(f"⏸️ Lange pauze van {delay:.1f}s voor natuurlijker gedrag...")
    
    time.sleep(delay)

def make_human_like_request(session, url, max_attempts=3):
    """Maak een request die meer menselijk lijkt."""
    for attempt in range(max_attempts):
        try:
            # Update headers voor elke request
            session.headers.update(get_random_headers())
            
            # Voeg referer toe (alsof je van FBref's homepage komt)
            if attempt == 0:
                session.headers['Referer'] = 'https://fbref.com/'
            else:
                session.headers['Referer'] = 'https://fbref.com/en/'
            
            response = session.get(url, timeout=30, allow_redirects=True)
            
            if response.status_code == 403:
                st.warning(f"⚠️ 403 Forbidden - Poging {attempt + 1}/{max_attempts}")
                if attempt < max_attempts - 1:
                    # Wacht langer bij 403
                    wait_time = random.uniform(10, 30)
                    st.info(f"⏳ Wacht {wait_time:.1f}s voordat we het opnieuw proberen...")
                    time.sleep(wait_time)
                continue
            elif response.status_code == 429:
                st.warning("⚠️ Rate limit bereikt - langere pauze...")
                time.sleep(random.uniform(30, 60))
                continue
            
            response.raise_for_status()
            return response.text
            
        except requests.exceptions.RequestException as e:
            st.warning(f"⚠️ Request fout (poging {attempt + 1}): {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(random.uniform(5, 15))
    
    return None

@st.cache_data(ttl=3600)  # Cache voor 1 uur
def cached_get_page_enhanced(url):
    """Enhanced cached versie van het ophalen van pagina's."""
    session = create_enhanced_session()
    return make_human_like_request(session, url)

def check_robots_txt():
    """Check FBref's robots.txt voor scraping regels."""
    try:
        session = create_enhanced_session()
        response = session.get("https://fbref.com/robots.txt", timeout=10)
        if response.status_code == 200:
            return response.text
    except:
        pass
    return None

def validate_url_access(url):
    """Test of een URL toegankelijk is voordat we beginnen."""
    st.info("🔍 Testen van URL toegankelijkheid...")
    
    session = create_enhanced_session()
    html_content = make_human_like_request(session, url)
    
    if html_content is None:
        return False, "Kan URL niet bereiken"
    
    if "Access Denied" in html_content or "403 Forbidden" in html_content:
        return False, "Toegang geweigerd door server"
    
    if len(html_content) < 1000:  # Te kort voor een echte pagina
        return False, "Onverwacht kort response"
    
    return True, "URL is toegankelijk"

# Rest van je functies blijven hetzelfde...
def simplify_column_name(col_name, tab_name):
    """Vereenvoudig kolomnamen door overbodige prefixes en suffixes te verwijderen."""
    if 'For Chicago Fire' in col_name:
        col_name = col_name.replace('For Chicago Fire_', '')
    
    if f'_{tab_name}' in col_name:
        col_name = col_name.replace(f'_{tab_name}', '')
    
    if 'Unnamed' in col_name or 'Match Report' in col_name:
        return None
    
    prefixes = ['Standard_', 'Expected_', 'Performance_', 'Pass Types_', 
                'Corner Kicks_', 'Outcomes_', 'Tackles_', 'Challenges_', 
                'Blocks_', 'Take-Ons_', 'Carries_', 'Receiving_', 'Aerial Duels_',
                'Penalty Kicks_', 'Launched_', 'Passes_', 'Goal Kicks_', 'Crosses_',
                'Sweeper_']
    
    for prefix in prefixes:
        if col_name.startswith(prefix):
            col_name = col_name.replace(prefix, '')
    
    col_name = col_name.replace(' ', '_').replace('/', '_').replace('%', 'Pct').replace('-', '_')
    return col_name

def remove_duplicate_columns(df):
    """Verwijder duplicate kolommen uit de DataFrame."""
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    
    if duplicate_cols:
        st.info(f"ℹ️ {len(duplicate_cols)} duplicate kolommen gevonden en verwijderd")
        df = df.loc[:, ~df.columns.duplicated()]
    
    return df

def scrape_fbref_team_season_enhanced(team_url, delay_between_requests=8, selected_tabs=None):
    # URL validatie
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

    # Test toegankelijkheid eerst
    test_url = f"{base_url}shooting/{team_name}-Match-Logs-All-Competitions"
    is_accessible, message = validate_url_access(test_url)
    
    if not is_accessible:
        st.error(f"❌ URL niet toegankelijk: {message}")
        return None
    else:
        st.success(f"✅ {message}")

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
    
    if selected_tabs is None:
        tabs_to_scrape = list(all_tabs.keys())
    else:
        tabs_to_scrape = selected_tabs
    
    merge_keys = ["Date", "Comp", "Opponent", "Venue", "Result"]
    dfs = []

    # Maak een session die hergebruikt wordt
    session = create_enhanced_session()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, tab in enumerate(tabs_to_scrape, 1):
        status_text.text(f"📥 Scraping {all_tabs.get(tab, tab)}... ({i}/{len(tabs_to_scrape)})")
        url = f"{base_url}{tab}/{team_name}-Match-Logs-All-Competitions"
        
        try:
            # Gebruik enhanced cached GET
            html_content = cached_get_page_enhanced(url)
            if html_content is None:
                st.warning(f"⚠️ Kon pagina niet ophalen voor {tab}, skip...")
                progress_bar.progress(i / len(tabs_to_scrape))
                continue
            
            # Check for Cloudflare challenge page
            if "Checking your browser" in html_content or "cf-browser-verification" in html_content:
                st.error("🚫 Cloudflare detectie - scraping geblokkeerd")
                return None
            
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
                if simple_col:
                    new_columns.append(simple_col)
                else:
                    columns_to_drop.append(col)
            
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

            # Maak kolommen uniek per tabblad
            columns_to_rename = [c for c in df.columns if c not in merge_keys]
            df = df.rename(columns={c: f"{c}_{tab}" for c in columns_to_rename})

            dfs.append(df)
            st.success(f"✅ {tab} succesvol gescraped")
            
            # Langere delay tussen requests
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

    df_final = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how="outer"), dfs)
    df_final = df_final.sort_values("Date", ascending=True)
    df_final = remove_duplicate_columns(df_final)

    return df_final

# ---------------- Streamlit UI ----------------
st.title("⚽ Enhanced FBref Scraper (Anti-Block Version)")

st.warning("⚠️ **Anti-Block Maatregelen Actief:**\n"
          "• Gebruik van rotating user agents\n"
          "• Langere delays en natuurlijker gedrag\n"
          "• URL toegankelijkheid wordt eerst getest\n"
          "• Maximum respect voor server limits")

# Toon robots.txt info
with st.expander("🤖 FBref Robots.txt Info"):
    robots_content = check_robots_txt()
    if robots_content:
        st.code(robots_content[:500] + "..." if len(robots_content) > 500 else robots_content)
    else:
        st.info("Kan robots.txt niet ophalen")

# URL input
url = st.text_input("👉 Plak hier de FBref team-URL:", 
                   placeholder="Bijv: https://fbref.com/en/squads/.../matchlogs/...")

# Tab selectie
st.subheader("📊 Selecteer data categorieën:")
col1, col2 = st.columns(2)

with col1:
    selected_basic = []
    if st.checkbox("🎯 Shooting Stats", value=True):
        selected_basic.append("shooting")
    if st.checkbox("📌 Passing Stats", value=False):  # Minder default selections
        selected_basic.append("passing")
    if st.checkbox("📊 Pass Types"):
        selected_basic.append("passing_types")
    if st.checkbox("🛡️ Defense Stats"):
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

# Delay instelling - hogere minimum
delay = st.slider("⏱️ Delay tussen requests (seconden):", 
                 min_value=5, max_value=20, value=10,  # Hogere defaults
                 help="Hogere waarde = veel minder kans op blokkering")

if st.button("🚀 Scrape team data", disabled=len(selected_tabs) == 0):
    if not url.strip():
        st.warning("⚠️ Geef eerst een geldige FBref-URL in.")
    elif len(selected_tabs) == 0:
        st.warning("⚠️ Selecteer minstens één data categorie.")
    else:
        estimated_time = len(selected_tabs) * delay * 2  # Realistischere schatting
        st.info(f"⏱️ Geschatte tijd: ~{estimated_time//60}m {estimated_time%60}s voor {len(selected_tabs)} categorieën")
        
        with st.spinner("Bezig met enhanced scraping..."):
            df = scrape_fbref_team_season_enhanced(url, delay, selected_tabs)

        if df is not None:
            st.success(f"✅ {len(df)} wedstrijden succesvol gescraped!")
            
            # Toon voorbeeld van de data
            st.subheader("📋 Voorbeeld van de data:")
            
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

            # Auto-download
            b64 = base64.b64encode(csv_data).decode('ascii')
            download_js = f"""
            <script>
            (function() {{
              var b64 = "{b64}";
              var link = document.createElement('a');
              link.href = 'data:text/csv;base64,' + b64;
              link.download = "{team_name}_season_stats.csv";
              link.style.display = 'none';
              document.body.appendChild(link);
              setTimeout(function() {{
                  link.click();
                  document.body.removeChild(link);
              }}, 700);
            }})();
            </script>
            """
            st.markdown(download_js, unsafe_allow_html=True)

# Tips sectie
with st.expander("💡 Anti-Block Tips & Troubleshooting"):
    st.markdown("""
    **Als je nog steeds 403 errors krijgt:**
    - 🔄 Probeer een andere internetconnectie (mobiele hotspot)
    - 🕰️ Wacht een paar uur en probeer later opnieuw
    - 🎯 Selecteer maar 1-2 data categorieën tegelijk
    - ⏰ Verhoog de delay naar 15-20 seconden
    - 🌍 Gebruik een VPN met een andere locatie
    
    **Best Practices:**
    - ✅ Test eerst met één klein team
    - 💾 Sla je data lokaal op na elke succesvolle run
    - 📅 Scrape niet meerdere teams op dezelfde dag
    - 🤝 Respecteer FBref's servers - zij bieden gratis data
    
    **Als niks werkt:**
    - FBref heeft mogelijk hun blocking verhoogd
    - Overweeg de FBref API (indien beschikbaar)
    - Of gebruik selenium-based scraping voor complexere anti-detection
    """)

st.info("🔧 **Nieuwe Features in deze versie:**\n"
        "• Rotating User-Agents voor betere camouflage\n"
        "• URL toegankelijkheidstest vooraf\n"
        "• Cloudflare detectie\n"
        "• Meer natuurlijke request patterns\n"
        "• Langere, meer variabele delays")
