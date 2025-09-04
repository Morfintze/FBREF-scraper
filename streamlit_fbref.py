# save as fbref_scraper_streamlit.py
import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
import re
from io import StringIO
from functools import reduce
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SAVE_PATH = "/Users/morfje/Documents/MLMORF"

st.set_page_config(page_title="FBref Team Scraper", layout="wide")

def make_session(retries=3, backoff=0.3, status_forcelist=(500,502,504)):
    s = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff,
                  status_forcelist=status_forcelist, allowed_methods=frozenset(['GET','POST']))
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    s.headers.update({'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100 Safari/537.36'})
    return s

def find_table_in_soup(soup, tab_hint=None):
    """Probeer tabel te vinden:
       1) echte <table>
       2) tabel in HTML comment blocks (FBref)
       kies tabel waarvan id/content matcht op tab_hint indien mogelijk.
    """
    # 1) directe tables
    tables = soup.find_all('table')
    if tables:
        if tab_hint:
            for t in tables:
                tid = t.get('id','') or ''
                if tab_hint in tid or tab_hint in repr(t)[:200]:
                    return t
        return tables[0]

    # 2) zoek in comments
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for c in comments:
        if '<table' not in c:
            continue
        try:
            comment_soup = BeautifulSoup(c, 'html.parser')
            t = comment_soup.find('table')
            if t:
                if tab_hint:
                    tid = t.get('id','') or ''
                    if tab_hint in tid or tab_hint in str(t)[:400]:
                        return t
                else:
                    return t
        except Exception:
            continue
    return None

def flatten_multicol(df):
    """Flatten MultiIndex headers (pandas.read_html can make multiindex)."""
    if isinstance(df.columns, pd.MultiIndex):
        cols = []
        for col in df.columns:
            parts = [str(p).strip() for p in col if p and str(p).strip()!='']
            cols.append("_".join(parts))
        df.columns = cols
    # make column names sane (no weird whitespace)
    df.columns = [re.sub(r'\s+', '_', c).strip() for c in df.columns]
    return df

def safe_parse_date_column(df):
    # find a column containing 'date' (case-insensitive)
    date_cols = [c for c in df.columns if re.search(r'date', c, re.I)]
    if date_cols:
        try:
            df['Date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
            return
        except Exception:
            df['Date'] = pd.NaT
            return
    # fallback: create empty Date column (so merges won't crash)
    df['Date'] = pd.NaT

def scrape_fbref_team_season(team_url, output_csv):
    # Validatie en basis-url extract
    match = re.search(r"/squads/([A-Za-z0-9]+)/([\d-]+)/matchlogs/all_comps/([^/]+)", team_url)
    if not match:
        st.error("❌ Ongeldige URL. Gebruik een FBref 'match logs' URL (bijv. .../matchlogs/all_comps/shooting/Team-Name-Match-Logs-All-Competitions).")
        return None

    team_id, season, remainder = match.groups()
    # NOTE: remainder may contain 'shooting/Team-Name-...'. We'll extract base part
    base_prefix = f"https://fbref.com/en/squads/{team_id}/{season}/matchlogs/all_comps/"

    tabs = ["shooting", "passing", "passing_types", "defense", "possession", "misc", "keeper", "keeper_adv"]
    merge_keys = ["Date", "Comp", "Opponent", "Venue", "Result"]
    dfs = []
    session = make_session()

    progress_bar = st.progress(0)
    for i, tab in enumerate(tabs, start=1):
        # build url safely
        # some inputs might already contain a trailing tab — avoid duplicating
        url = base_prefix + tab + "/"
        st.write(f"📥 Scraping {tab} -> {url}")
        try:
            resp = session.get(url, timeout=12)
        except Exception as e:
            st.warning(f"⚠️ Request failed voor {tab}: {e}")
            progress_bar.progress(i/len(tabs))
            continue

        if resp.status_code != 200:
            st.warning(f"⚠️ HTTP {resp.status_code} voor {tab}")
            progress_bar.progress(i/len(tabs))
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        table = find_table_in_soup(soup, tab_hint=tab)
        if table is None:
            st.warning(f"⚠️ Geen tabel gevonden voor {tab} (geen <table> of comment-table).")
            progress_bar.progress(i/len(tabs))
            continue

        # try to read with pandas
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception as e:
            st.warning(f"⚠️ pandas kon tabel voor {tab} niet lezen: {e}")
            progress_bar.progress(i/len(tabs))
            continue

        df = flatten_multicol(df)

        # ensure merge keys exist (create if absent)
        for key in merge_keys:
            if key not in df.columns:
                df[key] = None

        # parse date if present
        safe_parse_date_column(df)

        # rename non-merge columns to include tab suffix, keep merge_keys intact
        rename_map = {c: f"{c}_{tab}" for c in df.columns if c not in merge_keys}
        df = df.rename(columns=rename_map)

        dfs.append(df)
        progress_bar.progress(i/len(tabs))

    if not dfs:
        st.error("❌ Geen data gevonden voor dit team/seizoen (alle tabbladen mislukten).")
        return None

    # merge all on merge_keys (outer join to keep all matches)
    try:
        df_final = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how='outer'), dfs)
    except Exception as e:
        st.error(f"❌ Fout bij mergen: {e}")
        return None

    # sort by Date if available
    if 'Date' in df_final.columns:
        try:
            df_final = df_final.sort_values('Date', ascending=True)
        except Exception:
            pass

    # write CSV
    os.makedirs(SAVE_PATH, exist_ok=True)
    try:
        df_final.to_csv(output_csv, index=False)
        st.success(f"✅ Data opgeslagen in {output_csv} (rijen: {len(df_final)})")
    except Exception as e:
        st.error(f"⚠️ Opslaan CSV mislukt: {e}")
        return None

    return df_final

# ---------- Streamlit UI ----------
st.title("⚽ FBref Team Scraper (robust)")

url = st.text_input("👉 Plak hier de FBref team-URL (match logs):", value="")
if st.button("Scrape team data"):
    if url.strip():
        team_name = url.rstrip('/').split('/')[-1].replace("-Match-Logs-All-Competitions", "")
        # neem output pad
        safe_name = re.sub(r'[^0-9A-Za-z_-]+', '_', team_name) or "team"
        output_file = os.path.join(SAVE_PATH, f"{safe_name}_season_stats.csv")
        df = scrape_fbref_team_season(url, output_file)
        if df is not None:
            st.write("Eerste 20 rijen van de samengevoegde tabel:")
            st.dataframe(df.head(20))
            st.download_button("⬇️ Download CSV", data=df.to_csv(index=False).encode("utf-8"),
                               file_name=os.path.basename(output_file), mime="text/csv")
    else:
        st.warning("⚠️ Voer eerst een geldige FBref 'match logs' URL in.")
