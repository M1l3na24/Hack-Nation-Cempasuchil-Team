import streamlit as st
import pandas as pd
import plotly.express as px
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import requests
import os
import random

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Cempasuchil Dashboard", layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>.block-container {padding: 0rem;} section[data-testid="stSidebar"] {width: 350px !important;}</style>""", unsafe_allow_html=True)

# --- 2. SMART SEARCH LOGIC ---
STOPWORDS = {"where", "can", "i", "find", "is", "the", "are", "a", "an", "of", "in", "to", "for", "with", "show", "me", "list", "hospital", "clinic", "center"}

def smart_filter_data(df, query):
    if df.empty or not query: return pd.DataFrame()
    raw_words = query.lower().replace("?", "").replace(".", "").split()
    keywords = [w for w in raw_words if w not in STOPWORDS and len(w) > 2]
    
    if not keywords: return df.sort_values("capacity", ascending=False).head(15)
    
    search_col = df.astype(str).sum(axis=1).str.lower()
    scores = pd.Series(0, index=df.index)
    for k in keywords:
        scores += df["specialties"].astype(str).str.lower().str.count(k) * 3
        scores += df["equipment"].astype(str).str.lower().str.count(k) * 3
        scores += search_col.str.count(k)
        
    matches = df[scores > 0].copy()
    if matches.empty: return pd.DataFrame()
    
    matches["relevance"] = scores[matches.index]
    return matches.sort_values("relevance", ascending=False).head(20)

# --- 3. COORDINATES & SCORING ---
CITY_COORDS = {
    "Accra": [5.6037, -0.1870], "Kumasi": [6.6885, -1.6244], "Tamale": [9.4075, -0.8534],
    "Takoradi": [4.8845, -1.7554], "Cape Coast": [5.1315, -1.2795], "Sunyani": [7.3399, -2.3268],
    "Ho": [6.6124, 0.4674], "Wa": [10.0601, -2.5099], "Bolgatanga": [10.7856, -0.8514],
    "Koforidua": [6.0941, -0.2609], "Tema": [5.6698, -0.0166]
}

def get_coords_from_city(row):
    city = str(row['city']).strip().title()
    for key, val in CITY_COORDS.items():
        if key in city:
            return val[0] + random.uniform(-0.02, 0.02), val[1] + random.uniform(-0.02, 0.02)
    return 7.9465 + random.uniform(-0.5, 0.5), -1.0232 + random.uniform(-0.5, 0.5)

def calculate_desert_score(row):
    score = 0
    cap = pd.to_numeric(row['capacity'], errors='coerce')
    if cap > 20: score += 4
    elif cap > 0: score += 2
    
    specs = str(row['specialties']).lower()
    if any(k in specs for k in ['surgery', 'emergency', 'maternity', 'radiology']): score += 3
    elif len(specs) > 5: score += 1
        
    equip = str(row['equipment']).lower()
    if "x-ray" in equip or "mri" in equip: score += 1
    if len(equip) > 5: score += 2
    
    return min(10, score)

# --- 4. LOAD DATA ---
def load_data_force():
    if "user_token" in st.session_state and st.session_state.user_token:
        host = "https://dbc-90c394b5-b981.cloud.databricks.com"
        token = st.session_state.user_token
    else: return pd.DataFrame()

    try:
        # Nuke Env Vars to prevent 401 conflicts
        for var in ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_TOKEN"]:
            if var in os.environ: del os.environ[var]
        
        config = Config(host=host, token=token)
        w = WorkspaceClient(config=config)
        
        sql = "SELECT name, address_city as city, capacity, specialties, equipment, numberDoctors FROM workspace.default.ghana_health_facilities LIMIT 2000"
        
        res = w.statement_execution.execute_statement(
            warehouse_id=list(w.warehouses.list())[0].id, catalog="workspace", schema="default", statement=sql, wait_timeout="30s"
        )
        
        if res.result and res.result.data_array:
            df = pd.DataFrame(res.result.data_array, columns=["name", "city", "capacity", "specialties", "equipment", "numberDoctors"])
            coords = df.apply(get_coords_from_city, axis=1)
            df["lat"] = [c[0] for c in coords]
            df["lon"] = [c[1] for c in coords]
            df["Desert Score"] = df.apply(calculate_desert_score, axis=1)
            df["MapSize"] = df["Desert Score"].replace(0, 1) * 2
            return df
        return pd.DataFrame()
    except: return pd.DataFrame()

# --- 5. BRAIN ---
def query_brain(sys_msg, user_msg, context):
    try:
        host = "https://dbc-90c394b5-b981.cloud.databricks.com"
        url = f"{host}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations"
        payload = {
            "messages": [{"role": "system", "content": sys_msg}, {"role": "user", "content": f"CTX:{context}\nQ:{user_msg}"}],
            "max_tokens": 800, "temperature": 0.1
        }
        resp = requests.post(url, headers={"Authorization": f"Bearer {st.session_state.user_token}"}, json=payload)
        return resp.json()['choices'][0]['message']['content'] if resp.status_code == 200 else "Error"
    except: return "Error"

# --- 6. UI SETUP ---
if "master_df" not in st.session_state: st.session_state.master_df = pd.DataFrame()
if "active_df" not in st.session_state: st.session_state.active_df = pd.DataFrame()
if "messages" not in st.session_state: st.session_state.messages = []

# --- 7. SIDEBAR (SECURE & COMPACT) ---
with st.sidebar:
    st.title("🏵️ Cempasuchil")
    st.caption("Virtue Foundation Audit")
    
    # === SECURE AUTH SECTION ===
    # expanded=False makes it small/hidden by default
    with st.expander("🔐 Admin Keys", expanded=False): 
        # type="password" hides the text
        st.session_state.user_token = st.text_input("Databricks Token", type="password", help="Paste your PAT here")
        
        if st.button("🔄 Connect"):
            with st.spinner("Authenticating..."):
                df = load_data_force()
                if not df.empty:
                    st.session_state.master_df = df
                    st.session_state.active_df = df
                    st.success("Connected!")
                else: st.error("Failed.")

    # CHAT AREA
    chat_container = st.container()
    with chat_container:
        for m in st.session_state.messages:
            with st.chat_message(m["role"]): st.markdown(m["content"])
            
    # INPUT
    if prompt := st.chat_input("Ex: Find X-Ray facilities..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # SMART SEARCH
        df = st.session_state.master_df
        matched_df = smart_filter_data(df, prompt)
        
        if not matched_df.empty:
            st.session_state.active_df = matched_df
            context = matched_df.to_string()
        else:
            context = "No specific facilities matched."
            
        sys_p = "You are the Cempasuchil Agent. Use the CONTEXT to answer. Be concise."
        ans = query_brain(sys_p, prompt, context)
        st.session_state.messages.append({"role": "assistant", "content": ans})
        st.rerun()

    if st.button("🌍 Reset Map"):
        st.session_state.active_df = st.session_state.master_df
        st.rerun()

# --- 8. RENDER MAP ---
current_df = st.session_state.active_df
if not current_df.empty:
    fig = px.scatter_mapbox(
        current_df, lat="lat", lon="lon", color="Desert Score", size="MapSize",
        color_continuous_scale="RdYlGn", range_color=[0, 10], zoom=6, height=950,
        hover_name="name", hover_data=["city", "specialties"],
        title=f"Medical Desert Audit ({len(current_df)} Facilities)"
    )
    fig.update_layout(mapbox_style="carto-darkmatter", margin={"r":0,"t":40,"l":0,"b":0})
    st.plotly_chart(fig, use_container_layout=True)
else:
    st.info("👈 Open '🔐 Admin Keys' in the sidebar to connect.")