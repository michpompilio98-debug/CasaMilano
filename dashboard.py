import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db import get_listings, init_db
from config import SEARCH_CRITERIA

st.set_page_config(
    page_title="CasaMilano",
    page_icon="🏠",
    layout="wide",
)

try:
    sb_secrets = dict(st.secrets.get("supabase", {}))
    st.write("DEBUG secrets:", list(sb_secrets.keys()))
except Exception as e:
    st.write("DEBUG secrets error:", str(e))

init_db()

import db as _db
st.write("DEBUG backend:", "Supabase" if _db._USE_SUPABASE else "SQLite")

# ── Sidebar filters ──────────────────────────────────────────────────────────
st.sidebar.title("Filtri")

all_zones = SEARCH_CRITERIA["zones"] + ["Altro"]
selected_zones = st.sidebar.multiselect(
    "Zone", all_zones, default=SEARCH_CRITERIA["zones"]
)

max_ppm = st.sidebar.slider(
    "Max €/m²", 3000, 10000, SEARCH_CRITERIA["max_price_per_sqm"], step=100
)

rooms_options = {"Bilocale (2)": 2, "Trilocale (3)": 3, "Entrambi": None}
rooms_sel = st.sidebar.selectbox("Locali", list(rooms_options.keys()), index=2)
rooms_val = rooms_options[rooms_sel]

min_year = st.sidebar.number_input("Anno costruzione minimo", 2000, 2025, 2015)

sources = st.sidebar.multiselect(
    "Fonti",
    ["immobiliare", "idealista", "subito"],
    default=["immobiliare", "idealista", "subito"],
)

only_new = st.sidebar.checkbox("Solo nuovi annunci", value=False)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data(zones, max_ppm, rooms, min_year, only_new):
    listings = get_listings(
        zones=zones if zones else None,
        max_price_per_sqm=max_ppm,
        min_rooms=rooms,
        max_rooms=rooms,
        min_year=min_year,
        only_new=only_new,
    )
    return pd.DataFrame(listings) if listings else pd.DataFrame()


df = load_data(
    selected_zones, max_ppm,
    rooms_val, min_year, only_new,
)

# Filter by source
if not df.empty and sources:
    df = df[df["source"].isin(sources)]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏠 CasaMilano")
st.caption("Annunci aggiornati automaticamente ogni 4 ore")

if df.empty:
    st.warning("Nessun annuncio trovato. Esegui `python main.py scrape` per raccogliere i dati.")
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Annunci trovati", len(df))
col2.metric("Prezzo medio/m²", f"€{int(df['price_per_sqm'].dropna().mean()):,}" if not df['price_per_sqm'].dropna().empty else "—")
col3.metric("Prezzo min totale", f"€{int(df['price'].dropna().min()):,}" if not df['price'].dropna().empty else "—")
col4.metric("Nuovi annunci", int(df["is_new"].sum()) if "is_new" in df.columns else 0)

st.divider()

# ── Charts ────────────────────────────────────────────────────────────────────
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("€/m² per zona")
    zone_df = df.dropna(subset=["price_per_sqm", "zone"])
    if not zone_df.empty:
        fig = px.box(
            zone_df, x="zone", y="price_per_sqm",
            color="zone",
            labels={"zone": "Zona", "price_per_sqm": "€/m²"},
        )
        fig.add_hline(
            y=SEARCH_CRITERIA["max_price_per_sqm"],
            line_dash="dash", line_color="red",
            annotation_text=f"Max {SEARCH_CRITERIA['max_price_per_sqm']} €/m²",
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Dati insufficienti per il grafico.")

with chart_col2:
    st.subheader("Distribuzione prezzi totali")
    price_df = df.dropna(subset=["price"])
    if not price_df.empty:
        fig2 = px.histogram(
            price_df, x="price", nbins=20,
            color="source",
            labels={"price": "Prezzo (€)", "count": "Annunci"},
        )
        fig2.update_layout(height=350, bargap=0.05)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Dati insufficienti per il grafico.")

# ── Trend over time ───────────────────────────────────────────────────────────
if "first_seen" in df.columns and not df.empty:
    trend_df = df.dropna(subset=["price_per_sqm", "first_seen"]).copy()
    if not trend_df.empty:
        trend_df["date"] = pd.to_datetime(trend_df["first_seen"]).dt.date
        daily = trend_df.groupby("date")["price_per_sqm"].mean().reset_index()
        if len(daily) > 1:
            st.subheader("Trend €/m² nel tempo")
            fig3 = px.line(daily, x="date", y="price_per_sqm",
                           labels={"date": "Data", "price_per_sqm": "€/m² medio"})
            fig3.update_layout(height=280)
            st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Listings table ────────────────────────────────────────────────────────────
st.subheader(f"Annunci ({len(df)})")

display_df = df[[
    "source", "zone", "title", "price", "sqm", "price_per_sqm",
    "rooms", "address", "year_built", "floor", "energy_class", "url", "first_seen", "is_new"
]].copy()

display_df["price"] = display_df["price"].apply(
    lambda x: f"€{int(x):,}" if pd.notna(x) else "—"
)
display_df["price_per_sqm"] = display_df["price_per_sqm"].apply(
    lambda x: f"€{int(x):,}/m²" if pd.notna(x) else "—"
)
display_df["sqm"] = display_df["sqm"].apply(
    lambda x: f"{int(x)} m²" if pd.notna(x) else "—"
)
display_df["is_new"] = display_df["is_new"].apply(lambda x: "🆕" if x else "")

display_df = display_df.rename(columns={
    "source": "Fonte", "zone": "Zona", "title": "Titolo",
    "price": "Prezzo", "sqm": "m²", "price_per_sqm": "€/m²",
    "rooms": "Locali", "address": "Indirizzo", "year_built": "Anno",
    "floor": "Piano", "energy_class": "Classe E.", "url": "Link",
    "first_seen": "Visto il", "is_new": "Nuovo",
})

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Link": st.column_config.LinkColumn("Link"),
    },
)
