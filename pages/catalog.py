"""
Song Catalog Viewer
-------------------
Sortable / filterable table of all songs in the database.
Run alongside the main dashboard via: streamlit run dashboard.py
"""

import os
import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv(override=True)
DB_URL = os.environ["DATABASE_URL"]

st.set_page_config(page_title="Song Catalog", page_icon="🎵", layout="wide")
st.title("🎵 Song Catalog")

# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner="Loading catalog…")
def load_catalog() -> pd.DataFrame:
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            s.title,
            a.name                          AS artist,
            s.label,
            s.label_tier,
            s.release_date,
            s.genre_tags,
            s.under_radar,
            s.playlist_follower_count,
            s.isrc,
            s.spotify_track_id,
            s.first_signal_at::date         AS first_seen,
            s.created_at::date              AS added
        FROM songs s
        JOIN artists a ON a.id = s.artist_id
        ORDER BY s.created_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty:
        df["genre_tags"] = df["genre_tags"].apply(
            lambda g: ", ".join(g) if g else ""
        )
        df["playlist_follower_count"] = df["playlist_follower_count"].fillna(0).astype(int)
        df["under_radar"] = df["under_radar"].fillna(False)
    return df

df = load_catalog()

if df.empty:
    st.warning("No songs in catalog yet.")
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.header("🔍 Filters")

    search = st.text_input("Title or artist", placeholder="Search…")

    label_tiers = ["All"] + sorted(df["label_tier"].dropna().unique().tolist())
    tier_filter = st.selectbox("Label tier", label_tiers)

    under_radar_filter = st.selectbox(
        "Under radar",
        ["All", "Yes", "No"],
    )

    has_label = st.checkbox("Only songs with label data", value=False)
    has_genre = st.checkbox("Only songs with genre data", value=False)

    st.divider()
    st.caption(f"{len(df):,} total songs in catalog")

# ── Apply filters ─────────────────────────────────────────────────────────────

filtered = df.copy()

if search:
    q = search.lower()
    mask = (
        filtered["title"].str.lower().str.contains(q, na=False) |
        filtered["artist"].str.lower().str.contains(q, na=False)
    )
    filtered = filtered[mask]

if tier_filter != "All":
    filtered = filtered[filtered["label_tier"] == tier_filter]

if under_radar_filter == "Yes":
    filtered = filtered[filtered["under_radar"] == True]
elif under_radar_filter == "No":
    filtered = filtered[filtered["under_radar"] == False]

if has_label:
    filtered = filtered[filtered["label"].notna() & (filtered["label"] != "")]

if has_genre:
    filtered = filtered[filtered["genre_tags"] != ""]

# ── Summary metrics ───────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("Songs shown", f"{len(filtered):,}")
c2.metric("Major label",  f"{(filtered['label_tier'] == 'major').sum():,}")
c3.metric("Indie",        f"{(filtered['label_tier'] == 'indie').sum():,}")
c4.metric("Unsigned",     f"{(filtered['label_tier'] == 'unsigned').sum():,}")

st.divider()

# ── Table ─────────────────────────────────────────────────────────────────────

st.dataframe(
    filtered,
    use_container_width=True,
    height=650,
    column_config={
        "title": st.column_config.TextColumn("Title", width="medium"),
        "artist": st.column_config.TextColumn("Artist", width="medium"),
        "label": st.column_config.TextColumn("Label", width="medium"),
        "label_tier": st.column_config.TextColumn("Tier", width="small"),
        "release_date": st.column_config.DateColumn("Released", width="small"),
        "genre_tags": st.column_config.TextColumn("Genres", width="large"),
        "under_radar": st.column_config.CheckboxColumn("Under Radar", width="small"),
        "playlist_follower_count": st.column_config.NumberColumn(
            "Playlist Followers", format="%d", width="small"
        ),
        "isrc": st.column_config.TextColumn("ISRC", width="small"),
        "spotify_track_id": st.column_config.TextColumn("Spotify ID", width="small"),
        "first_seen": st.column_config.DateColumn("First Seen", width="small"),
        "added": st.column_config.DateColumn("Added", width="small"),
    },
    column_order=[
        "title", "artist", "label_tier", "label", "release_date",
        "genre_tags", "under_radar", "playlist_follower_count",
        "first_seen", "added", "isrc", "spotify_track_id",
    ],
)

st.caption(f"Showing {len(filtered):,} of {len(df):,} songs · click any column header to sort")
