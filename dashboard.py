"""
Everywhere Chart — Live Scoring Dashboard
-----------------------------------------
Run:  streamlit run dashboard.py
      (from the Charts_Project directory with DATABASE_URL in .env)
"""

import json
import math
import os
import re
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

load_dotenv(override=True)
DB_URL = os.environ["DATABASE_URL"]

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Everywhere Chart",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
header[data-testid="stHeader"] { display: none; }
[data-testid="stToolbar"] { display: none; }
#MainMenu { display: none; }
.block-container { padding-top: 0 !important; }
</style>
""", unsafe_allow_html=True)

# ── Password gate ─────────────────────────────────────────────────────────────

def _check_password() -> bool:
    """Return True if the user has entered the correct password."""
    correct = st.secrets.get("APP_PASSWORD", os.environ.get("APP_PASSWORD", "civilwar"))

    if st.session_state.get("authenticated"):
        return True

    st.markdown("""
    <style>
    .login-wrap { max-width: 340px; margin: 15vh auto 0; text-align: center; }
    .login-title { font-size: 1.6rem; font-weight: 600; margin-bottom: 0.25rem; }
    .login-sub { color: #888; font-size: 0.85rem; margin-bottom: 1.5rem; }
    </style>
    <div class="login-wrap">
      <div class="login-title">🌍 Everywhere Chart</div>
      <div class="login-sub">Enter the access password to continue</div>
    </div>
    """, unsafe_allow_html=True)

    pw = st.text_input("Password", type="password", label_visibility="collapsed",
                       placeholder="Password")
    if pw:
        if pw == correct:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False

if not _check_password():
    st.stop()

# ── Data loading (cached) ─────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Loading signals from database...")
def load_signals(window_days: int) -> pd.DataFrame:
    """Pull raw signal_events for the rolling window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            se.id,
            se.song_id::text,
            se.source_platform,
            se.weighted_score,
            se.intentionality_score,
            se.engagement_multiplier,
            se.is_home_community,
            se.resolution_confidence,
            se.observed_at,
            COALESCE(c.community_type, se.source_platform) AS community_type,
            c.casual_weight        AS community_casual_weight,
            COALESCE(c.external_id, se.source_platform) AS community_name,
            se.community_id::text,
            s.title                AS song_title,
            a.name                 AS artist_name
        FROM signal_events se
        LEFT JOIN communities c ON se.community_id = c.id
        JOIN songs s            ON se.song_id = s.id
        JOIN artists a          ON s.artist_id = a.id
        WHERE se.observed_at >= %s
          AND se.resolution_confidence >= 0.65
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=300)
def load_prev_scores(window_days: int) -> dict:
    """Load last week's penetration scores for momentum calculation."""
    prev_date = date.today() - timedelta(days=7)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT song_id::text, penetration_score, rank
        FROM song_scores
        WHERE window_date = %s
        ORDER BY rank
    """, (prev_date,))
    rows = cur.fetchall()
    conn.close()
    return {r["song_id"]: {"score": float(r["penetration_score"] or 0), "rank": r["rank"]}
            for r in rows}

# ── Sidebar dials ─────────────────────────────────────────────────────────────

with st.sidebar:
    stats_placeholder = st.container()
    st.divider()
    st.header("⚙️ Scoring Dials")

    st.subheader("Window")
    window_days = st.select_slider(
        "Rolling window",
        options=[7, 14, 30, 60, 90],
        value=7,
        help="How many days of signal history to include",
    )

    st.subheader("Platform Weights")
    st.caption("Scale each platform's contribution. 1.0 = default.")
    w_press   = st.slider("Press",   0.0, 3.0, 1.0, 0.05)
    w_spotify = st.slider("Spotify", 0.0, 3.0, 1.0, 0.05)
    w_reddit  = st.slider("Reddit",  0.0, 3.0, 1.0, 0.05)
    w_shazam  = st.slider("Shazam",  0.0, 3.0, 1.0, 0.05)
    w_tiktok  = st.slider("TikTok",  0.0, 3.0, 1.0, 0.05)
    w_youtube = st.slider("YouTube", 0.0, 3.0, 1.0, 0.05)

    platform_weights = {
        "press":   w_press,
        "spotify": w_spotify,
        "reddit":  w_reddit,
        "shazam":  w_shazam,
        "tiktok":  w_tiktok,
        "youtube": w_youtube,
    }

    st.subheader("Scoring Formula")
    home_coeff = st.slider(
        "Home community coefficient",
        0.0, 1.0, 0.1, 0.05,
        help="How much home-community signals count toward the score. 0 = excluded entirely.",
    )
    diversity_coeff = st.slider(
        "Diversity multiplier",
        0.0, 0.5, 0.25, 0.05,
        help="Bonus per additional community type. 0.25 = +25% per extra type.",
    )

    st.caption(
        "Press score counts each unique publication once per song — "
        "volume of articles within a publication doesn't inflate the score."
    )

    st.subheader("Filters")
    min_community_types = st.slider(
        "Min community types",
        1, 5, 1,
        help="Songs must appear across this many distinct audience types.",
    )
    min_casual_weight = st.slider(
        "Min community casual weight",
        0.0, 1.0, 0.0, 0.05,
        help="Exclude signals from niche communities below this threshold.",
    )
    exclude_non_music = st.checkbox(
        "Exclude non-music communities",
        value=False,
        help="Filter out signals from non_music community type.",
    )

    st.subheader("Chart")
    chart_size = st.slider("Results to show", 10, 50, 25, 5)

    if st.button("🔄 Refresh data from DB"):
        st.cache_data.clear()
        st.rerun()

# ── Score computation ─────────────────────────────────────────────────────────

def compute_scores(df: pd.DataFrame, platform_weights: dict,
                   home_coeff: float, diversity_coeff: float,
                   min_community_types: int, min_casual_weight: float,
                   exclude_non_music: bool) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame()

    d = df.copy()

    # Apply platform weights
    d["adj_score"] = d.apply(
        lambda r: r["weighted_score"] * platform_weights.get(r["source_platform"], 1.0),
        axis=1,
    )

    # Drop rows from platforms weighted to zero — they should contribute nothing,
    # including to community type counts and diversity scoring
    d = d[d["adj_score"] > 0]
    if d.empty:
        return pd.DataFrame()

    # Apply casual weight filter
    if min_casual_weight > 0:
        d = d[
            d["community_casual_weight"].isna() |
            (d["community_casual_weight"] >= min_casual_weight)
        ]

    # Exclude non_music communities
    if exclude_non_music:
        d = d[d["community_type"] != "non_music"]

    if d.empty:
        return pd.DataFrame()

    # ── Press: deduplicate to one score per unique publication (community_id) ──
    # Multiple articles from the same outlet count once — breadth matters, not volume.
    # Keep the highest weighted_score row per (song, community) so we use the
    # most representative signal from each publication.
    press_mask = d["source_platform"] == "press"
    press_deduped = (
        d[press_mask]
        .sort_values("adj_score", ascending=False)
        .drop_duplicates(subset=["song_id", "community_id"])
    )
    d_effective = pd.concat([d[~press_mask], press_deduped], ignore_index=True)

    # Home / out split
    d_effective["home_score"] = d_effective["adj_score"].where(d_effective["is_home_community"] == True, 0)
    d_effective["out_score"]  = d_effective["adj_score"].where(d_effective["is_home_community"] != True, 0)

    # Per-song aggregation
    song_agg = d_effective.groupby(["song_id", "song_title", "artist_name"]).agg(
        out_of_home          = ("out_score",      "sum"),
        home                 = ("home_score",     "sum"),
        community_count      = ("community_id",   "nunique"),
        community_type_count = ("community_type", "nunique"),
        total_signals        = ("id",             "count"),
    ).reset_index()

    # Signal breakdown by platform (uses deduplicated press scores for display)
    platform_totals = (
        d_effective.groupby(["song_id", "source_platform"])["adj_score"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    song_agg = song_agg.merge(platform_totals, on="song_id", how="left")

    # Filter by min community types
    song_agg = song_agg[song_agg["community_type_count"] >= min_community_types]
    if song_agg.empty:
        return pd.DataFrame()

    # Diversity multiplier
    song_agg["div_mult"] = 1 + diversity_coeff * (song_agg["community_type_count"] - 1).clip(lower=0)

    # Penetration score
    def pen_score(row):
        num = row["out_of_home"] + row["home"] * home_coeff
        den = math.log10(row["home"] + 1) if row["home"] > 0 else 1.0
        return round((num * row["div_mult"]) / den, 4)

    song_agg["penetration_score"] = song_agg.apply(pen_score, axis=1)

    return song_agg.sort_values("penetration_score", ascending=False).reset_index(drop=True)

# ── Chart data builder ────────────────────────────────────────────────────────

COMMUNITY_COLORS = {
    "non_music":     "#bf3e2e",
    "lifestyle":     "#a07c28",
    "sports":        "#2d6645",
    "entertainment": "#4a4a44",
    "general_music": "#9a9a90",
    "genre":         "#9a9a90",
    "artist":        "#9a9a90",
}

def build_songs_data(scored: pd.DataFrame, df_raw: pd.DataFrame,
                     prev_scores: dict, top_n: int) -> list:
    max_score = float(scored["penetration_score"].iloc[0]) if not scored.empty else 1.0
    songs = []
    for i, row in scored.head(top_n).iterrows():
        rank = i + 1
        song_id = row["song_id"]
        score = float(row["penetration_score"])

        prev = prev_scores.get(song_id, {})
        prev_rank = prev.get("rank")

        if prev_rank is None:
            dt, delta_str = "nw", "NEW"
        elif prev_rank > rank:
            dt, delta_str = "up", f"+{prev_rank - rank}"
        elif prev_rank < rank:
            dt, delta_str = "dn", f"−{rank - prev_rank}"
        else:
            dt, delta_str = "nt", "—"

        # Out-of-home community breakdown
        song_sigs = df_raw[(df_raw["song_id"] == song_id) & (df_raw["is_home_community"] != True)]
        comm_agg = (
            song_sigs.groupby(["community_name", "community_type", "source_platform"])["weighted_score"]
            .sum().reset_index()
            .sort_values("weighted_score", ascending=False)
            .head(5)
        )
        max_c = float(comm_agg["weighted_score"].max()) if not comm_agg.empty else 1.0

        contexts = []
        for _, cr in comm_agg.iterrows():
            name = cr["community_name"]
            if cr["source_platform"] == "reddit" and not name.startswith("r/"):
                name = f"r/{name}"
            contexts.append({
                "n": name,
                "t": cr["community_type"],
                "d": round(float(cr["weighted_score"]) / max_c, 2),
                "c": COMMUNITY_COLORS.get(cr["community_type"], "#9a9a90"),
            })

        furthest = contexts[0] if contexts else {"n": "—", "t": "—"}
        n_types  = int(row["community_type_count"])

        tags = []
        if dt == "nw":
            tags.append({"l": "new entry", "c": "o"})
        elif dt == "up":
            tags.append({"l": delta_str, "c": "g"})
        elif dt == "dn":
            tags.append({"l": delta_str, "c": "r"})
        tags.append({"l": f"{n_types} contexts", "c": "g" if n_types >= 4 else ""})

        # Platform breakdown for expanded view
        platforms = []
        for p in ["press", "spotify", "reddit", "shazam", "tiktok", "youtube"]:
            if p in row and float(row[p]) > 0:
                platforms.append({"p": p, "v": round(float(row[p]), 1)})

        songs.append({
            "rank":         rank,
            "prev":         prev_rank,
            "title":        row["song_title"],
            "artist":       row["artist_name"],
            "score":        f"{score:.1f}",
            "pen":          int(round(score / max_score * 100)),
            "penCtx":       f"{n_types} distinct types",
            "delta":        delta_str,
            "dt":           dt,
            "communities":  int(row["community_count"]),
            "signals":      int(row["total_signals"]),
            "furthest":     furthest["n"],
            "furthestType": furthest["t"],
            "contexts":     contexts,
            "platforms":    platforms,
            "tags":         tags,
        })
    return songs


def render_chart_html(songs: list, window_days: int) -> str:
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=window_days)
    date_lbl = f"{start_dt.strftime('%b %-d')} – {end_dt.strftime('%-d, %Y')}"
    songs_js = json.dumps(songs, ensure_ascii=False)

    lead = songs[0] if songs else {}
    sec  = songs[1:3]

    def move_label(s):
        if s["dt"] == "nw": return "New entry"
        if s["dt"] == "up": return f"↑ #{s['prev']} → #{s['rank']}"
        if s["dt"] == "dn": return f"↓ #{s['prev']} → #{s['rank']}"
        return f"Holds at #{s['rank']}"

    def lead_community_pills(s):
        return "".join(
            f'<div class="cpill">'
            f'<div class="cdot" style="background:{c["c"]}"></div>'
            f'<div class="cnm">{c["n"]}</div>'
            f'<div class="cdv">{c["d"]}</div>'
            f'</div>'
            for c in s.get("contexts", [])[:4]
        )

    def sec_card(s):
        badge_col = {"up": "var(--green)", "nw": "var(--gold)", "dn": "var(--accent)"}.get(s["dt"], "var(--ink3)")
        return f"""
        <div class="sec-item">
          <div class="sec-eyebrow">
            <span class="sbadge" style="background:var(--bg3);color:{badge_col}">{move_label(s)}</span>
          </div>
          <div class="sec-title">{s['title']}</div>
          <div class="sec-artist">{s['artist']}</div>
          <div class="sec-stats">
            <div class="ssi"><div class="ssi-l">Score</div><div class="ssi-v">{s['score']}</div></div>
            <div class="ssi"><div class="ssi-l">Types</div><div class="ssi-v">{s['penCtx'].split()[0]}</div></div>
            <div class="ssi"><div class="ssi-l">Furthest</div>
              <div class="ssi-v" style="font-size:10px;font-family:var(--mono);padding-top:2px">{s['furthest']}</div>
            </div>
          </div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,600;1,400;1,600&family=JetBrains+Mono:wght@400;500&family=Hahmlet:wght@300;400;500&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --ink:#0f0f0d;--ink2:#4a4a44;--ink3:#9a9a90;
  --rule:#e0ddd5;--bg:#faf8f3;--bg2:#f2efe8;--bg3:#eae7de;
  --accent:#bf3e2e;--green:#2d6645;--gold:#a07c28;
  --serif:'Playfair Display',Georgia,serif;
  --sans:'Hahmlet',Georgia,serif;
  --mono:'JetBrains Mono',monospace;
}}
@media(prefers-color-scheme:dark){{
  :root{{
    --ink:#f0ede4;--ink2:#aaa89e;--ink3:#666560;
    --rule:#272722;--bg:#0e0e0b;--bg2:#161612;--bg3:#1e1e19;
    --accent:#d95040;--green:#4a9e6a;--gold:#c09040;
  }}
}}
body{{background:var(--bg);color:var(--ink);font-family:var(--sans);font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}}
button{{cursor:pointer;font-family:inherit}}
.wrap{{max-width:820px;margin:0 auto;padding:0 24px 80px}}
.mast{{padding:30px 0 16px;display:flex;align-items:flex-end;justify-content:space-between;border-bottom:1.5px solid var(--ink)}}
.wordmark{{font-family:var(--serif);font-size:30px;line-height:1;letter-spacing:-0.3px}}
.wordmark em{{font-style:italic;color:var(--accent)}}
.mast-right{{text-align:right}}
.mast-date{{font-family:var(--mono);font-size:10px;color:var(--ink3);letter-spacing:.06em;text-transform:uppercase}}
.section-lbl{{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--ink3);padding:20px 0 10px;border-bottom:1px solid var(--rule)}}
.card{{border:1px solid var(--rule);border-top:none}}
.lead-grid{{display:grid;grid-template-columns:1fr 210px}}
.lead-story{{padding:20px 22px;border-right:1px solid var(--rule)}}
.lead-eyebrow{{display:flex;align-items:center;gap:8px;margin-bottom:10px}}
.etag{{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--bg);background:var(--accent);padding:2px 7px}}
.emove{{font-family:var(--mono);font-size:10px;color:var(--green)}}
.lead-title{{font-family:var(--serif);font-size:22px;line-height:1.15;font-style:italic;margin-bottom:2px}}
.lead-artist{{font-family:var(--mono);font-size:10px;color:var(--ink3);margin-bottom:12px;text-transform:uppercase;letter-spacing:.04em}}
.lead-text{{font-size:13px;color:var(--ink2);line-height:1.8}}
.lead-data{{padding:18px;background:var(--bg2)}}
.lrank{{font-family:var(--serif);font-size:52px;line-height:1;color:var(--ink);margin-bottom:1px}}
.lrank-sub{{font-family:var(--mono);font-size:9px;color:var(--ink3);text-transform:uppercase;letter-spacing:.05em;margin-bottom:16px}}
.dblock{{margin-bottom:12px}}
.dlbl{{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink3);margin-bottom:2px}}
.dval{{font-family:var(--serif);font-size:15px;color:var(--ink)}}
.dsub{{font-family:var(--mono);font-size:9px;color:var(--ink3);margin-top:1px}}
.cpill{{display:flex;align-items:center;gap:5px;padding:2px 0;font-family:var(--mono);font-size:9px}}
.cdot{{width:4px;height:4px;border-radius:50%;flex-shrink:0}}
.cnm{{color:var(--ink2);flex:1}}
.cdv{{color:var(--ink3)}}
.sec-grid{{display:grid;grid-template-columns:1fr 1fr}}
.sec-item{{padding:16px 18px;border-right:1px solid var(--rule)}}
.sec-item:last-child{{border-right:none}}
.sec-eyebrow{{margin-bottom:7px}}
.sbadge{{font-family:var(--mono);font-size:9px;padding:2px 6px;letter-spacing:.05em}}
.sec-title{{font-family:var(--serif);font-size:15px;line-height:1.2;font-style:italic;margin-bottom:2px}}
.sec-artist{{font-family:var(--mono);font-size:9px;color:var(--ink3);margin-bottom:9px;text-transform:uppercase;letter-spacing:.04em}}
.sec-stats{{display:flex;gap:14px;margin-top:10px;padding-top:10px;border-top:1px solid var(--rule)}}
.ssi .ssi-l{{font-family:var(--mono);font-size:9px;color:var(--ink3);text-transform:uppercase;letter-spacing:.05em}}
.ssi .ssi-v{{font-family:var(--serif);font-size:14px;color:var(--ink)}}
.chart-top{{padding:14px 0 0}}
.chart-tabs{{display:flex;border-bottom:1px solid var(--rule)}}
.ctab{{font-family:var(--mono);font-size:11px;padding:7px 14px;color:var(--ink3);border:none;background:none;border-bottom:2px solid transparent;margin-bottom:-1px;cursor:pointer;letter-spacing:.03em;transition:color .15s}}
.ctab:hover{{color:var(--ink)}}
.ctab.on{{color:var(--accent);border-bottom-color:var(--accent)}}
.chart-meta{{display:flex;align-items:center;justify-content:space-between;padding:10px 0 8px;border-bottom:1px solid var(--rule)}}
.chart-desc{{font-family:var(--mono);font-size:10px;color:var(--ink3)}}
.how-btn{{font-family:var(--mono);font-size:9px;color:var(--ink3);border:1px solid var(--rule);padding:3px 7px;background:none;letter-spacing:.03em;transition:all .15s}}
.how-btn:hover{{border-color:var(--ink);color:var(--ink)}}
.method-box{{padding:12px;background:var(--bg2);border-bottom:1px solid var(--rule);font-size:12px;color:var(--ink2);line-height:1.7;display:none}}
.chart-cols{{display:grid;grid-template-columns:30px 1fr 96px 68px;gap:0 10px;padding:6px 0 5px;font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.07em;color:var(--ink3);border-bottom:1px solid var(--rule)}}
.srow{{display:grid;grid-template-columns:30px 1fr 96px 68px;gap:0 10px;padding:12px 6px;border-bottom:1px solid var(--rule);align-items:start;cursor:pointer;transition:background .1s}}
.srow:hover{{background:var(--bg2)}}
.srank{{font-family:var(--serif);font-size:18px;line-height:1.1;color:var(--ink3);padding-top:2px}}
.srank.t{{color:var(--ink)}}
.stitle{{font-family:var(--serif);font-size:15px;line-height:1.2;color:var(--ink)}}
.sartist{{font-family:var(--mono);font-size:9px;color:var(--ink3);margin-top:2px;letter-spacing:.03em;text-transform:uppercase}}
.stags{{display:flex;gap:3px;margin-top:5px;flex-wrap:wrap}}
.stag{{font-family:var(--mono);font-size:9px;padding:1px 5px;border:1px solid var(--rule);color:var(--ink3);letter-spacing:.02em}}
.stag.r{{border-color:var(--accent);color:var(--accent)}}
.stag.g{{border-color:var(--green);color:var(--green)}}
.stag.o{{border-color:var(--gold);color:var(--gold)}}
.spen-lbl{{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:var(--ink3);margin-bottom:3px}}
.spen-bar{{height:2px;background:var(--rule);margin-bottom:3px}}
.spen-fill{{height:2px;background:var(--accent);transition:width .6s ease}}
.spen-ctx{{font-family:var(--mono);font-size:9px;color:var(--ink3)}}
.spen-ctx span{{color:var(--ink2)}}
.sdelta{{text-align:right;font-family:var(--mono);font-size:11px}}
.sdelta.up{{color:var(--green)}}
.sdelta.dn{{color:var(--accent)}}
.sdelta.nw{{color:var(--gold)}}
.sdelta-sub{{font-size:9px;color:var(--ink3);margin-top:2px}}
.sexp{{display:none;padding:14px 6px 14px 40px;border-bottom:1px solid var(--rule);background:var(--bg2);animation:expandIn .2s ease}}
.sexp.open{{display:block}}
@keyframes expandIn{{from{{opacity:0;transform:translateY(-4px)}}to{{opacity:1;transform:translateY(0)}}}}
.exp-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-bottom:12px}}
.exp-card{{background:var(--bg);border:1px solid var(--rule);padding:9px 10px}}
.ecl{{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink3);margin-bottom:2px}}
.ecv{{font-family:var(--serif);font-size:16px;color:var(--ink);line-height:1.2}}
.ecs{{font-family:var(--mono);font-size:9px;color:var(--ink3);margin-top:1px}}
.exp-lbl{{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink3);margin-bottom:7px}}
.ctx-item{{display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:1px solid var(--rule)}}
.ctx-item:last-child{{border-bottom:none}}
.ctx-dot{{width:4px;height:4px;border-radius:50%;flex-shrink:0}}
.ctx-nm{{flex:1;color:var(--ink2);font-family:var(--mono);font-size:9px}}
.ctx-tp{{font-family:var(--mono);font-size:9px;color:var(--ink3);width:84px}}
.ctx-bw{{width:60px;height:2px;background:var(--rule)}}
.ctx-bf{{height:2px}}
.ctx-dv{{font-family:var(--mono);font-size:9px;color:var(--ink3);width:22px;text-align:right}}
.plat-row{{display:flex;gap:8px;margin-top:10px;padding-top:10px;border-top:1px solid var(--rule);flex-wrap:wrap}}
.plat-pill{{font-family:var(--mono);font-size:9px;padding:3px 7px;border:1px solid var(--rule);color:var(--ink2)}}
@media(max-width:600px){{
  .lead-grid{{grid-template-columns:1fr}}
  .lead-data{{border-top:1px solid var(--rule)}}
  .sec-grid{{grid-template-columns:1fr}}
  .sec-item{{border-right:none;border-bottom:1px solid var(--rule)}}
  .exp-grid{{grid-template-columns:1fr 1fr}}
}}
</style>
</head>
<body>
<div class="wrap">

<div class="mast">
  <div class="wordmark">the <em>everywhere</em> chart</div>
  <div class="mast-right">
    <div class="mast-date">Week of {date_lbl}</div>
  </div>
</div>

<div class="section-lbl">This week's stories</div>

{"" if not lead else f'''
<div class="card" id="card-lead">
  <div class="lead-grid">
    <div class="lead-story">
      <div class="lead-eyebrow">
        <span class="etag">Lead story</span>
        <span class="emove">{move_label(lead)}</span>
      </div>
      <div class="lead-title">{lead["title"]}</div>
      <div class="lead-artist">{lead["artist"]}</div>
      <div class="lead-text">
        Reaching <strong>{lead["penCtx"]}</strong> with its strongest out-of-home signal
        in <strong>{lead["furthest"]}</strong> ({lead["furthestType"].replace("_", " ")}).
        {lead["signals"]} total signals across {lead["communities"]} communities this week.
      </div>
    </div>
    <div class="lead-data">
      <div class="lrank">1</div>
      <div class="lrank-sub">{move_label(lead)} &middot; {window_days}d window</div>
      <div class="dblock">
        <div class="dlbl">Penetration score</div>
        <div class="dval">{lead["score"]}</div>
      </div>
      <div class="dblock">
        <div class="dlbl">Community types</div>
        <div class="dval">{lead["penCtx"].split()[0]}</div>
      </div>
      <div class="dblock">
        <div class="dlbl">Active in</div>
        <div style="margin-top:3px">{lead_community_pills(lead)}</div>
      </div>
    </div>
  </div>
</div>
'''}

{"" if len(sec) < 2 else f'''
<div class="card" id="card-secondary">
  <div class="sec-grid">
    {sec_card(sec[0])}
    {sec_card(sec[1])}
  </div>
</div>
'''}

<div class="section-lbl" style="margin-top:8px">Chart</div>

<div class="card" style="border-top:1px solid var(--rule)" id="card-chart">
  <div class="chart-top">
    <div class="chart-tabs">
      <button class="ctab on">Crossover</button>
    </div>
    <div class="chart-meta">
      <div class="chart-desc">Out-of-home penetration score &middot; {window_days}-day rolling window</div>
      <button class="how-btn" onclick="toggleMethod()">How this works</button>
    </div>
    <div class="method-box" id="method-box">
      Songs are ranked by <strong>cultural penetration</strong> — intentional engagement
      across communities far from a song's home fanbase. A mention in a cooking forum
      outweighs thousands of plays in an artist's fan community.
      The diversity multiplier rewards songs reaching multiple distinct audience types.
    </div>
  </div>
  <div class="chart-cols">
    <span></span><span>Song</span><span>Reach</span><span style="text-align:right">Week</span>
  </div>
  <div id="chart-rows"></div>
</div>

</div>
<script>
const SONGS = {songs_js};
let openIdx = null;

function renderChart() {{
  const wrap = document.getElementById('chart-rows');
  wrap.innerHTML = '';
  SONGS.forEach((s, i) => {{
    const row = document.createElement('div');
    row.className = 'srow';
    row.innerHTML = `
      <div class="srank ${{s.rank <= 3 ? 't' : ''}}">${{s.rank}}</div>
      <div>
        <div class="stitle">${{s.title}}</div>
        <div class="sartist">${{s.artist}}</div>
        <div class="stags">${{s.tags.map(t => `<span class="stag ${{t.c}}">${{t.l}}</span>`).join('')}}</div>
      </div>
      <div>
        <div class="spen-lbl">Penetration</div>
        <div class="spen-bar"><div class="spen-fill" style="width:0%" data-width="${{s.pen}}%"></div></div>
        <div class="spen-ctx"><span>${{s.penCtx}}</span></div>
      </div>
      <div class="sdelta ${{s.dt}}">
        ${{s.delta}}
        <div class="sdelta-sub">${{s.prev ? 'was #' + s.prev : 'debut'}}</div>
      </div>
    `;
    row.addEventListener('click', () => toggle(i, s));

    const exp = document.createElement('div');
    exp.className = 'sexp';
    exp.id = 'exp-' + i;
    exp.innerHTML = `
      <div class="exp-grid">
        <div class="exp-card">
          <div class="ecl">Score</div>
          <div class="ecv">${{s.score}}</div>
          <div class="ecs">out-of-home weighted</div>
        </div>
        <div class="exp-card">
          <div class="ecl">Community types</div>
          <div class="ecv">${{s.penCtx.split(' ')[0]}}</div>
          <div class="ecs">distinct types</div>
        </div>
        <div class="exp-card">
          <div class="ecl">Furthest reach</div>
          <div class="ecv" style="font-size:11px;font-family:var(--mono);padding-top:2px">${{s.furthest}}</div>
          <div class="ecs">${{s.furthestType}} &middot; ${{s.communities}} communities</div>
        </div>
      </div>
      <div class="exp-lbl">Where it's being talked about</div>
      <div>
        ${{s.contexts.map(c => `
          <div class="ctx-item">
            <div class="ctx-dot" style="background:${{c.c}}"></div>
            <div class="ctx-nm">${{c.n}}</div>
            <div class="ctx-tp">${{c.t.replace('_',' ')}}</div>
            <div class="ctx-bw"><div class="ctx-bf" style="width:${{Math.round(c.d*100)}}%;background:${{c.c}}"></div></div>
            <div class="ctx-dv">${{c.d}}</div>
          </div>
        `).join('')}}
      </div>
      <div class="plat-row">
        ${{s.platforms.map(p => `<span class="plat-pill">${{p.p}}: ${{p.v}}</span>`).join('')}}
      </div>
    `;

    wrap.appendChild(row);
    wrap.appendChild(exp);
  }});

  requestAnimationFrame(() => {{
    document.querySelectorAll('[data-width]').forEach(el => {{
      setTimeout(() => {{ el.style.width = el.dataset.width; }}, 100);
    }});
  }});
}}

function toggle(i, s) {{
  const el = document.getElementById('exp-' + i);
  if (el.classList.contains('open')) {{
    el.classList.remove('open');
    openIdx = null;
  }} else {{
    if (openIdx !== null) document.getElementById('exp-' + openIdx).classList.remove('open');
    el.classList.add('open');
    openIdx = i;
  }}
}}

function toggleMethod() {{
  const b = document.getElementById('method-box');
  b.style.display = b.style.display === 'block' ? 'none' : 'block';
}}

renderChart();
</script>
</body>
</html>"""

# ── Render ────────────────────────────────────────────────────────────────────

df_raw = load_signals(window_days)
prev_scores = load_prev_scores(window_days)

if df_raw.empty:
    st.warning("No signal events found for this window. Try increasing the window size.")
    st.stop()

scored = compute_scores(
    df_raw, platform_weights, home_coeff, diversity_coeff,
    min_community_types, min_casual_weight, exclude_non_music,
)

if scored.empty:
    st.warning("No songs pass the current filters. Try relaxing min community types or casual weight.")
    st.stop()

# Top-line metrics in sidebar
with stats_placeholder:
    st.caption("📊 Current window stats")
    c1, c2 = st.columns(2)
    c1.metric("Songs scored", len(scored))
    c2.metric("Signal events", f"{len(df_raw):,}")
    c3, c4 = st.columns(2)
    c3.metric("Top score", f"{scored['penetration_score'].iloc[0]:.2f}")
    c4.metric("Avg types", f"{scored['community_type_count'].mean():.1f}")

# ── Editorial chart ────────────────────────────────────────────────────────────

songs_data = build_songs_data(scored, df_raw, prev_scores, chart_size)
chart_html = render_chart_html(songs_data, window_days)
components.html(chart_html, height=300 + chart_size * 80, scrolling=True)

# ── Raw signal explorer ───────────────────────────────────────────────────────

with st.expander("🔍 Raw signal events for a song"):
    song_options = scored.head(50).apply(
        lambda r: f"{r['artist_name']} — {r['song_title']}", axis=1
    ).tolist()
    selected = st.selectbox("Pick a song", song_options)
    if selected:
        idx = song_options.index(selected)
        song_id = scored.iloc[idx]["song_id"]
        raw = df_raw[df_raw["song_id"] == song_id][[
            "source_platform", "community_name", "community_type",
            "weighted_score", "is_home_community", "observed_at"
        ]].copy()
        raw["weighted_score"] = raw["weighted_score"].round(4)
        raw = raw.sort_values("weighted_score", ascending=False)
        st.dataframe(raw, use_container_width=True, hide_index=True)
