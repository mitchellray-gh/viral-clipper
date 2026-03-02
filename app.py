"""
Viral Clipper — Streamlit Dashboard
====================================
Run with:  streamlit run app.py
"""

import os
import sys
import json
import sqlite3
import subprocess
import threading
import queue as q_mod
from pathlib import Path
from datetime import datetime, timezone

# ── Ensure project root is on sys.path ───────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# ── Add ffmpeg to PATH if not already there ───────────────────────────────────
FFMPEG_BIN = r"C:\ffmpeg\ffmpeg-8.0.1-essentials_build\bin"
if Path(FFMPEG_BIN).exists() and FFMPEG_BIN not in os.environ.get("PATH", ""):
    os.environ["PATH"] = FFMPEG_BIN + os.pathsep + os.environ.get("PATH", "")

import streamlit as st
from dotenv import load_dotenv, set_key, dotenv_values

load_dotenv(str(ROOT / ".env"))
load_dotenv(str(ROOT / "config" / ".env"), override=False)

import yaml

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Viral Clipper",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Load config ───────────────────────────────────────────────────────────────
@st.cache_resource
def load_config():
    cfg_path = ROOT / "config" / "settings.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()
DB_PATH = ROOT / CONFIG.get("queue", {}).get("db_path", "data/queue.db")

# ── Sidebar navigation ────────────────────────────────────────────────────────
st.sidebar.title("🎬 Viral Clipper")
st.sidebar.markdown("---")
PAGE = st.sidebar.radio(
    "Navigate",
    ["📊 Dashboard", "🔥 Trends", "▶️ Run Pipeline", "📋 Queue", "⚙️ Settings"],
    label_visibility="collapsed",
)
st.sidebar.markdown("---")
st.sidebar.caption("github.com/mitchellray-gh/viral-clipper")


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_db_conn():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def get_queue_stats():
    conn = get_db_conn()
    if not conn:
        return {}
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM clips GROUP BY status"
        ).fetchall()
        stats = {r["status"]: r["cnt"] for r in rows}
        total = sum(stats.values())
        processed = conn.execute(
            "SELECT COUNT(*) as cnt FROM processed_videos"
        ).fetchone()["cnt"]
        stats["_total"] = total
        stats["_videos_processed"] = processed
        return stats
    finally:
        conn.close()


def get_recent_clips(limit=50, status_filter=None):
    conn = get_db_conn()
    if not conn:
        return []
    try:
        if status_filter and status_filter != "All":
            rows = conn.execute(
                "SELECT * FROM clips WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status_filter, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM clips ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_trend_snapshots(limit=200):
    conn = get_db_conn()
    if not conn:
        return []
    try:
        if not conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trend_snapshots'"
        ).fetchone():
            return []
        rows = conn.execute(
            """SELECT keyword, source, score, phase, velocity, breakout, recorded_at
               FROM trend_snapshots
               ORDER BY recorded_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def status_badge(status: str) -> str:
    colors = {
        "published": "🟢",
        "scheduled": "🔵",
        "metadata_ready": "🟣",
        "edited": "🟡",
        "scored": "🟡",
        "transcribed": "⚪",
        "downloaded": "⚪",
        "discovered": "⚪",
        "failed": "🔴",
        "skipped": "⬛",
    }
    return colors.get(status, "⚪") + " " + status


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

if PAGE == "📊 Dashboard":
    st.title("📊 Dashboard")

    stats = get_queue_stats()

    # ── Metric cards ─────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Clips", stats.get("_total", 0))
    c2.metric("Published", stats.get("published", 0))
    c3.metric("Scheduled", stats.get("scheduled", 0))
    c4.metric("Ready to Edit", stats.get("metadata_ready", stats.get("scored", 0)))
    c5.metric("Videos Processed", stats.get("_videos_processed", 0))

    st.markdown("---")

    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Recent Clips")
        clips = get_recent_clips(limit=20)
        if clips:
            import pandas as pd
            df = pd.DataFrame(clips)[
                ["title", "trend_keyword", "virality_score", "status", "created_at"]
            ].rename(columns={
                "trend_keyword": "trend",
                "virality_score": "score",
                "created_at": "added",
            })
            df["score"] = df["score"].apply(lambda x: f"{x:.2f}" if x else "—")
            df["added"] = pd.to_datetime(df["added"]).dt.strftime("%m/%d %H:%M")
            df["title"] = df["title"].fillna("").str[:60]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No clips yet — run the pipeline to get started.")

    with col_right:
        st.subheader("Queue Breakdown")
        if stats:
            import pandas as pd
            filtered = {k: v for k, v in stats.items() if not k.startswith("_")}
            if filtered:
                df_s = pd.DataFrame(
                    [{"Status": k, "Count": v} for k, v in filtered.items()]
                ).sort_values("Count", ascending=False)
                st.dataframe(df_s, hide_index=True, use_container_width=True)
            else:
                st.info("Queue is empty.")

    # ── Log tail ─────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📄 Recent Log")
    log_path = ROOT / CONFIG.get("logging", {}).get("file", "logs/pipeline.log")
    if log_path.exists():
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = "".join(lines[-60:])
        st.code(tail, language=None)
    else:
        st.info("No log file yet.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Trends
# ═══════════════════════════════════════════════════════════════════════════════

elif PAGE == "🔥 Trends":
    st.title("🔥 Trending Topics")

    tab_live, tab_history = st.tabs(["Live Fetch", "Snapshot History"])

    with tab_live:
        st.caption("Fetches trends from RSS, Hacker News, Google Trends, Reddit, and YouTube Trending in real time.")
        if st.button("🔄 Fetch Trends Now", type="primary"):
            with st.spinner("Fetching trends from all sources..."):
                try:
                    from src.trends import TrendAggregator
                    agg = TrendAggregator(CONFIG)
                    topics = agg.fetch_all()

                    import pandas as pd
                    rows = []
                    for t in topics:
                        rows.append({
                            "keyword": t.keyword[:80],
                            "source": t.source,
                            "score": round(t.score, 3),
                            "momentum": round(getattr(t, "momentum_score", t.score), 3),
                            "velocity": round(getattr(t, "velocity", 0.0), 4),
                            "phase": getattr(t, "phase", "—"),
                            "breakout": "🔥" if getattr(t, "breakout", False) else "",
                        })
                    df = pd.DataFrame(rows)
                    st.success(f"Found **{len(topics)}** trending topics")

                    # Breakouts first
                    breakouts = df[df["breakout"] == "🔥"]
                    if not breakouts.empty:
                        st.markdown("#### 🔥 Breakout Topics")
                        st.dataframe(breakouts, hide_index=True, use_container_width=True)

                    st.markdown("#### All Topics (ranked by momentum score)")
                    st.dataframe(df, hide_index=True, use_container_width=True)

                except Exception as e:
                    st.error(f"Fetch failed: {e}")

    with tab_history:
        st.caption("Historical trend snapshots stored by the momentum tracker.")
        rows = get_trend_snapshots(limit=500)
        if rows:
            import pandas as pd
            df = pd.DataFrame(rows)
            df["recorded_at"] = pd.to_datetime(df["recorded_at"]).dt.strftime("%m/%d %H:%M")
            df["breakout"] = df["breakout"].apply(lambda x: "🔥" if x else "")
            st.dataframe(df, hide_index=True, use_container_width=True)
        else:
            st.info("No snapshots yet — run the pipeline at least once to build history.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Run Pipeline
# ═══════════════════════════════════════════════════════════════════════════════

elif PAGE == "▶️ Run Pipeline":
    st.title("▶️ Run Pipeline")

    st.markdown("""
    Each button runs the pipeline as a subprocess and streams live log output below.
    The full run goes: **Trends → Discover → Download → Transcribe → Score → Edit → Queue → Publish**
    """)

    col1, col2, col3 = st.columns(3)
    run_full = col1.button("🚀 Full Pipeline Run", type="primary", use_container_width=True)
    run_publish = col2.button("📤 Publish Due Clips", use_container_width=True)
    run_status = col3.button("📋 Show Status", use_container_width=True)

    col4, col5 = st.columns(2)
    run_discover = col4.button("🔍 Discover Only", use_container_width=True)
    run_schedule = col5.button("🗓️ Schedule Queue", use_container_width=True)

    mode = None
    if run_full:
        mode = "run"
    elif run_publish:
        mode = "publish"
    elif run_status:
        mode = "status"
    elif run_discover:
        mode = "discover"
    elif run_schedule:
        mode = "schedule"

    if mode:
        st.markdown("---")
        st.subheader(f"Output — `python pipeline.py {mode}`")
        log_box = st.empty()
        output_lines = []

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        if FFMPEG_BIN not in env.get("PATH", ""):
            env["PATH"] = FFMPEG_BIN + os.pathsep + env.get("PATH", "")

        proc = subprocess.Popen(
            [sys.executable, str(ROOT / "pipeline.py"), mode],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        for line in proc.stdout:
            output_lines.append(line.rstrip())
            # Keep last 200 lines visible
            visible = "\n".join(output_lines[-200:])
            log_box.code(visible, language=None)

        proc.wait()
        rc = proc.returncode
        if rc == 0:
            st.success("Pipeline completed successfully.")
        else:
            st.warning(f"Pipeline exited with code {rc} (warnings above may be non-fatal).")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Queue
# ═══════════════════════════════════════════════════════════════════════════════

elif PAGE == "📋 Queue":
    st.title("📋 Content Queue")

    ALL_STATUSES = [
        "All", "published", "scheduled", "metadata_ready", "edited",
        "scored", "transcribed", "downloaded", "discovered", "failed", "skipped"
    ]

    col_filter, col_limit = st.columns([3, 1])
    status_filter = col_filter.selectbox("Filter by status", ALL_STATUSES)
    limit = col_limit.number_input("Rows", min_value=10, max_value=500, value=100, step=10)

    clips = get_recent_clips(limit=int(limit), status_filter=status_filter)

    if not clips:
        st.info("No clips found.")
    else:
        import pandas as pd

        df = pd.DataFrame(clips)

        # Display columns
        display_cols = [
            "id", "title", "trend_keyword", "virality_score",
            "status", "scheduled_at", "published_at", "youtube_short_id", "created_at"
        ]
        df_display = df[[c for c in display_cols if c in df.columns]].copy()
        df_display["virality_score"] = df_display["virality_score"].apply(
            lambda x: f"{x:.2f}" if x else "—"
        )
        df_display["title"] = df_display["title"].fillna("").str[:70]

        st.caption(f"Showing {len(df_display)} clips")
        st.dataframe(df_display, hide_index=True, use_container_width=True)

        # ── Clip detail expander ──────────────────────────────────────────────
        st.markdown("---")
        st.subheader("Clip Detail")
        clip_ids = [c["id"] for c in clips]
        selected_id = st.selectbox("Select a clip to inspect", clip_ids)
        if selected_id:
            clip = next((c for c in clips if c["id"] == selected_id), None)
            if clip:
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(f"**Title:** {clip.get('title') or '—'}")
                    st.markdown(f"**Trend:** {clip.get('trend_keyword') or '—'}")
                    st.markdown(f"**Status:** {status_badge(clip.get('status',''))}")
                    st.markdown(f"**Virality Score:** {clip.get('virality_score', 0):.2f}")
                    st.markdown(f"**Clip Window:** {clip.get('start_time',0):.1f}s – {clip.get('end_time',0):.1f}s")
                    if clip.get("youtube_short_id"):
                        yt_url = f"https://www.youtube.com/shorts/{clip['youtube_short_id']}"
                        st.markdown(f"**YouTube:** [{yt_url}]({yt_url})")
                with c2:
                    if clip.get("description"):
                        st.markdown("**Description:**")
                        st.text(clip["description"][:400])
                    meta = {}
                    if clip.get("metadata_json"):
                        try:
                            meta = json.loads(clip["metadata_json"])
                        except Exception:
                            pass
                    if meta:
                        st.markdown("**AI Metadata:**")
                        if meta.get("reasoning"):
                            st.info(meta["reasoning"])
                        if meta.get("hook_phrase"):
                            st.markdown(f"Hook: *\"{meta['hook_phrase']}\"*")

                clip_path = clip.get("clip_path", "")
                if clip_path and Path(clip_path).exists():
                    st.markdown("**Preview:**")
                    st.video(clip_path)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Settings
# ═══════════════════════════════════════════════════════════════════════════════

elif PAGE == "⚙️ Settings":
    st.title("⚙️ Settings")

    env_path = ROOT / ".env"
    tab_creds, tab_cfg = st.tabs(["🔑 API Credentials", "📝 Pipeline Config"])

    with tab_creds:
        st.caption(f"Editing `{env_path}` — values are saved immediately on submit.")

        current = dotenv_values(str(env_path)) if env_path.exists() else {}

        with st.form("creds_form"):
            google_key = st.text_input(
                "GOOGLE_API_KEY (Gemini)",
                value=current.get("GOOGLE_API_KEY", ""),
                type="password",
                help="Used for Gemini 2.0 Flash — get it at aistudio.google.com"
            )
            yt_key = st.text_input(
                "YOUTUBE_API_KEY (Data API v3)",
                value=current.get("YOUTUBE_API_KEY", ""),
                type="password",
                help="Separate key from Google Cloud Console with YouTube Data API v3 enabled"
            )
            yt_channel = st.text_input(
                "YOUTUBE_CHANNEL_ID",
                value=current.get("YOUTUBE_CHANNEL_ID", ""),
                help="Your channel ID, e.g. UCxxxxxxxxxx"
            )
            reddit_id = st.text_input(
                "REDDIT_CLIENT_ID",
                value=current.get("REDDIT_CLIENT_ID", ""),
            )
            reddit_secret = st.text_input(
                "REDDIT_CLIENT_SECRET",
                value=current.get("REDDIT_CLIENT_SECRET", ""),
                type="password",
            )
            reddit_agent = st.text_input(
                "REDDIT_USER_AGENT",
                value=current.get("REDDIT_USER_AGENT", "ViralClipper/1.0"),
            )
            newsdata_key = st.text_input(
                "NEWSDATA_API_KEY",
                value=current.get("NEWSDATA_API_KEY", ""),
                type="password",
                help="Optional — free tier at newsdata.io"
            )

            submitted = st.form_submit_button("💾 Save Credentials", type="primary")

        if submitted:
            env_path.touch()
            field_map = {
                "GOOGLE_API_KEY": google_key,
                "YOUTUBE_API_KEY": yt_key,
                "YOUTUBE_CHANNEL_ID": yt_channel,
                "REDDIT_CLIENT_ID": reddit_id,
                "REDDIT_CLIENT_SECRET": reddit_secret,
                "REDDIT_USER_AGENT": reddit_agent,
                "NEWSDATA_API_KEY": newsdata_key,
            }
            for key, val in field_map.items():
                set_key(str(env_path), key, val)
            st.success("Credentials saved to `.env`")
            load_dotenv(str(env_path), override=True)

        # ── API health check ──────────────────────────────────────────────────
        st.markdown("---")
        st.subheader("API Health Check")
        if st.button("🩺 Test API Keys"):
            results = {}

            # Gemini
            try:
                from google import genai
                client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY", ""))
                resp = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents="Reply with just the word OK"
                )
                results["Gemini 2.0 Flash"] = ("✅", resp.text.strip()[:50])
            except Exception as e:
                results["Gemini 2.0 Flash"] = ("❌", str(e)[:100])

            # YouTube Data API
            try:
                from googleapiclient.discovery import build
                yt = build("youtube", "v3", developerKey=os.environ.get("YOUTUBE_API_KEY", ""))
                r = yt.videos().list(part="id", chart="mostPopular", maxResults=1).execute()
                results["YouTube Data API v3"] = ("✅", f"Got {len(r.get('items',[]))} item(s)")
            except Exception as e:
                results["YouTube Data API v3"] = ("❌", str(e)[:100])

            for svc, (icon, msg) in results.items():
                st.markdown(f"{icon} **{svc}**: {msg}")

    with tab_cfg:
        st.caption("Current `config/settings.yaml` (read-only preview)")
        cfg_path = ROOT / "config" / "settings.yaml"
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                content = f.read()
            st.code(content, language="yaml")
        else:
            st.error("settings.yaml not found.")
