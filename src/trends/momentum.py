"""
Trend Momentum Tracker
Stores trend scores over time in SQLite and computes:
  - Velocity: how fast a score is rising right now
  - Acceleration: is the velocity itself increasing?
  - Niche phase: new / rising / peak / declining
  - Breakout signal: sustained multi-source, high-velocity topic

This is the most predictive signal for finding a niche BEFORE it peaks.
"""

import sqlite3
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# Minimum hours between momentum snapshots for the same keyword
_SNAPSHOT_INTERVAL_HOURS = 3


@dataclass
class MomentumSignal:
    keyword: str
    current_score: float
    velocity: float          # score delta per hour (positive = rising)
    acceleration: float      # velocity delta — is it accelerating?
    phase: str               # "new" / "rising" / "peak" / "declining" / "dead"
    breakout: bool           # True if this is a high-confidence breakout signal
    source_count: int
    first_seen_hours_ago: float
    snapshots: int           # how many data points collected
    momentum_score: float    # composite 0.0-1.0 for use in pipeline ranking


# Phase thresholds
_VELOCITY_RISING = 0.05     # score/hour — above this = rising
_VELOCITY_PEAK = 0.01       # near-zero velocity at high score = peaked
_VELOCITY_DECLINING = -0.02  # negative = declining
_BREAKOUT_VELOCITY = 0.12   # fast_rising above this = breakout
_BREAKOUT_MIN_SOURCES = 3   # must appear in this many sources to be breakout


class TrendMomentumTracker:
    def __init__(self, db_path: str = "data/queue.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trend_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword_normalized TEXT NOT NULL,
                    keyword_display TEXT NOT NULL,
                    score REAL NOT NULL,
                    source_count INTEGER DEFAULT 1,
                    sources_json TEXT DEFAULT '[]',
                    category TEXT DEFAULT '',
                    recorded_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_kw_time
                ON trend_snapshots(keyword_normalized, recorded_at)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS niche_performance (
                    keyword_normalized TEXT PRIMARY KEY,
                    total_clips_made INTEGER DEFAULT 0,
                    total_views INTEGER DEFAULT 0,
                    avg_ctr REAL DEFAULT 0.0,
                    avg_retention REAL DEFAULT 0.0,
                    last_updated TEXT
                )
            """)
            conn.commit()

    def record_snapshots(self, topics: list):
        """
        Persist a batch of TrendingTopic objects as time-series snapshots.
        Avoids duplicate snapshots within the interval window.
        """
        import re
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=_SNAPSHOT_INTERVAL_HOURS)).isoformat()

        with self._get_conn() as conn:
            for topic in topics:
                kw_norm = re.sub(r"[^a-z0-9 ]", "", topic.keyword.lower().strip())[:120]
                if not kw_norm:
                    continue

                # Check if we already have a recent snapshot for this keyword
                existing = conn.execute(
                    "SELECT id FROM trend_snapshots WHERE keyword_normalized=? AND recorded_at>? LIMIT 1",
                    (kw_norm, cutoff)
                ).fetchone()

                if existing:
                    # Update score if higher (same window)
                    conn.execute(
                        "UPDATE trend_snapshots SET score=MAX(score,?), source_count=MAX(source_count,?), "
                        "sources_json=? WHERE id=?",
                        (
                            topic.score,
                            topic.metadata.get("source_count", 1),
                            json.dumps(topic.metadata.get("sources", [])),
                            existing["id"]
                        )
                    )
                else:
                    conn.execute(
                        "INSERT INTO trend_snapshots "
                        "(keyword_normalized, keyword_display, score, source_count, sources_json, category, recorded_at) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (
                            kw_norm,
                            topic.keyword[:200],
                            topic.score,
                            topic.metadata.get("source_count", 1),
                            json.dumps(topic.metadata.get("sources", [])),
                            topic.category,
                            now.isoformat()
                        )
                    )
            conn.commit()

        # Prune old snapshots (keep 7 days)
        self._prune_old(days=7)

    def compute_momentum(self, topics: list) -> dict[str, MomentumSignal]:
        """
        For each topic in the list, compute momentum signals using historical data.
        Returns dict keyed by normalized keyword.
        """
        import re
        now = datetime.now(timezone.utc)
        signals = {}

        with self._get_conn() as conn:
            for topic in topics:
                kw_norm = re.sub(r"[^a-z0-9 ]", "", topic.keyword.lower().strip())[:120]
                if not kw_norm:
                    continue

                # Get all snapshots for this keyword in last 48h
                rows = conn.execute(
                    "SELECT score, source_count, sources_json, recorded_at FROM trend_snapshots "
                    "WHERE keyword_normalized=? AND recorded_at>? ORDER BY recorded_at ASC",
                    (kw_norm, (now - timedelta(hours=48)).isoformat())
                ).fetchall()

                signal = self._compute_signal(kw_norm, topic, rows, now)
                signals[kw_norm] = signal

        return signals

    def _compute_signal(self, kw_norm: str, topic, rows: list, now: datetime) -> MomentumSignal:
        current_score = topic.score
        source_count = topic.metadata.get("source_count", 1)

        if not rows:
            # First time we've seen this — treat as new, unknown velocity
            return MomentumSignal(
                keyword=topic.keyword,
                current_score=current_score,
                velocity=0.0,
                acceleration=0.0,
                phase="new",
                breakout=False,
                source_count=source_count,
                first_seen_hours_ago=0.0,
                snapshots=0,
                momentum_score=min(1.0, current_score * 0.7 + 0.2)  # new = potential bonus
            )

        # Parse timestamps
        times = []
        scores = []
        for row in rows:
            try:
                dt = datetime.fromisoformat(row["recorded_at"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                hours_ago = (now - dt).total_seconds() / 3600
                times.append(hours_ago)
                scores.append(row["score"])
            except Exception:
                pass

        # Most recent is last snapshot; current is now
        times.insert(0, 0.0)
        scores.insert(0, current_score)

        # Reverse so time[0] is oldest, time[-1] is newest (current)
        times = times[::-1]
        scores = scores[::-1]

        first_seen_hours_ago = times[0] if times else 0.0

        # Velocity: linear regression slope (score/hour) over last 24h
        velocity = self._linear_slope(times, scores)

        # Acceleration: slope of velocity over two windows (12h each)
        acceleration = 0.0
        if len(scores) >= 4:
            mid = len(scores) // 2
            v1 = self._linear_slope(times[:mid], scores[:mid])
            v2 = self._linear_slope(times[mid:], scores[mid:])
            acceleration = v2 - v1

        # Phase classification
        phase = self._classify_phase(current_score, velocity, acceleration, len(rows))

        # Breakout detection:
        # High velocity + multi-source + accelerating + seen recently
        max_source_count = max(row["source_count"] for row in rows) if rows else source_count
        breakout = (
            velocity >= _BREAKOUT_VELOCITY
            and max_source_count >= _BREAKOUT_MIN_SOURCES
            and phase in ("new", "rising")
            and acceleration >= 0
        )

        # Composite momentum score
        # Heavily rewards velocity + multi-source + early phase
        phase_bonus = {"new": 0.25, "rising": 0.20, "peak": 0.0, "declining": -0.20, "dead": -0.40}.get(phase, 0.0)
        velocity_contribution = min(0.40, max(0.0, velocity * 3))  # velocity up to +0.40
        breakout_bonus = 0.15 if breakout else 0.0
        source_bonus = min(0.15, (max_source_count - 1) * 0.05)

        momentum_score = max(0.0, min(1.0,
            current_score * 0.35
            + velocity_contribution
            + phase_bonus
            + source_bonus
            + breakout_bonus
        ))

        return MomentumSignal(
            keyword=topic.keyword,
            current_score=current_score,
            velocity=round(velocity, 4),
            acceleration=round(acceleration, 4),
            phase=phase,
            breakout=breakout,
            source_count=max_source_count,
            first_seen_hours_ago=round(first_seen_hours_ago, 1),
            snapshots=len(rows),
            momentum_score=round(momentum_score, 3)
        )

    def _classify_phase(self, score: float, velocity: float, acceleration: float, n_snapshots: int) -> str:
        if n_snapshots < 2:
            return "new"
        if velocity >= _VELOCITY_RISING and score < 0.75:
            return "rising"
        if velocity >= _VELOCITY_RISING and score >= 0.75:
            return "peak"  # high score + still rising slightly = near peak
        if abs(velocity) <= _VELOCITY_PEAK and score >= 0.60:
            return "peak"
        if velocity <= _VELOCITY_DECLINING:
            return "declining" if score > 0.20 else "dead"
        return "rising"  # default for ambiguous

    def _linear_slope(self, times: list[float], scores: list[float]) -> float:
        """Simple linear regression slope (score per hour)."""
        n = len(times)
        if n < 2:
            return 0.0
        # x = hours_since_first (reversed: 0=oldest, n=newest)
        x = [times[-1] - t for t in times]  # oldest → largest x
        y = scores
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        den = sum((xi - mean_x) ** 2 for xi in x)
        return num / den if den > 0 else 0.0

    def get_breakout_topics(self, limit: int = 10) -> list[str]:
        """Return normalized keywords of current breakout topics."""
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=6)).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT keyword_display FROM trend_snapshots "
                "WHERE recorded_at>? AND source_count>=? ORDER BY score DESC LIMIT ?",
                (cutoff, _BREAKOUT_MIN_SOURCES, limit)
            ).fetchall()
        return [r["keyword_display"] for r in rows]

    def update_niche_performance(self, keyword_normalized: str, views: int, ctr: float, retention: float):
        """Store real-world performance data for a niche after clips are published."""
        now = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO niche_performance (keyword_normalized, total_views, avg_ctr, avg_retention, total_clips_made, last_updated)
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(keyword_normalized) DO UPDATE SET
                    total_views = total_views + excluded.total_views,
                    avg_ctr = (avg_ctr * total_clips_made + excluded.avg_ctr) / (total_clips_made + 1),
                    avg_retention = (avg_retention * total_clips_made + excluded.avg_retention) / (total_clips_made + 1),
                    total_clips_made = total_clips_made + 1,
                    last_updated = excluded.last_updated
            """, (keyword_normalized, views, ctr, retention, now))
            conn.commit()

    def get_historically_strong_niches(self) -> list[dict]:
        """Return niches that have historically produced high-performing clips."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM niche_performance WHERE total_clips_made >= 2 "
                "ORDER BY (avg_ctr * avg_retention) DESC LIMIT 20"
            ).fetchall()
        return [dict(r) for r in rows]

    def _prune_old(self, days: int = 7):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._get_conn() as conn:
            conn.execute("DELETE FROM trend_snapshots WHERE recorded_at<?", (cutoff,))
            conn.commit()
