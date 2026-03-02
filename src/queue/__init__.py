"""
Content Queue
SQLite-backed queue for managing the pipeline's content backlog:
- Tracks clips from discovery through editing through publication
- Manages the posting schedule
- Prevents duplicate uploads
"""

import sqlite3
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ClipStatus(str, Enum):
    DISCOVERED = "discovered"       # Video URL found, not yet downloaded
    DOWNLOADED = "downloaded"       # Raw video downloaded
    TRANSCRIBED = "transcribed"     # Transcript available
    SCORED = "scored"               # Virality scored, clip boundaries identified
    EDITED = "edited"               # Short video produced
    METADATA_READY = "metadata_ready"  # Title/tags/desc generated
    SCHEDULED = "scheduled"         # Assigned a publish datetime
    PUBLISHED = "published"         # Successfully uploaded
    FAILED = "failed"               # Error at some stage
    SKIPPED = "skipped"             # Below threshold, skipped


@dataclass
class QueuedClip:
    id: str                         # UUID
    video_id: str                   # YouTube source video ID
    source_url: str
    source_title: str
    trend_keyword: str
    start_time: float
    end_time: float
    virality_score: float
    status: str
    clip_path: str = ""             # Path to edited short MP4
    title: str = ""
    description: str = ""
    tags: str = ""                  # JSON list
    hashtags: str = ""              # JSON list
    category_id: str = "22"
    scheduled_at: Optional[str] = None
    published_at: Optional[str] = None
    youtube_short_id: str = ""      # Uploaded video ID
    error_message: str = ""
    created_at: str = ""
    updated_at: str = ""
    metadata_json: str = ""         # Extra metadata as JSON

    def get_tags(self) -> list:
        try:
            return json.loads(self.tags) if self.tags else []
        except Exception:
            return []

    def get_hashtags(self) -> list:
        try:
            return json.loads(self.hashtags) if self.hashtags else []
        except Exception:
            return []

    def get_metadata(self) -> dict:
        try:
            return json.loads(self.metadata_json) if self.metadata_json else {}
        except Exception:
            return {}


class ContentQueue:
    def __init__(self, config: dict):
        self.cfg = config.get("queue", {})
        self.db_path = self.cfg.get("db_path", "data/queue.db")
        self.max_backlog = self.cfg.get("max_backlog", 500)
        self.posts_per_day = self.cfg.get("posts_per_day", 3)
        self.posting_times = self.cfg.get("posting_times", [14, 18, 23])
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS clips (
                    id TEXT PRIMARY KEY,
                    video_id TEXT NOT NULL,
                    source_url TEXT,
                    source_title TEXT,
                    trend_keyword TEXT,
                    start_time REAL,
                    end_time REAL,
                    virality_score REAL,
                    status TEXT NOT NULL DEFAULT 'discovered',
                    clip_path TEXT,
                    title TEXT,
                    description TEXT,
                    tags TEXT,
                    hashtags TEXT,
                    category_id TEXT DEFAULT '22',
                    scheduled_at TEXT,
                    published_at TEXT,
                    youtube_short_id TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata_json TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_clips_status ON clips(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_clips_video_start ON clips(video_id, start_time)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_videos (
                    video_id TEXT PRIMARY KEY,
                    processed_at TEXT NOT NULL,
                    clip_count INTEGER DEFAULT 0
                )
            """)
            conn.commit()
        logger.info(f"Queue database initialized at {self.db_path}")

    def add_clip(self, clip) -> str:
        """Add a ClipCandidate to the queue. Returns the clip ID."""
        import uuid
        clip_id = str(uuid.uuid4())[:8] + f"_{clip.video_id}_{int(clip.start_time)}"
        now = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            # Check for duplicate (same video, overlapping start time window)
            existing = conn.execute(
                "SELECT id FROM clips WHERE video_id=? AND ABS(start_time-?)< 5",
                (clip.video_id, clip.start_time)
            ).fetchone()

            if existing:
                logger.debug(f"Duplicate clip skipped for {clip.video_id} @ {clip.start_time}s")
                return existing["id"]

            conn.execute("""
                INSERT INTO clips (
                    id, video_id, source_url, source_title, trend_keyword,
                    start_time, end_time, virality_score, status,
                    created_at, updated_at, metadata_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                clip_id, clip.video_id, clip.source_url, "",
                clip.trend_keyword, clip.start_time, clip.end_time,
                clip.virality_score, ClipStatus.SCORED,
                now, now, json.dumps({
                    "hook_phrase": clip.hook_phrase,
                    "title_suggestion": clip.title_suggestion,
                    "reasoning": clip.reasoning,
                    "hook_score": clip.hook_score,
                    "emotional_score": clip.emotional_score,
                    "info_score": clip.info_score
                })
            ))
            conn.commit()

        return clip_id

    def update_status(self, clip_id: str, status: ClipStatus, **kwargs):
        """Update a clip's status and any additional fields."""
        now = datetime.now(timezone.utc).isoformat()
        valid_fields = {
            "clip_path", "title", "description", "tags", "hashtags",
            "category_id", "scheduled_at", "published_at",
            "youtube_short_id", "error_message", "source_title"
        }
        updates = {"status": status.value, "updated_at": now}
        for k, v in kwargs.items():
            if k in valid_fields:
                if isinstance(v, (list, dict)):
                    v = json.dumps(v)
                updates[k] = v

        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [clip_id]

        with self._get_conn() as conn:
            conn.execute(f"UPDATE clips SET {set_clause} WHERE id=?", values)
            conn.commit()

    def get_clips_by_status(self, status: ClipStatus, limit: int = 50) -> list[QueuedClip]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM clips WHERE status=? ORDER BY virality_score DESC LIMIT ?",
                (status.value, limit)
            ).fetchall()
        return [QueuedClip(**dict(row)) for row in rows]

    def get_clip(self, clip_id: str) -> Optional[QueuedClip]:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM clips WHERE id=?", (clip_id,)).fetchone()
        return QueuedClip(**dict(row)) if row else None

    def schedule_upcoming(self):
        """
        Assign scheduled_at datetimes to edited+metadata-ready clips
        following the configured posting_times schedule.
        Already-scheduled/published clips are preserved.
        """
        ready = self.get_clips_by_status(ClipStatus.METADATA_READY)
        if not ready:
            return

        # Find next available posting slots
        slots = self._next_available_slots(len(ready))

        for clip, slot in zip(ready, slots):
            self.update_status(
                clip.id, ClipStatus.SCHEDULED,
                scheduled_at=slot.isoformat()
            )
            logger.info(f"Scheduled clip {clip.id} for {slot.isoformat()}")

    def get_due_clips(self) -> list[QueuedClip]:
        """Get clips that are scheduled for now or past-due."""
        now = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM clips WHERE status=? AND scheduled_at<=? ORDER BY scheduled_at",
                (ClipStatus.SCHEDULED.value, now)
            ).fetchall()
        return [QueuedClip(**dict(row)) for row in rows]

    def _next_available_slots(self, count: int) -> list[datetime]:
        """Generate next N available posting slots, avoiding already-scheduled times."""
        with self._get_conn() as conn:
            taken = set(
                row[0] for row in conn.execute(
                    "SELECT scheduled_at FROM clips WHERE status IN (?,?) AND scheduled_at IS NOT NULL",
                    (ClipStatus.SCHEDULED.value, ClipStatus.PUBLISHED.value)
                ).fetchall()
            )

        slots = []
        now = datetime.now(timezone.utc)
        check_date = now.date()

        while len(slots) < count:
            for hour in sorted(self.posting_times):
                slot = datetime(
                    check_date.year, check_date.month, check_date.day,
                    hour, 0, 0, tzinfo=timezone.utc
                )
                if slot > now and slot.isoformat() not in taken:
                    slots.append(slot)
                    taken.add(slot.isoformat())
                    if len(slots) >= count:
                        break
            check_date += timedelta(days=1)

        return slots[:count]

    def mark_video_processed(self, video_id: str, clip_count: int):
        now = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO processed_videos (video_id, processed_at, clip_count) VALUES (?,?,?)",
                (video_id, now, clip_count)
            )
            conn.commit()

    def is_video_processed(self, video_id: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT video_id FROM processed_videos WHERE video_id=?", (video_id,)
            ).fetchone()
        return row is not None

    def get_stats(self) -> dict:
        """Return queue statistics."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM clips GROUP BY status"
            ).fetchall()
            total_published = conn.execute(
                "SELECT COUNT(*) FROM clips WHERE status='published'"
            ).fetchone()[0]
        stats = {row["status"]: row["cnt"] for row in rows}
        stats["total_published"] = total_published
        stats["backlog"] = stats.get(ClipStatus.SCHEDULED.value, 0) + stats.get(ClipStatus.METADATA_READY.value, 0)
        return stats
