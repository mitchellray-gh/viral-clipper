"""
YouTube Analytics Feedback Loop
Polls the YouTube Analytics API for performance data on published Shorts:
  - CTR (click-through rate) — are thumbnails working?
  - Average view duration / retention — are clips holding attention?
  - Impressions — is YouTube distributing the content?
  - Estimated revenue (if monetized)

This data feeds back into:
  1. TrendMomentumTracker.niche_performance — which niches produce top clips
  2. Future virality scoring weight calibration
"""

import os
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ClipPerformance:
    youtube_id: str
    title: str
    trend_keyword: str
    impressions: int
    views: int
    ctr: float                 # 0.0 - 1.0 (impressionClickThroughRate)
    avg_view_duration_secs: float
    avg_view_percentage: float  # 0.0 - 100.0 (retention%)
    likes: int
    comments: int
    shares: int
    days_live: int


class AnalyticsFeedbackCollector:
    def __init__(self, config: dict, queue, momentum_tracker=None):
        self.config = config
        self.queue = queue
        self.momentum = momentum_tracker
        self.token_file = os.environ.get("YOUTUBE_TOKEN_FILE", "config/youtube_token.json")
        self._service = None

    def _get_service(self):
        """Get authenticated YouTube Analytics API client."""
        if self._service is None:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            from googleapiclient.discovery import build

            SCOPES = [
                "https://www.googleapis.com/auth/yt-analytics.readonly",
                "https://www.googleapis.com/auth/youtube.readonly"
            ]

            token_path = Path(self.token_file)
            if not token_path.exists():
                raise FileNotFoundError(
                    "YouTube token not found. Run the pipeline once to authenticate."
                )

            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                token_path.write_text(creds.to_json())

            self._service = build("youtubeAnalytics", "v2", credentials=creds)
        return self._service

    def collect_recent_performance(self, days_back: int = 7) -> list[ClipPerformance]:
        """
        Fetch performance metrics for all clips published in the last N days.
        Updates niche_performance in the momentum tracker.
        """
        from src.queue import ClipStatus

        published = self.queue.get_clips_by_status(ClipStatus.PUBLISHED, limit=200)
        if not published:
            logger.info("No published clips found for analytics collection")
            return []

        # Filter to only recently published
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back + 7)
        recent = [
            c for c in published
            if c.published_at and datetime.fromisoformat(c.published_at) > cutoff
        ]

        if not recent:
            logger.info("No recently published clips within analytics window")
            return []

        logger.info(f"Collecting analytics for {len(recent)} published clips...")
        results = []

        for clip in recent:
            if not clip.youtube_short_id:
                continue
            try:
                perf = self._fetch_clip_metrics(clip)
                if perf:
                    results.append(perf)
                    self._update_momentum(clip.trend_keyword, perf)
            except Exception as e:
                logger.warning(f"Analytics fetch failed for {clip.youtube_short_id}: {e}")

        self._log_performance_summary(results)
        return results

    def _fetch_clip_metrics(self, clip) -> Optional[ClipPerformance]:
        """Fetch YouTube Analytics metrics for a single video."""
        try:
            service = self._get_service()
        except Exception as e:
            logger.warning(f"Analytics service unavailable: {e}")
            return self._fetch_basic_stats(clip)

        try:
            published_date = datetime.fromisoformat(clip.published_at or datetime.now(timezone.utc).isoformat())
            start_date = published_date.strftime("%Y-%m-%d")
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            response = service.reports().query(
                ids="channel==MINE",
                startDate=start_date,
                endDate=end_date,
                metrics="views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,"
                        "impressions,impressionClickThroughRate,likes,comments,shares",
                filters=f"video=={clip.youtube_short_id}",
                dimensions="video"
            ).execute()

            rows = response.get("rows", [])
            if not rows:
                return None

            row = rows[0]
            days_live = max(1, (datetime.now(timezone.utc) - published_date).days)

            return ClipPerformance(
                youtube_id=clip.youtube_short_id,
                title=clip.title,
                trend_keyword=clip.trend_keyword,
                impressions=int(row[2]),
                views=int(row[1]),
                ctr=float(row[6]) / 100,   # API returns percentage
                avg_view_duration_secs=float(row[4]),
                avg_view_percentage=float(row[5]),
                likes=int(row[7]),
                comments=int(row[8]),
                shares=int(row[9]),
                days_live=days_live
            )

        except Exception as e:
            logger.debug(f"Analytics API error: {e}")
            return self._fetch_basic_stats(clip)

    def _fetch_basic_stats(self, clip) -> Optional[ClipPerformance]:
        """
        Fallback: use YouTube Data API v3 for basic stats (no CTR/retention).
        Still useful for view counts to feed niche performance tracking.
        """
        try:
            import googleapiclient.discovery as gd
            api_key = os.environ.get("YOUTUBE_API_KEY", "")
            if not api_key:
                return None

            yt = gd.build("youtube", "v3", developerKey=api_key)
            resp = yt.videos().list(
                part="statistics",
                id=clip.youtube_short_id
            ).execute()

            items = resp.get("items", [])
            if not items:
                return None

            stats = items[0].get("statistics", {})
            published_date = datetime.fromisoformat(
                clip.published_at or datetime.now(timezone.utc).isoformat()
            )
            days_live = max(1, (datetime.now(timezone.utc) - published_date).days)

            return ClipPerformance(
                youtube_id=clip.youtube_short_id,
                title=clip.title,
                trend_keyword=clip.trend_keyword,
                impressions=0,
                views=int(stats.get("viewCount", 0)),
                ctr=0.0,
                avg_view_duration_secs=0.0,
                avg_view_percentage=0.0,
                likes=int(stats.get("likeCount", 0)),
                comments=int(stats.get("commentCount", 0)),
                shares=0,
                days_live=days_live
            )
        except Exception as e:
            logger.debug(f"Basic stats fetch failed: {e}")
            return None

    def _update_momentum(self, trend_keyword: str, perf: ClipPerformance):
        """Feed clip performance back into niche_performance tracking."""
        if not self.momentum or not trend_keyword:
            return
        import re
        kw_norm = re.sub(r"[^a-z0-9 ]", "", trend_keyword.lower().strip())[:120]
        if kw_norm:
            self.momentum.update_niche_performance(
                keyword_normalized=kw_norm,
                views=perf.views,
                ctr=perf.ctr,
                retention=perf.avg_view_percentage / 100
            )

    def _log_performance_summary(self, results: list[ClipPerformance]):
        if not results:
            return
        total_views = sum(r.views for r in results)
        avg_ctr = sum(r.ctr for r in results) / len(results) if results else 0
        avg_ret = sum(r.avg_view_percentage for r in results) / len(results) if results else 0

        top = sorted(results, key=lambda r: r.views, reverse=True)[:3]
        logger.info(f"Analytics summary: {len(results)} clips | "
                    f"{total_views:,} total views | "
                    f"avg CTR={avg_ctr:.1%} | "
                    f"avg retention={avg_ret:.1f}%")
        for r in top:
            logger.info(f"  Top: '{r.title[:50]}' | {r.views:,} views | "
                        f"CTR={r.ctr:.1%} | ret={r.avg_view_percentage:.0f}%")

    def recommend_best_niches(self, top_n: int = 5) -> list[str]:
        """
        Return the top-performing niches based on historical analytics,
        for use in biasing discovery toward proven profitable topics.
        """
        if not self.momentum:
            return []
        strong = self.momentum.get_historically_strong_niches()
        return [n["keyword_normalized"] for n in strong[:top_n]]
