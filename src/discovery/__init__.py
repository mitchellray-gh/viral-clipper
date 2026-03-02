"""
Content Discovery Module
Searches YouTube for videos related to trending topics,
scores them for clippability, and returns a prioritized list.
"""

import os
import logging
import time
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredVideo:
    video_id: str
    url: str
    title: str
    channel_id: str
    channel_name: str
    view_count: int
    like_count: int
    comment_count: int
    duration_seconds: int
    published_at: datetime
    description: str
    tags: list[str]
    trend_keyword: str
    discovery_score: float       # 0.0 - 1.0 combined virality+relevance score
    metadata: dict = field(default_factory=dict)

    def __str__(self):
        return f"[{self.channel_name}] {self.title} | {self.view_count:,} views | {self.duration_seconds}s"


def _iso_duration_to_seconds(duration: str) -> int:
    """Parse ISO 8601 duration (e.g. PT4M13S) to seconds."""
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    m = re.match(pattern, duration)
    if not m:
        return 0
    hours = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    secs = int(m.group(3) or 0)
    return hours * 3600 + mins * 60 + secs


class ContentDiscovery:
    def __init__(self, config: dict):
        self.config = config
        self.disc_cfg = config.get("discovery", {})
        self.max_per_niche = self.disc_cfg.get("max_videos_per_niche", 5)
        self.min_views = self.disc_cfg.get("min_video_views", 50000)
        self.min_duration = self.disc_cfg.get("min_video_duration", 120)
        self.max_duration = self.disc_cfg.get("max_video_duration", 7200)
        self.max_age_days = self.disc_cfg.get("video_age_days", 30)
        self.exclude_channels = set(self.disc_cfg.get("exclude_channels", []))
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")

    def discover_for_trends(self, trends: list, max_topics: int = 20) -> list[DiscoveredVideo]:
        """
        Given a list of TrendingTopic objects, find suitable YouTube videos
        for each of the top trending topics.
        """
        if not self.api_key:
            logger.error("YOUTUBE_API_KEY not set — cannot discover content")
            return []

        from googleapiclient.discovery import build

        youtube = build("youtube", "v3", developerKey=self.api_key)
        discovered: list[DiscoveredVideo] = []
        seen_ids: set[str] = set()

        for trend in trends[:max_topics]:
            keyword = trend.keyword
            logger.info(f"Discovering videos for trend: '{keyword}'")
            try:
                videos = self._search_videos(youtube, keyword, trend)
                for v in videos:
                    if v.video_id not in seen_ids:
                        seen_ids.add(v.video_id)
                        discovered.append(v)
                time.sleep(0.5)  # respect quota
            except Exception as e:
                logger.warning(f"Discovery failed for '{keyword}': {e}")

        # Re-rank by discovery score
        discovered.sort(key=lambda v: v.discovery_score, reverse=True)
        logger.info(f"Discovered {len(discovered)} unique videos across {min(max_topics, len(trends))} trends")
        return discovered

    def _search_videos(self, youtube, keyword: str, trend) -> list[DiscoveredVideo]:
        now = datetime.now(timezone.utc)
        published_after = (now - timedelta(days=self.max_age_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Search for videos
        search_response = youtube.search().list(
            part="id,snippet",
            q=keyword,
            type="video",
            order="relevance",
            publishedAfter=published_after,
            maxResults=min(25, self.max_per_niche * 5),
            relevanceLanguage="en",
            videoEmbeddable="true",
            videoSyndicated="true"
        ).execute()

        video_ids = [
            item["id"]["videoId"]
            for item in search_response.get("items", [])
            if item["id"].get("videoId")
        ]

        if not video_ids:
            return []

        # Get detailed stats
        details_response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids)
        ).execute()

        results = []
        for item in details_response.get("items", []):
            video = self._parse_video(item, keyword, trend)
            if video:
                results.append(video)

        # Return top N by discovery score
        results.sort(key=lambda v: v.discovery_score, reverse=True)
        return results[:self.max_per_niche]

    def _parse_video(self, item: dict, keyword: str, trend) -> Optional[DiscoveredVideo]:
        video_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        # Duration
        duration = _iso_duration_to_seconds(content.get("duration", "PT0S"))
        if duration < self.min_duration or duration > self.max_duration:
            return None

        # View count
        view_count = int(stats.get("viewCount", 0))
        if view_count < self.min_views:
            return None

        channel_id = snippet.get("channelId", "")
        if channel_id in self.exclude_channels:
            return None

        like_count = int(stats.get("likeCount", 0))
        comment_count = int(stats.get("commentCount", 0))

        # Parse publish date
        try:
            published_at = datetime.fromisoformat(
                snippet.get("publishedAt", "2000-01-01T00:00:00Z").replace("Z", "+00:00")
            )
        except Exception:
            published_at = datetime.now(timezone.utc)

        # Freshness bonus: newer videos get higher score
        age_days = (datetime.now(timezone.utc) - published_at).days
        freshness = max(0.0, 1.0 - (age_days / self.max_age_days))

        # Engagement rate (likes + comments per view)
        engagement = 0.0
        if view_count > 0:
            engagement = min(1.0, (like_count + comment_count * 2) / view_count * 100)

        # View velocity: views relative to channel average (approximated by view count)
        view_norm = min(1.0, view_count / 5_000_000)

        # Trend alignment — boost if trend keyword in title
        title = snippet.get("title", "")
        kw_words = set(keyword.lower().split())
        title_words = set(title.lower().split())
        overlap = len(kw_words & title_words) / max(len(kw_words), 1)
        relevance = min(1.0, overlap * 2)

        # Combined discovery score
        discovery_score = (
            view_norm * 0.35
            + engagement * 0.25
            + freshness * 0.25
            + relevance * 0.15
        )

        # Boost from trend score
        discovery_score = min(1.0, discovery_score + trend.score * 0.1)

        return DiscoveredVideo(
            video_id=video_id,
            url=f"https://www.youtube.com/watch?v={video_id}",
            title=title,
            channel_id=channel_id,
            channel_name=snippet.get("channelTitle", ""),
            view_count=view_count,
            like_count=like_count,
            comment_count=comment_count,
            duration_seconds=duration,
            published_at=published_at,
            description=snippet.get("description", "")[:300],
            tags=snippet.get("tags", []),
            trend_keyword=keyword,
            discovery_score=discovery_score,
            metadata={
                "category_id": snippet.get("categoryId", ""),
                "live_content": content.get("licensedContent", False),
                "age_days": age_days
            }
        )
