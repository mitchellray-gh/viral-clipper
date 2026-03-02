"""YouTube Trending fetcher via YouTube Data API v3."""

import os
import logging

logger = logging.getLogger(__name__)


class YouTubeTrendingFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.region_code = config.get("region_code", "US")
        self.category_ids = config.get("category_ids", ["0"])
        self.max_results = config.get("max_results", 50)

    def fetch(self):
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from src.trends import TrendingTopic

        topics = []
        api_key = os.environ.get("YOUTUBE_API_KEY", "")
        if not api_key:
            logger.warning("YOUTUBE_API_KEY not set — skipping YouTube Trending")
            return topics

        try:
            youtube = build("youtube", "v3", developerKey=api_key)

            seen_ids = set()
            for category_id in self.category_ids:
                try:
                    request = youtube.videos().list(
                        part="snippet,statistics",
                        chart="mostPopular",
                        regionCode=self.region_code,
                        videoCategoryId=category_id if category_id != "0" else "",
                        maxResults=min(50, self.max_results),
                        hl="en"
                    )
                    response = request.execute()
                    items = response.get("items", [])

                    max_views = max(
                        (int(i["statistics"].get("viewCount", 0)) for i in items),
                        default=1
                    )

                    for i, item in enumerate(items):
                        video_id = item["id"]
                        if video_id in seen_ids:
                            continue
                        seen_ids.add(video_id)

                        snippet = item["snippet"]
                        stats = item.get("statistics", {})
                        views = int(stats.get("viewCount", 0))
                        likes = int(stats.get("likeCount", 0))
                        comments = int(stats.get("commentCount", 0))

                        title = snippet.get("title", "")
                        channel = snippet.get("channelTitle", "")

                        # Engagement score: views + like/comment bonus
                        engagement = views + (likes * 10) + (comments * 5)
                        norm_score = min(1.0, views / max_views)

                        topics.append(TrendingTopic(
                            keyword=title[:120],
                            source="youtube_trending",
                            score=norm_score,
                            raw_score=float(views),
                            category=f"category_{category_id}",
                            description=snippet.get("description", "")[:200],
                            url=f"https://www.youtube.com/watch?v={video_id}",
                            metadata={
                                "video_id": video_id,
                                "channel": channel,
                                "views": views,
                                "likes": likes,
                                "comments": comments,
                                "engagement": engagement,
                                "category_id": category_id
                            }
                        ))

                except HttpError as e:
                    logger.warning(f"YouTube API error for category {category_id}: {e}")

        except Exception as e:
            logger.error(f"YouTube Trending fetch error: {e}")

        return topics
