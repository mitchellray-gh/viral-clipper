"""RSS feed trend fetcher using feedparser."""

import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


class RssTrendsFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.feeds = config.get("feeds", [])

    def fetch(self):
        import feedparser
        from src.trends import TrendingTopic

        topics = []
        now = datetime.now(timezone.utc)

        for feed_cfg in self.feeds:
            url = feed_cfg.get("url", "")
            name = feed_cfg.get("name", url)
            if not url:
                continue
            try:
                feed = feedparser.parse(url)
                entries = feed.entries[:20]  # top 20 per feed

                for i, entry in enumerate(entries):
                    title = entry.get("title", "").strip()
                    if not title:
                        continue

                    # Parse publication date
                    pub_age_hours = 48.0  # default if no date
                    for date_field in ["published", "updated"]:
                        try:
                            dt_str = entry.get(date_field, "")
                            if dt_str:
                                dt = parsedate_to_datetime(dt_str)
                                pub_age_hours = (now - dt).total_seconds() / 3600
                                break
                        except Exception:
                            pass

                    # Fresher = higher score. Decay over 48 hours.
                    freshness = max(0.0, 1.0 - (pub_age_hours / 48.0))
                    rank_score = 1.0 - (i / len(entries))
                    score = (freshness * 0.6) + (rank_score * 0.4)

                    topics.append(TrendingTopic(
                        keyword=title[:120],
                        source=f"rss/{name}",
                        score=score,
                        raw_score=rank_score,
                        category=name,
                        description=entry.get("summary", title)[:300],
                        url=entry.get("link", ""),
                        metadata={"age_hours": pub_age_hours}
                    ))

                time.sleep(0.3)  # brief pause per feed

            except Exception as e:
                logger.warning(f"RSS feed '{name}' error: {e}")

        return topics
