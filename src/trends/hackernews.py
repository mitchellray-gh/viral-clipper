"""HackerNews trending fetcher (completely free, no auth required)."""

import logging
import time
import urllib.request
import json

logger = logging.getLogger(__name__)

HN_BASE = "https://hacker-news.firebaseio.com/v0"


def _hn_get(path: str) -> dict | list | None:
    try:
        with urllib.request.urlopen(f"{HN_BASE}{path}.json", timeout=8) as r:
            return json.loads(r.read())
    except Exception as e:
        logger.debug(f"HN request failed for {path}: {e}")
        return None


class HackerNewsFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.fetch_top = config.get("fetch_top", 30)

    def fetch(self):
        from src.trends import TrendingTopic

        topics = []
        try:
            # Top stories + Best stories for maximum viral coverage
            top_ids = _hn_get("/topstories") or []
            best_ids = _hn_get("/beststories") or []

            # Combine and deduplicate
            combined = list(dict.fromkeys(top_ids[:self.fetch_top] + best_ids[:20]))

            max_score = 1
            items = []
            for story_id in combined[:self.fetch_top]:
                item = _hn_get(f"/item/{story_id}")
                if item and item.get("type") == "story":
                    items.append(item)
                    max_score = max(max_score, item.get("score", 0))
                time.sleep(0.05)  # gentle rate limit

            for i, item in enumerate(items):
                title = item.get("title", "").strip()
                hn_score = item.get("score", 0)
                comments = item.get("descendants", 0)
                url = item.get("url", f"https://news.ycombinator.com/item?id={item['id']}")

                if not title:
                    continue

                norm = min(1.0, hn_score / max_score)
                comment_boost = min(0.2, comments / 1000)

                topics.append(TrendingTopic(
                    keyword=title[:120],
                    source="hackernews",
                    score=min(1.0, norm + comment_boost),
                    raw_score=float(hn_score),
                    category="tech",
                    description=title,
                    url=url,
                    metadata={"hn_score": hn_score, "comments": comments, "hn_id": item["id"]}
                ))

        except Exception as e:
            logger.error(f"HackerNews fetch error: {e}")

        return topics
