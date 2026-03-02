"""
Trend Aggregation Module
Pulls trending topics from: Google Trends, Reddit, RSS feeds, YouTube Trending, HackerNews
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from .google_trends import GoogleTrendsFetcher
from .reddit_trends import RedditTrendsFetcher
from .rss_trends import RssTrendsFetcher
from .youtube_trending import YouTubeTrendingFetcher
from .hackernews import HackerNewsFetcher

logger = logging.getLogger(__name__)


@dataclass
class TrendingTopic:
    keyword: str
    source: str
    score: float           # 0.0 - 1.0 normalized relevance/virality score
    raw_score: float       # original score from source
    category: str = ""
    description: str = ""
    url: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)

    def __str__(self):
        return f"[{self.source}] {self.keyword} (score={self.score:.2f})"


class TrendAggregator:
    """
    Aggregates trending topics from multiple sources, deduplicates,
    cross-references, and returns a ranked list of niches to target.
    """

    def __init__(self, config: dict):
        self.config = config
        self.fetchers = []
        self._init_fetchers()

    def _init_fetchers(self):
        cfg = self.config.get("trends", {})
        if cfg.get("google_trends", {}).get("enabled", True):
            self.fetchers.append(GoogleTrendsFetcher(cfg["google_trends"]))
        if cfg.get("reddit", {}).get("enabled", True):
            self.fetchers.append(RedditTrendsFetcher(cfg["reddit"]))
        if cfg.get("rss", {}).get("enabled", True):
            self.fetchers.append(RssTrendsFetcher(cfg["rss"]))
        if cfg.get("youtube_trending", {}).get("enabled", True):
            self.fetchers.append(YouTubeTrendingFetcher(cfg["youtube_trending"]))
        if cfg.get("hackernews", {}).get("enabled", True):
            self.fetchers.append(HackerNewsFetcher(cfg.get("hackernews", {})))

    def fetch_all(self) -> list[TrendingTopic]:
        """Fetch from all sources, merge, deduplicate, and rank."""
        all_topics: list[TrendingTopic] = []

        for fetcher in self.fetchers:
            try:
                topics = fetcher.fetch()
                logger.info(f"{fetcher.__class__.__name__}: fetched {len(topics)} topics")
                all_topics.extend(topics)
            except Exception as e:
                logger.warning(f"{fetcher.__class__.__name__} failed: {e}")

        merged = self._merge_and_deduplicate(all_topics)
        ranked = sorted(merged, key=lambda t: t.score, reverse=True)
        logger.info(f"Total unique trending topics after merge: {len(ranked)}")
        return ranked

    def _merge_and_deduplicate(self, topics: list[TrendingTopic]) -> list[TrendingTopic]:
        """
        Merge topics with the same/similar keyword across sources.
        Cross-source appearance boosts the score.
        """
        from collections import defaultdict
        import re

        def normalize(kw: str) -> str:
            return re.sub(r"[^a-z0-9 ]", "", kw.lower().strip())

        buckets: dict[str, list[TrendingTopic]] = defaultdict(list)
        for topic in topics:
            key = normalize(topic.keyword)
            if not key:
                continue
            # Find closest existing bucket
            matched = False
            for existing_key in list(buckets.keys()):
                # Simple overlap check - if 60%+ words match
                kw_words = set(key.split())
                ex_words = set(existing_key.split())
                if kw_words and ex_words:
                    overlap = len(kw_words & ex_words) / max(len(kw_words), len(ex_words))
                    if overlap > 0.6:
                        buckets[existing_key].append(topic)
                        matched = True
                        break
            if not matched:
                buckets[key].append(topic)

        merged = []
        for key, bucket in buckets.items():
            # Use the representative topic (highest raw score)
            primary = max(bucket, key=lambda t: t.raw_score)
            # Boost score per additional source (up to +0.3)
            source_boost = min(0.3, (len(bucket) - 1) * 0.1)
            primary.score = min(1.0, primary.score + source_boost)
            primary.metadata["source_count"] = len(bucket)
            primary.metadata["sources"] = list({t.source for t in bucket})
            merged.append(primary)

        return merged
