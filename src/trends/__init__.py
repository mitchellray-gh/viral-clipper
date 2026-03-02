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
from .momentum import TrendMomentumTracker

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
    # Populated after momentum analysis
    momentum_score: float = 0.0     # composite score including velocity
    velocity: float = 0.0           # score/hour (positive = rising fast)
    phase: str = "new"              # new / rising / peak / declining / dead
    breakout: bool = False          # high-confidence early breakout signal

    def __str__(self):
        phase_tag = f" [{self.phase}" + (" 🔥" if self.breakout else "") + "]"
        return f"[{self.source}] {self.keyword} (score={self.score:.2f}, vel={self.velocity:+.3f}){phase_tag}"


class TrendAggregator:
    """
    Aggregates trending topics from multiple sources, deduplicates,
    cross-references, and returns a ranked list of niches to target.
    """

    def __init__(self, config: dict):
        self.config = config
        self.fetchers = []
        self._init_fetchers()
        db_path = config.get("queue", {}).get("db_path", "data/queue.db")
        self.momentum = TrendMomentumTracker(db_path=db_path)

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
        """Fetch from all sources, merge, deduplicate, rank by momentum."""
        all_topics: list[TrendingTopic] = []

        for fetcher in self.fetchers:
            try:
                topics = fetcher.fetch()
                logger.info(f"{fetcher.__class__.__name__}: fetched {len(topics)} topics")
                all_topics.extend(topics)
            except Exception as e:
                logger.warning(f"{fetcher.__class__.__name__} failed: {e}")

        merged = self._merge_and_deduplicate(all_topics)

        # Persist snapshots for velocity tracking
        try:
            self.momentum.record_snapshots(merged)
        except Exception as e:
            logger.warning(f"Momentum snapshot failed: {e}")

        # Compute momentum signals and enrich each topic
        try:
            signals = self.momentum.compute_momentum(merged)
            for topic in merged:
                import re
                kw_norm = re.sub(r"[^a-z0-9 ]", "", topic.keyword.lower().strip())[:120]
                sig = signals.get(kw_norm)
                if sig:
                    topic.momentum_score = sig.momentum_score
                    topic.velocity = sig.velocity
                    topic.phase = sig.phase
                    topic.breakout = sig.breakout
                else:
                    topic.momentum_score = topic.score
        except Exception as e:
            logger.warning(f"Momentum computation failed: {e}")
            for topic in merged:
                topic.momentum_score = topic.score

        # Rank by momentum_score, not raw score — rewards rising topics
        ranked = sorted(merged, key=lambda t: t.momentum_score, reverse=True)

        # Log breakout signals prominently
        breakouts = [t for t in ranked if t.breakout]
        if breakouts:
            logger.info(f"🔥 {len(breakouts)} BREAKOUT topics detected: "
                        + ", ".join(t.keyword[:40] for t in breakouts[:5]))

        # Filter out declining/dead topics (they've already peaked)
        alive = [t for t in ranked if t.phase not in ("declining", "dead")]
        declined = len(ranked) - len(alive)
        if declined:
            logger.info(f"Filtered {declined} declining/dead topics from pipeline")

        logger.info(f"Total viable trending topics: {len(alive)}")
        return alive

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
            sources = list({t.source for t in bucket})

            # Escape velocity: when a topic crosses from social chatter (fast sources)
            # into mainstream news (slow sources), it's hitting viral escape velocity.
            fast_sources = {"google_trends", "google_trends_rising", "hackernews"}
            slow_sources = {s for s in sources if s.startswith("reddit") or s.startswith("rss")}
            yt_source = {s for s in sources if s == "youtube_trending"}

            has_fast = any(s in fast_sources for s in sources)
            has_slow = bool(slow_sources)
            has_yt = bool(yt_source)

            # Base boost per additional source
            source_boost = min(0.20, (len(bucket) - 1) * 0.07)

            # Escape velocity multiplier: fast + slow crossover = strong signal
            escape_boost = 0.0
            if has_fast and has_slow:
                escape_boost += 0.15  # social chatter → mainstream press crossover
            if has_yt and (has_fast or has_slow):
                escape_boost += 0.10  # YouTube trending + other = already viral
            if len(sources) >= 4:
                escape_boost += 0.10  # omnipresent topic

            primary.score = min(1.0, primary.score + source_boost + escape_boost)
            primary.metadata["source_count"] = len(bucket)
            primary.metadata["sources"] = sources
            primary.metadata["escape_velocity"] = escape_boost > 0.10
            merged.append(primary)

        return merged
