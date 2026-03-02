"""Google Trends fetcher using pytrends."""

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class GoogleTrendsFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.timeframe = config.get("timeframe", "now 1-d")
        self.geo = config.get("geo", "")
        self.categories = config.get("categories", [])

    def fetch(self):
        # urllib3 v2 renamed method_whitelist -> allowed_methods; patch for pytrends compat
        try:
            import urllib3.util.retry as _retry
            if not hasattr(_retry.Retry, '_patched_for_pytrends'):
                _orig = _retry.Retry.__init__
                def _patched(self, *a, **kw):
                    if 'method_whitelist' in kw:
                        kw['allowed_methods'] = kw.pop('method_whitelist')
                    _orig(self, *a, **kw)
                _retry.Retry.__init__ = _patched
                _retry.Retry._patched_for_pytrends = True
        except Exception:
            pass
        from pytrends.request import TrendReq
        from pytrends.exceptions import ResponseError
        from src.trends import TrendingTopic

        topics = []
        try:
            pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25), retries=2, backoff_factor=0.5)

            # Real-time trending searches (replaces deprecated trending_searches)
            try:
                rt_df = pytrends.realtime_trending_searches(pn="US")
                # Columns vary by pytrends version; grab first text-like column
                title_col = "title" if "title" in rt_df.columns else rt_df.columns[0]
                keywords = rt_df[title_col].dropna().tolist()[:30]
                for i, keyword in enumerate(keywords):
                    keyword = str(keyword).strip()
                    if not keyword:
                        continue
                    score = 1.0 - (i / max(len(keywords), 1))
                    topics.append(TrendingTopic(
                        keyword=keyword,
                        source="google_trends",
                        score=score,
                        raw_score=len(keywords) - i,
                        category="trending",
                        description=f"Google Trends real-time rank #{i+1}"
                    ))
                logger.info(f"Google Trends: fetched {len(topics)} real-time topics")
            except Exception as e:
                logger.debug(f"realtime_trending_searches unavailable: {e}")

            time.sleep(1)  # be polite to API

            # Rising related queries for key seed terms
            try:
                pytrends.build_payload(
                    ["viral", "trending", "breaking news"],
                    timeframe=self.timeframe,
                    geo=self.geo
                )
                related = pytrends.related_queries()
                for term, data in related.items():
                    if data and data.get("rising") is not None:
                        rising_df = data["rising"]
                        if rising_df is not None and not rising_df.empty:
                            for _, row in rising_df.head(10).iterrows():
                                query = str(row.get("query", ""))
                                value = float(row.get("value", 0))
                                if query:
                                    topics.append(TrendingTopic(
                                        keyword=query,
                                        source="google_trends_rising",
                                        score=min(1.0, value / 100),
                                        raw_score=value,
                                        category="rising",
                                        description="Rising related query"
                                    ))
            except Exception as e:
                logger.debug(f"Related queries failed: {e}")

        except Exception as e:
            logger.error(f"Google Trends fetch error: {e}")

        return topics
