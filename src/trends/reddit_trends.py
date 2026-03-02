"""Reddit trending posts fetcher using PRAW."""

import os
import logging

logger = logging.getLogger(__name__)


class RedditTrendsFetcher:
    def __init__(self, config: dict):
        self.config = config
        self.subreddits = config.get("subreddits", ["worldnews", "technology"])
        self.post_limit = config.get("post_limit", 25)
        self.min_score = config.get("min_score", 500)

    def fetch(self):
        import praw
        from src.trends import TrendingTopic

        topics = []
        try:
            reddit = praw.Reddit(
                client_id=os.environ["REDDIT_CLIENT_ID"],
                client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                user_agent=os.environ.get("REDDIT_USER_AGENT", "ViralClipper/1.0"),
                read_only=True
            )

            for subreddit_name in self.subreddits:
                try:
                    subreddit = reddit.subreddit(subreddit_name)
                    # Fetch hot + rising posts
                    posts = list(subreddit.hot(limit=self.post_limit))
                    posts += list(subreddit.rising(limit=10))

                    max_score = max((p.score for p in posts if p.score > 0), default=1)

                    for post in posts:
                        if post.score < self.min_score:
                            continue
                        if post.is_self and len(post.selftext) < 50:
                            continue  # skip low-content text posts

                        normalized = min(1.0, post.score / max_score)
                        # Boost for award count
                        award_boost = min(0.2, post.total_awards_received * 0.02)
                        topics.append(TrendingTopic(
                            keyword=post.title[:120],
                            source=f"reddit/{subreddit_name}",
                            score=min(1.0, normalized + award_boost),
                            raw_score=float(post.score),
                            category=subreddit_name,
                            description=post.title,
                            url=f"https://reddit.com{post.permalink}",
                            metadata={
                                "upvote_ratio": post.upvote_ratio,
                                "num_comments": post.num_comments,
                                "awards": post.total_awards_received,
                                "flair": post.link_flair_text or ""
                            }
                        ))
                except Exception as e:
                    logger.warning(f"Reddit r/{subreddit_name} error: {e}")

        except Exception as e:
            logger.error(f"Reddit auth/init error: {e}")

        return topics
