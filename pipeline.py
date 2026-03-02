"""
Viral Clipper Pipeline - Main Orchestrator
==========================================
Ties together all pipeline stages:
  1. Trend Aggregation  → find hot niches
  2. Content Discovery  → find source videos on YouTube
  3. Download           → pull video files via yt-dlp
  4. Transcribe         → faster-whisper local transcription
  5. Virality Scoring   → Gemini identifies best clip moments
  6. Editing            → 9:16 vertical, captions, watermark
  7. Metadata Gen       → Gemini generates titles/tags/hashtags
  8. Queue + Schedule   → SQLite backlog + posting calendar
  9. Publish            → YouTube Data API upload + local save

Run modes:
  python pipeline.py run          → full pipeline run
  python pipeline.py publish      → publish due scheduled clips only
  python pipeline.py status       → show queue statistics
  python pipeline.py schedule     → run on a fixed daily schedule (daemon)
"""

import os
import sys
import logging
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv


# ── Logging setup ───────────────────────────────────────────────────────────

def setup_logging(config: dict):
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO"))
    log_file = log_cfg.get("file", "logs/pipeline.log")
    max_bytes = log_cfg.get("max_bytes", 10_485_760)
    backup_count = log_cfg.get("backup_count", 5)

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    from logging.handlers import RotatingFileHandler
    handlers = [
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers
    )


# ── Config loader ────────────────────────────────────────────────────────────

def load_config(path: str = "config/settings.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Pipeline stages ──────────────────────────────────────────────────────────

logger = logging.getLogger("pipeline")


class ViralClipperPipeline:
    def __init__(self, config: dict):
        self.config = config
        self._init_components()

    def _init_components(self):
        from src.trends import TrendAggregator
        from src.discovery import ContentDiscovery
        from src.downloader import VideoDownloader
        from src.transcription import WhisperTranscriber
        from src.clipper import ViralityScorer
        from src.editor import VideoEditor
        from src.metadata import MetadataGenerator
        from src.queue import ContentQueue
        from src.publisher import YouTubePublisher
        from src.publisher.analytics import AnalyticsFeedbackCollector

        self.trends = TrendAggregator(self.config)
        self.discovery = ContentDiscovery(self.config)
        self.downloader = VideoDownloader(self.config)
        self.transcriber = WhisperTranscriber(self.config)
        self.scorer = ViralityScorer(self.config)
        self.editor = VideoEditor(self.config)
        self.metadata_gen = MetadataGenerator(self.config)
        self.queue = ContentQueue(self.config)
        self.publisher = YouTubePublisher(self.config)
        self.analytics = AnalyticsFeedbackCollector(self.config, self.trends.momentum)

    # ── Stage 1+2: Discover new content from trends ──────────────────────────

    def stage_discover(self, max_topics: int = 20) -> list:
        """Fetch trends and find source YouTube videos."""
        logger.info("=" * 60)
        logger.info("STAGE 1/2: Trend Aggregation + Content Discovery")
        logger.info("=" * 60)

        trends = self.trends.fetch_all()
        if not trends:
            logger.warning("No trending topics found — skipping discovery")
            return []

        # Highlight breakout topics for operator visibility
        breakouts = [t for t in trends if getattr(t, "breakout", False)]
        logger.info(f"Top 5 trends: {[t.keyword for t in trends[:5]]}")
        if breakouts:
            logger.info(f"🔥 BREAKOUT signals: {[t.keyword for t in breakouts[:3]]}")

        videos = self.discovery.discover_for_trends(trends, max_topics=max_topics)
        logger.info(f"Discovered {len(videos)} source videos")

        # Filter out already-processed videos
        new_videos = [v for v in videos if not self.queue.is_video_processed(v.video_id)]
        logger.info(f"{len(new_videos)} new (unprocessed) videos")
        return new_videos

    # ── Stage 3-7: Process a single video into clips ─────────────────────────

    def stage_process_video(self, video) -> int:
        """
        Full processing pipeline for a single discovered video.
        Returns number of clips added to queue.
        """
        vid_id = video.video_id
        logger.info(f"\n{'─'*50}")
        logger.info(f"Processing: [{vid_id}] {video.title[:70]}")
        logger.info(f"  Trend: {video.trend_keyword} | Views: {video.view_count:,} | {video.duration_seconds}s")

        # ── Download ──────────────────────────────────────────────────────────
        existing_path = self.downloader.is_already_downloaded(vid_id)
        if existing_path:
            logger.info(f"  ↳ Already downloaded: {existing_path}")
            download = type("D", (), {
                "success": True, "file_path": existing_path,
                "title": video.title, "duration": video.duration_seconds,
                "width": 1920, "height": 1080, "fps": 30.0
            })()
        else:
            logger.info(f"  ↳ Downloading...")
            download = self.downloader.download(vid_id, video.title)
            if not download.success:
                logger.warning(f"  ✗ Download failed: {download.error}")
                return 0

        # ── Transcribe ────────────────────────────────────────────────────────
        logger.info(f"  ↳ Transcribing...")
        transcript = self.transcriber.transcribe_cached(download.file_path, vid_id)
        if not transcript:
            logger.warning(f"  ✗ Transcription failed for {vid_id}")
            return 0

        logger.info(f"  ✓ Transcript: {len(transcript.segments)} segments, {len(transcript.full_text)} chars")

        # ── Score virality ────────────────────────────────────────────────────
        logger.info(f"  ↳ Scoring clips with Gemini...")
        clip_candidates = self.scorer.find_clips(
            transcript, vid_id, video.url, video.trend_keyword,
            video_path=download.file_path,
            source_metadata=video.metadata
        )
        if not clip_candidates:
            logger.info(f"  ✗ No clips above virality threshold for {vid_id}")
            self.queue.mark_video_processed(vid_id, 0)
            return 0

        logger.info(f"  ✓ Found {len(clip_candidates)} viral clips")

        # ── Edit each clip ────────────────────────────────────────────────────
        clips_added = 0
        for clip_candidate in clip_candidates:
            try:
                logger.info(f"  ↳ Editing clip {clip_candidate.start_time:.1f}s-{clip_candidate.end_time:.1f}s "
                            f"(score={clip_candidate.virality_score:.2f})")

                # Get word timestamps for this clip window
                words = transcript.get_words_window(clip_candidate.start_time, clip_candidate.end_time)

                clip_id = f"{vid_id}_{int(clip_candidate.start_time)}"
                short = self.editor.create_short(
                    source_path=download.file_path,
                    start_time=clip_candidate.start_time,
                    end_time=clip_candidate.end_time,
                    clip_id=clip_id,
                    words=words
                )

                if not short.success:
                    logger.warning(f"  ✗ Editing failed: {short.error}")
                    continue

                # ── Generate metadata ──────────────────────────────────────
                logger.info(f"  ↳ Generating metadata with Gemini...")
                meta = self.metadata_gen.generate(clip_candidate, download.title)

                # ── Add to queue ───────────────────────────────────────────
                queue_id = self.queue.add_clip(clip_candidate)
                from src.queue import ClipStatus
                self.queue.update_status(
                    queue_id,
                    ClipStatus.METADATA_READY,
                    clip_path=short.output_path,
                    source_title=download.title,
                    title=meta.title,
                    description=meta.description,
                    tags=meta.tags,
                    hashtags=meta.hashtags,
                    category_id=meta.category_id
                )

                logger.info(f"  ✓ Clip ready: '{meta.title}'")
                clips_added += 1

            except Exception as e:
                logger.error(f"  ✗ Clip processing error: {e}\n{traceback.format_exc()}")

        self.queue.mark_video_processed(vid_id, clips_added)

        # Cleanup old downloads to save space
        self.downloader.cleanup_old_downloads(keep_days=3)

        return clips_added

    # ── Stage 8: Schedule pending clips ──────────────────────────────────────

    def stage_schedule(self):
        """Assign scheduled_at times to metadata-ready clips."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 8: Scheduling")
        logger.info("=" * 60)
        self.queue.schedule_upcoming()
        stats = self.queue.get_stats()
        logger.info(f"Queue stats: {stats}")

    # ── Stage 9: Publish due clips ────────────────────────────────────────────

    def stage_publish(self):
        """Upload scheduled clips that are due now."""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 9: Publishing due clips")
        logger.info("=" * 60)

        due = self.queue.get_due_clips()
        if not due:
            logger.info("No clips due for publishing")
            return

        logger.info(f"Publishing {len(due)} clip(s)...")

        for clip in due:
            logger.info(f"  ↳ Uploading: '{clip.title}' [{clip.id}]")
            # Always save locally
            self.publisher.save_locally(clip, self.config.get("publisher", {}).get("output_dir", "data/shorts"))
            # Then try to upload to YouTube
            result = self.publisher.upload(clip)
            from src.queue import ClipStatus
            if result.success:
                self.queue.update_status(
                    clip.id, ClipStatus.PUBLISHED,
                    youtube_short_id=result.youtube_id,
                    published_at=datetime.now(timezone.utc).isoformat()
                )
                logger.info(f"  ✓ Published: {result.youtube_url}")
            else:
                # Save locally even if upload fails
                logger.error(f"  ✗ Upload failed: {result.error}")
                if "quota" in result.error.lower():
                    logger.warning("  ⚠ Quota exceeded — saved locally only")
                    break

        # Collect YouTube Analytics for recently published clips and feed back
        # performance data to the momentum/niche tracker
        try:
            logger.info("  ↳ Collecting analytics feedback...")
            perfs = self.analytics.collect_recent_performance(days_back=7)
            if perfs:
                top = sorted(perfs, key=lambda p: p.virality_score, reverse=True)[:3]
                logger.info(f"  ✓ Analytics: top clips = {[p.niche for p in top]}")
                recs = self.analytics.recommend_best_niches(top_n=5)
                if recs:
                    logger.info(f"  💹 Best-performing niches: {recs}")
        except Exception as e:
            logger.warning(f"  Analytics collection failed (non-fatal): {e}")

    # ── Full pipeline run ─────────────────────────────────────────────────────

    def run_full_pipeline(self, max_topics: int = 15, max_videos: int = 10):
        """Run the complete pipeline end-to-end."""
        start = time.time()
        logger.info("\n" + "╔" + "═" * 58 + "╗")
        logger.info("║  VIRAL CLIPPER PIPELINE — FULL RUN" + " " * 22 + "║")
        logger.info("║  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " * 42 + "║")
        logger.info("╚" + "═" * 58 + "╝")

        # 1+2: Discover
        videos = self.stage_discover(max_topics=max_topics)

        # 3-7: Process top videos (up to max_videos)
        total_clips = 0
        for i, video in enumerate(videos[:max_videos]):
            logger.info(f"\n[Video {i+1}/{min(max_videos, len(videos))}]")
            clips = self.stage_process_video(video)
            total_clips += clips
            # Brief pause between videos
            time.sleep(2)

        # 8: Schedule
        self.stage_schedule()

        # 9: Publish due clips
        self.stage_publish()

        elapsed = time.time() - start
        stats = self.queue.get_stats()

        logger.info("\n" + "=" * 60)
        logger.info(f"PIPELINE COMPLETE in {elapsed:.0f}s")
        logger.info(f"New clips this run: {total_clips}")
        logger.info(f"Queue: {stats}")
        logger.info("=" * 60)

    def print_status(self):
        """Print current queue statistics."""
        stats = self.queue.get_stats()
        print("\n📊 Viral Clipper Queue Status")
        print("=" * 40)
        for k, v in stats.items():
            print(f"  {k:<25} {v}")
        print("=" * 40)

    def run_scheduled(self):
        """Run the pipeline on a daily schedule using APScheduler."""
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = BlockingScheduler(timezone="UTC")
        pipeline_hour = self.config.get("pipeline", {}).get("run_hour", 6)

        # Daily full pipeline run
        scheduler.add_job(
            self.run_full_pipeline,
            CronTrigger(hour=pipeline_hour, minute=0),
            id="daily_pipeline",
            name="Daily pipeline run",
            misfire_grace_time=3600
        )

        # Publish due clips every hour
        scheduler.add_job(
            self.stage_publish,
            CronTrigger(minute=5),
            id="hourly_publish",
            name="Hourly publish check"
        )

        logger.info(f"Pipeline scheduler started (daily run at {pipeline_hour:02d}:00 UTC)")
        logger.info("Press Ctrl+C to stop")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Pipeline scheduler stopped")


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def main():
    load_dotenv("config/.env")
    config = load_config("config/settings.yaml")
    setup_logging(config)

    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    pipeline = ViralClipperPipeline(config)

    if mode == "run":
        pipeline.run_full_pipeline()
    elif mode == "publish":
        pipeline.stage_publish()
    elif mode == "schedule":
        pipeline.run_scheduled()
    elif mode == "status":
        pipeline.print_status()
    elif mode == "discover":
        videos = pipeline.stage_discover()
        for v in videos[:10]:
            print(f"  [{v.view_count:>10,} views] {v.title[:60]} | {v.trend_keyword}")
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python pipeline.py [run|publish|schedule|status|discover]")
        sys.exit(1)


if __name__ == "__main__":
    main()
