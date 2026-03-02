"""
Video Downloader Module
Downloads YouTube videos using yt-dlp with smart path management.
"""

import os
import re
import logging
import hashlib
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    """Remove characters unsafe for filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", name)[:80]


@dataclass
class DownloadResult:
    video_id: str
    file_path: str
    title: str
    duration: int
    width: int
    height: int
    fps: float
    filesize_bytes: int
    success: bool
    error: str = ""


class VideoDownloader:
    def __init__(self, config: dict):
        self.cfg = config.get("downloader", {})
        self.output_dir = Path(self.cfg.get("output_dir", "data/downloads"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.format = self.cfg.get("format", "bestvideo[height<=1080]+bestaudio/best[height<=1080]")
        self.max_filesize_mb = self.cfg.get("max_filesize_mb", 500)
        self.cookies_file = self.cfg.get("cookies_file")
        self.proxy = os.environ.get("YT_DLP_PROXY", "")

    def download(self, video_id: str, title: str = "") -> DownloadResult:
        """Download a single YouTube video. Returns DownloadResult."""
        import yt_dlp

        url = f"https://www.youtube.com/watch?v={video_id}"
        safe_title = _sanitize_filename(title or video_id)
        out_path = self.output_dir / f"{video_id}_{safe_title}.%(ext)s"

        ydl_opts = {
            "format": self.format,
            "outtmpl": str(out_path),
            "quiet": True,
            "no_warnings": False,
            "merge_output_format": "mp4",
            "writeinfojson": False,
            "writethumbnail": False,
            "max_filesize": self.max_filesize_mb * 1024 * 1024,
            "retries": 3,
            "fragment_retries": 3,
            "skip_unavailable_fragments": True,
            "postprocessors": [{
                "key": "FFmpegVideoConvertor",
                "preferedformat": "mp4"
            }]
        }

        if self.cookies_file and Path(self.cookies_file).exists():
            ydl_opts["cookiefile"] = self.cookies_file

        if self.proxy:
            ydl_opts["proxy"] = self.proxy

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if not info:
                    return DownloadResult(
                        video_id=video_id, file_path="", title=title,
                        duration=0, width=0, height=0, fps=0.0,
                        filesize_bytes=0, success=False, error="No info returned"
                    )

                # Locate downloaded file
                final_path = self._find_downloaded_file(video_id)
                if not final_path:
                    return DownloadResult(
                        video_id=video_id, file_path="", title=info.get("title", title),
                        duration=0, width=0, height=0, fps=0.0,
                        filesize_bytes=0, success=False, error="Downloaded file not found"
                    )

                file_size = Path(final_path).stat().st_size

                # Extract metadata from highest-quality format
                formats = info.get("formats", [{}])
                best_fmt = max(formats, key=lambda f: f.get("height", 0) or 0, default={})

                return DownloadResult(
                    video_id=video_id,
                    file_path=str(final_path),
                    title=info.get("title", title),
                    duration=int(info.get("duration", 0)),
                    width=int(best_fmt.get("width", 0) or info.get("width", 0) or 0),
                    height=int(best_fmt.get("height", 0) or info.get("height", 0) or 0),
                    fps=float(best_fmt.get("fps", 0) or info.get("fps", 30) or 30),
                    filesize_bytes=file_size,
                    success=True
                )

        except Exception as e:
            logger.error(f"Download failed for {video_id}: {e}")
            return DownloadResult(
                video_id=video_id, file_path="", title=title,
                duration=0, width=0, height=0, fps=0.0,
                filesize_bytes=0, success=False, error=str(e)
            )

    def _find_downloaded_file(self, video_id: str) -> Optional[Path]:
        """Locate the downloaded file for a video_id."""
        for f in self.output_dir.glob(f"{video_id}_*.mp4"):
            return f
        for f in self.output_dir.glob(f"*{video_id}*.mp4"):
            return f
        return None

    def is_already_downloaded(self, video_id: str) -> Optional[str]:
        """Check if a video was already downloaded. Returns path or None."""
        path = self._find_downloaded_file(video_id)
        return str(path) if path else None

    def cleanup_old_downloads(self, keep_days: int = 7):
        """Remove downloaded source videos older than keep_days to save disk space."""
        import time
        now = time.time()
        cutoff = now - (keep_days * 86400)
        removed = 0
        for f in self.output_dir.glob("*.mp4"):
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        if removed:
            logger.info(f"Cleaned up {removed} old source videos from downloads/")
