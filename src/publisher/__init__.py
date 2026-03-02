"""
YouTube Publisher
Uploads finished Shorts to YouTube via the YouTube Data API v3.
Handles OAuth2 authentication, quota management, and retry logic.
"""

import os
import json
import logging
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# YouTube API quota cost per upload: 1600 units (out of 10,000/day)
# So you can upload ~6 videos per day with default quota
_UPLOAD_QUOTA_COST = 1600
_DAILY_QUOTA = 10000


@dataclass
class UploadResult:
    success: bool
    youtube_id: str = ""
    youtube_url: str = ""
    error: str = ""


class YouTubePublisher:
    def __init__(self, config: dict):
        self.cfg = config.get("publisher", {})
        self.privacy = self.cfg.get("privacy_status", "public")
        self.notify_subs = self.cfg.get("notify_subscribers", True)
        self.made_for_kids = self.cfg.get("made_for_kids", False)
        self.client_secrets = os.environ.get(
            "YOUTUBE_CLIENT_SECRETS_FILE", "config/client_secrets.json"
        )
        self.token_file = os.environ.get(
            "YOUTUBE_TOKEN_FILE", "config/youtube_token.json"
        )
        self._youtube = None
        self._quota_used = 0

    def _authenticate(self):
        """
        Authenticate with YouTube OAuth2.
        On first run, opens browser for authorization.
        Subsequent runs use saved token.
        """
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

        creds = None
        token_path = Path(self.token_file)

        if token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            except Exception as e:
                logger.warning(f"Token file load failed: {e}")

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not Path(self.client_secrets).exists():
                    raise FileNotFoundError(
                        f"YouTube client secrets not found at {self.client_secrets}. "
                        "Download from Google Cloud Console → APIs & Services → Credentials"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(self.client_secrets, SCOPES)
                creds = flow.run_local_server(port=0)

            # Save token for future runs
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json())
            logger.info(f"YouTube OAuth token saved to {self.token_file}")

        self._youtube = build("youtube", "v3", credentials=creds)
        return self._youtube

    def _get_client(self):
        if self._youtube is None:
            self._authenticate()
        return self._youtube

    def upload(self, clip: "QueuedClip") -> UploadResult:
        """
        Upload a single Short to YouTube.
        Returns UploadResult with the YouTube video ID on success.
        """
        if not Path(clip.clip_path).exists():
            return UploadResult(success=False, error=f"Clip file not found: {clip.clip_path}")

        if self._quota_used + _UPLOAD_QUOTA_COST > _DAILY_QUOTA:
            return UploadResult(success=False, error="YouTube API daily quota limit reached")

        try:
            youtube = self._get_client()

            tags = clip.get_tags()
            hashtags = clip.get_hashtags()
            description = clip.description or ""
            if hashtags:
                hashtag_str = " ".join(f"#{h.lstrip('#')}" for h in hashtags)
                if hashtag_str not in description:
                    description = f"{description}\n\n{hashtag_str}".strip()

            body = {
                "snippet": {
                    "title": clip.title or "Epic Short",
                    "description": description,
                    "tags": tags,
                    "categoryId": clip.category_id or "22",
                    "defaultLanguage": "en",
                },
                "status": {
                    "privacyStatus": self.privacy,
                    "selfDeclaredMadeForKids": self.made_for_kids,
                    "embeddable": self.cfg.get("embeddable", True),
                    "license": self.cfg.get("license", "youtube"),
                    "publicStatsViewable": True,
                    "publishAt": clip.scheduled_at if self.privacy == "private" else None
                }
            }

            # Clean up None values
            body["status"] = {k: v for k, v in body["status"].items() if v is not None}

            from googleapiclient.http import MediaFileUpload
            media = MediaFileUpload(
                clip.clip_path,
                mimetype="video/mp4",
                resumable=True,
                chunksize=1024 * 1024 * 5  # 5MB chunks
            )

            request = youtube.videos().insert(
                part="snippet,status",
                body=body,
                media_body=media,
                notifySubscribers=self.notify_subs
            )

            # Resumable upload with progress
            response = None
            retry_count = 0
            while response is None:
                try:
                    status, response = request.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        logger.info(f"Upload progress: {progress}% for {clip.id}")
                except Exception as e:
                    retry_count += 1
                    if retry_count > 3:
                        raise
                    logger.warning(f"Upload chunk error (retry {retry_count}): {e}")
                    time.sleep(2 ** retry_count)

            self._quota_used += _UPLOAD_QUOTA_COST
            video_id = response.get("id", "")
            logger.info(f"✅ Uploaded: https://youtube.com/shorts/{video_id} | '{clip.title}'")

            return UploadResult(
                success=True,
                youtube_id=video_id,
                youtube_url=f"https://youtube.com/shorts/{video_id}"
            )

        except Exception as e:
            logger.error(f"Upload failed for {clip.id}: {e}")
            return UploadResult(success=False, error=str(e))

    def upload_batch(self, clips: list, queue: "ContentQueue") -> dict:
        """Upload a batch of due clips, respecting quota limits."""
        from src.queue import ClipStatus

        results = {"uploaded": 0, "failed": 0, "quota_exceeded": False}

        for clip in clips:
            if self._quota_used + _UPLOAD_QUOTA_COST > _DAILY_QUOTA:
                logger.warning("YouTube daily quota limit reached — stopping uploads")
                results["quota_exceeded"] = True
                break

            result = self.upload(clip)
            if result.success:
                queue.update_status(
                    clip.id, ClipStatus.PUBLISHED,
                    youtube_short_id=result.youtube_id,
                    published_at=__import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc
                    ).isoformat()
                )
                results["uploaded"] += 1
            else:
                queue.update_status(
                    clip.id, ClipStatus.FAILED,
                    error_message=result.error
                )
                results["failed"] += 1

            time.sleep(3)  # Brief pause between uploads

        return results

    def save_locally(self, clip: "QueuedClip", output_dir: str = "data/shorts"):
        """
        Save clip to organized local folder structure.
        Used alongside or instead of YouTube upload.
        """
        from pathlib import Path
        import shutil

        if not clip.clip_path or not Path(clip.clip_path).exists():
            logger.warning(f"Cannot save locally — no clip file for {clip.id}")
            return

        # Organize by date
        now = __import__("datetime").datetime.now()
        date_dir = Path(output_dir) / now.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        dest = date_dir / Path(clip.clip_path).name
        if not dest.exists():
            shutil.copy2(clip.clip_path, dest)
            logger.info(f"Saved locally: {dest}")

        # Save metadata sidecar
        meta = {
            "id": clip.id,
            "title": clip.title,
            "description": clip.description,
            "tags": clip.get_tags(),
            "hashtags": clip.get_hashtags(),
            "virality_score": clip.virality_score,
            "trend_keyword": clip.trend_keyword,
            "source_url": clip.source_url,
            "scheduled_at": clip.scheduled_at
        }
        meta_path = dest.with_suffix(".json")
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
