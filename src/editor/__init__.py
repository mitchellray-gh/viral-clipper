"""
Video Editor Module
Converts source clips into YouTube Shorts format:
- Crop/reframe to 9:16 (1080x1920)
- Blur pillarbox fill for landscape videos
- Burn-in animated word-level captions
- Optional watermark and background music
- Uses ffmpeg for performance-critical operations
"""

import os
import json
import logging
import subprocess
import tempfile
import shutil
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

SHORT_WIDTH = 1080
SHORT_HEIGHT = 1920


@dataclass
class EditedShort:
    clip_id: str
    output_path: str
    duration: float
    resolution: tuple[int, int]
    has_captions: bool
    success: bool
    error: str = ""


class VideoEditor:
    def __init__(self, config: dict):
        self.cfg = config
        self.editor_cfg = config.get("editor", {})
        self.caption_cfg = self.editor_cfg.get("captions", {})
        self.bg_cfg = self.editor_cfg.get("background", {})
        self.brand_cfg = self.editor_cfg.get("branding", {})
        self.music_cfg = self.editor_cfg.get("music", {})
        self.shorts_dir = Path(config.get("publisher", {}).get("output_dir", "data/shorts"))
        self.clips_dir = Path("data/clips")
        self.shorts_dir.mkdir(parents=True, exist_ok=True)
        self.clips_dir.mkdir(parents=True, exist_ok=True)
        self._check_ffmpeg()

    def _check_ffmpeg(self):
        try:
            result = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
            if result.returncode != 0:
                raise RuntimeError("ffmpeg returned non-zero exit code")
        except FileNotFoundError:
            raise RuntimeError("ffmpeg not found — install ffmpeg and add to PATH")

    def create_short(
        self,
        source_path: str,
        start_time: float,
        end_time: float,
        clip_id: str,
        words: list = None
    ) -> EditedShort:
        """
        Full pipeline: extract clip → convert to 9:16 → add captions → finalize.
        """
        tmp_dir = None
        try:
            tmp_dir = tempfile.mkdtemp(prefix="vc_edit_")
            tmp_raw = os.path.join(tmp_dir, "raw.mp4")
            tmp_vertical = os.path.join(tmp_dir, "vertical.mp4")
            output_path = str(self.shorts_dir / f"{clip_id}.mp4")

            # Step 1: Extract clip segment
            logger.info(f"[{clip_id}] Extracting {start_time:.1f}s - {end_time:.1f}s")
            self._extract_clip(source_path, start_time, end_time, tmp_raw)

            # Step 2: Convert to vertical 9:16 format
            logger.info(f"[{clip_id}] Converting to 9:16 vertical format")
            self._convert_to_vertical(tmp_raw, tmp_vertical)

            # Step 3: Add captions if enabled
            if self.caption_cfg.get("enabled", True) and words:
                logger.info(f"[{clip_id}] Adding captions")
                tmp_captioned = os.path.join(tmp_dir, "captioned.mp4")
                self._add_captions(tmp_vertical, tmp_captioned, words, start_time)
                source_for_final = tmp_captioned
            else:
                source_for_final = tmp_vertical

            # Step 4: Add watermark if configured
            watermark = self.brand_cfg.get("watermark_text")
            if watermark:
                tmp_watermarked = os.path.join(tmp_dir, "watermarked.mp4")
                self._add_watermark(source_for_final, tmp_watermarked, watermark)
                source_for_final = tmp_watermarked

            # Step 5: Add background music if enabled
            if self.music_cfg.get("enabled", False):
                music_file = self._pick_music_track()
                if music_file:
                    tmp_music = os.path.join(tmp_dir, "music.mp4")
                    self._add_background_music(source_for_final, tmp_music, music_file)
                    source_for_final = tmp_music

            # Step 6: Final copy to output
            shutil.copy2(source_for_final, output_path)

            duration = end_time - start_time
            logger.info(f"[{clip_id}] Short created: {output_path}")

            return EditedShort(
                clip_id=clip_id,
                output_path=output_path,
                duration=duration,
                resolution=(SHORT_WIDTH, SHORT_HEIGHT),
                has_captions=bool(self.caption_cfg.get("enabled", True) and words),
                success=True
            )

        except Exception as e:
            logger.error(f"[{clip_id}] Editing failed: {e}")
            return EditedShort(
                clip_id=clip_id, output_path="",
                duration=0, resolution=(0, 0),
                has_captions=False, success=False, error=str(e)
            )
        finally:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir, ignore_errors=True)

    def _extract_clip(self, source: str, start: float, end: float, output: str):
        """Extract a time segment from source video using ffmpeg."""
        duration = end - start
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(start),
            "-i", source,
            "-t", str(duration),
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-avoid_negative_ts", "make_zero",
            output,
            "-loglevel", "error"
        ]
        self._run_cmd(cmd)

    def _convert_to_vertical(self, source: str, output: str):
        """
        Convert any aspect ratio to 9:16 (1080x1920) vertical format.
        For landscape: smart crop to 9:16, with blurred background fill.
        For portrait/square: scale to fit with blur padding.
        """
        blur_strength = self.bg_cfg.get("blur_strength", 25)

        # Detect input dimensions
        probe = self._probe_video(source)
        src_w = probe.get("width", 1920)
        src_h = probe.get("height", 1080)
        aspect = src_w / src_h if src_h > 0 else 1.0

        if aspect > 1.0:
            # Landscape → needs pillarbox treatment
            # Background: scale to full height, blur
            # Foreground: crop 9:16 from center, scale to fit
            vf = (
                f"[0:v]scale={SHORT_WIDTH}:{SHORT_HEIGHT}:force_original_aspect_ratio=increase,"
                f"crop={SHORT_WIDTH}:{SHORT_HEIGHT},gblur=sigma={blur_strength}[bg];"
                f"[0:v]scale=-2:{SHORT_HEIGHT}:force_original_aspect_ratio=decrease[fg];"
                f"[bg][fg]overlay=(W-w)/2:(H-h)/2"
            )
        else:
            # Portrait or square → just scale and pad
            vf = (
                f"scale={SHORT_WIDTH}:{SHORT_HEIGHT}:force_original_aspect_ratio=decrease,"
                f"pad={SHORT_WIDTH}:{SHORT_HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=#000000"
            )

        cmd = [
            "ffmpeg", "-y", "-i", source,
            "-filter_complex", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-r", "30",
            output,
            "-loglevel", "error"
        ]
        self._run_cmd(cmd)

    def _add_captions(self, source: str, output: str, words: list, clip_start_offset: float):
        """
        Burn word-level animated captions using ffmpeg drawtext filters.
        Words are grouped into lines and highlighted as spoken.
        """
        font_path = self.caption_cfg.get("font", "assets/fonts/Montserrat-Bold.ttf")
        font_size = self.caption_cfg.get("font_size", 52)
        color = self.caption_cfg.get("color", "white")
        stroke_color = self.caption_cfg.get("stroke_color", "black")
        stroke_width = self.caption_cfg.get("stroke_width", 3)
        highlight_color = self.caption_cfg.get("highlight_color", "#FFD700")
        words_per_line = self.caption_cfg.get("words_per_line", 4)
        position = self.caption_cfg.get("position", "center")
        highlight = self.caption_cfg.get("highlight_active_word", True)

        # Group words into lines
        lines = self._group_words_to_lines(words, clip_start_offset, words_per_line)

        if not lines:
            shutil.copy2(source, output)
            return

        # Build ffmpeg drawtext filters
        filters = []
        y_base = self._caption_y_position(position, font_size, len(lines))

        for line_idx, line in enumerate(lines):
            line_text = " ".join(w["word"].strip() for w in line["words"])
            # Escape special chars for ffmpeg
            line_text_esc = line_text.replace("'", "\\'").replace(":", "\\:").replace("\\", "\\\\")

            start = line["start"]
            end = line["end"] + 0.2  # brief linger

            y_pos = y_base + (line_idx % 3) * (font_size + 10)

            # Base line text (always visible during its duration)
            filters.append(
                f"drawtext=text='{line_text_esc}'"
                f":font='{font_path}'"
                f":fontsize={font_size}"
                f":fontcolor={color}@0.95"
                f":borderw={stroke_width}"
                f":bordercolor={stroke_color}@0.95"
                f":x=(w-text_w)/2"
                f":y={int(y_pos)}"
                f":enable='between(t,{start:.3f},{end:.3f})'"
            )

            # Word-level highlight overlay
            if highlight:
                for word_info in line["words"]:
                    w_start = word_info["start"]
                    w_end = word_info["end"] + 0.05
                    # Build highlighted version: word in gold, rest in white
                    words_in_line = [w["word"].strip() for w in line["words"]]
                    highlight_text = " ".join(
                        w if w != word_info["word"].strip() else w
                        for w in words_in_line
                    )
                    word_esc = word_info["word"].strip().replace("'", "\\'")

                    filters.append(
                        f"drawtext=text='{word_esc}'"
                        f":font='{font_path}'"
                        f":fontsize={int(font_size * 1.05)}"
                        f":fontcolor={highlight_color}@1.0"
                        f":borderw={stroke_width + 1}"
                        f":bordercolor={stroke_color}@1.0"
                        f":x=(w-text_w)/2"
                        f":y={int(y_pos) - 2}"
                        f":enable='between(t,{w_start:.3f},{w_end:.3f})'"
                    )

        vf = ",".join(filters)

        cmd = [
            "ffmpeg", "-y", "-i", source,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "copy",
            output,
            "-loglevel", "error"
        ]
        self._run_cmd(cmd)

    def _group_words_to_lines(self, words: list, offset: float, words_per_line: int) -> list[dict]:
        """Group WordTimestamp objects into caption lines."""
        lines = []
        current_line = []

        def make_line(word_group):
            if not word_group:
                return None
            return {
                "words": [{"word": w.word, "start": max(0, w.start - offset), "end": max(0, w.end - offset)} for w in word_group],
                "start": max(0, word_group[0].start - offset),
                "end": max(0, word_group[-1].end - offset)
            }

        for w in words:
            current_line.append(w)
            if len(current_line) >= words_per_line:
                line = make_line(current_line)
                if line:
                    lines.append(line)
                current_line = []

        if current_line:
            line = make_line(current_line)
            if line:
                lines.append(line)

        return lines

    def _caption_y_position(self, position: str, font_size: int, num_lines: int) -> int:
        block_height = num_lines * (font_size + 10)
        if position == "top":
            return SHORT_HEIGHT * 0.08
        elif position == "bottom":
            return SHORT_HEIGHT * 0.82 - block_height
        else:  # center
            return SHORT_HEIGHT * 0.55

    def _add_watermark(self, source: str, output: str, text: str):
        """Add text watermark to the video."""
        pos = self.brand_cfg.get("watermark_position", "top_right")
        opacity = self.brand_cfg.get("watermark_opacity", 0.7)

        if pos == "top_right":
            x, y = "w-text_w-20", "20"
        elif pos == "top_left":
            x, y = "20", "20"
        elif pos == "bottom_right":
            x, y = "w-text_w-20", "h-text_h-20"
        else:
            x, y = "20", "h-text_h-20"

        text_esc = text.replace("'", "\\'")
        vf = (
            f"drawtext=text='{text_esc}':fontsize=32:fontcolor=white@{opacity}"
            f":borderw=2:bordercolor=black@{opacity}:x={x}:y={y}"
        )
        cmd = [
            "ffmpeg", "-y", "-i", source,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "copy",
            output, "-loglevel", "error"
        ]
        self._run_cmd(cmd)

    def _add_background_music(self, source: str, output: str, music_path: str):
        """Mix background music with existing audio."""
        vol = self.music_cfg.get("volume", 0.15)
        fade_in = self.music_cfg.get("fade_in", 1.0)
        fade_out = self.music_cfg.get("fade_out", 2.0)

        cmd = [
            "ffmpeg", "-y", "-i", source, "-i", music_path,
            "-filter_complex",
            f"[1:a]volume={vol},afade=t=in:st=0:d={fade_in},afade=t=out:st=999:d={fade_out}[music];"
            "[0:a][music]amix=inputs=2:duration=first[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            output, "-loglevel", "error"
        ]
        self._run_cmd(cmd)

    def _pick_music_track(self) -> Optional[str]:
        """Pick a random royalty-free music track from assets/music/."""
        import random
        music_dir = Path("assets/music")
        tracks = list(music_dir.glob("*.mp3")) + list(music_dir.glob("*.wav"))
        return str(random.choice(tracks)) if tracks else None

    def _probe_video(self, path: str) -> dict:
        """Use ffprobe to get video dimensions."""
        try:
            cmd = [
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_streams", "-select_streams", "v:0", path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            data = json.loads(result.stdout)
            streams = data.get("streams", [{}])
            if streams:
                return {
                    "width": int(streams[0].get("width", 1920)),
                    "height": int(streams[0].get("height", 1080))
                }
        except Exception as e:
            logger.debug(f"ffprobe error: {e}")
        return {"width": 1920, "height": 1080}

    def _run_cmd(self, cmd: list):
        """Run a subprocess command, raising on error."""
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {' '.join(cmd[:4])}...\n{result.stderr[-500:]}")
