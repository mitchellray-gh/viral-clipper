"""
Transcription Module
Uses faster-whisper (local, free) to transcribe video audio
with word-level timestamps for precise clipping.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class WordTimestamp:
    word: str
    start: float   # seconds
    end: float     # seconds
    probability: float


@dataclass
class Segment:
    id: int
    text: str
    start: float
    end: float
    words: list[WordTimestamp] = field(default_factory=list)
    avg_logprob: float = 0.0
    no_speech_prob: float = 0.0

    @property
    def duration(self) -> float:
        return self.end - self.start

    @property
    def confidence(self) -> float:
        """Higher = more confident transcription."""
        return max(0.0, 1.0 + self.avg_logprob)


@dataclass
class Transcript:
    video_id: str
    language: str
    segments: list[Segment]
    full_text: str
    duration: float

    def get_text_window(self, start: float, end: float) -> str:
        """Get all transcript text within a time window."""
        parts = []
        for seg in self.segments:
            if seg.end >= start and seg.start <= end:
                # Partial overlap — include the segment
                parts.append(seg.text.strip())
        return " ".join(parts)

    def get_words_window(self, start: float, end: float) -> list[WordTimestamp]:
        """Get all word timestamps within a time window."""
        words = []
        for seg in self.segments:
            for w in seg.words:
                if w.start >= start and w.end <= end + 0.5:
                    words.append(w)
        return words

    def to_srt(self) -> str:
        """Export as SRT subtitle format."""
        lines = []
        for i, seg in enumerate(self.segments, 1):
            start_ts = _format_srt_time(seg.start)
            end_ts = _format_srt_time(seg.end)
            lines.append(f"{i}\n{start_ts} --> {end_ts}\n{seg.text.strip()}\n")
        return "\n".join(lines)


def _format_srt_time(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


class WhisperTranscriber:
    def __init__(self, config: dict):
        self.cfg = config.get("transcription", {})
        self.model_name = self.cfg.get("model", "base.en")
        self.device = self.cfg.get("device", "auto")
        self.compute_type = self.cfg.get("compute_type", "int8")
        self.language = self.cfg.get("language", "en")
        self.word_timestamps = self.cfg.get("word_timestamps", True)
        self.vad_filter = self.cfg.get("vad_filter", True)
        self._model = None

    def _load_model(self):
        if self._model is None:
            from faster_whisper import WhisperModel

            device = self.device
            if device == "auto":
                try:
                    import torch
                    device = "cuda" if torch.cuda.is_available() else "cpu"
                except ImportError:
                    device = "cpu"

            logger.info(f"Loading Whisper model '{self.model_name}' on {device}/{self.compute_type}")
            self._model = WhisperModel(
                self.model_name,
                device=device,
                compute_type=self.compute_type
            )
        return self._model

    def transcribe(self, video_path: str, video_id: str = "") -> Optional[Transcript]:
        """
        Transcribe a video file, returning a Transcript with word timestamps.
        Extracts audio first using ffmpeg for faster processing.
        """
        import subprocess
        import tempfile

        model = self._load_model()

        # Extract audio to WAV for faster processing
        audio_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                audio_path = tmp.name

            cmd = [
                "ffmpeg", "-y", "-i", video_path,
                "-ac", "1",           # mono
                "-ar", "16000",       # 16kHz (Whisper native)
                "-f", "wav",
                audio_path,
                "-loglevel", "error"
            ]
            subprocess.run(cmd, check=True, capture_output=True)

            logger.info(f"Transcribing {video_id or video_path} ...")
            segments_iter, info = model.transcribe(
                audio_path,
                language=self.language if self.language != "auto" else None,
                word_timestamps=self.word_timestamps,
                vad_filter=self.vad_filter,
                vad_parameters=dict(min_silence_duration_ms=500),
                beam_size=5,
                best_of=5
            )

            segments = []
            full_text_parts = []

            for seg in segments_iter:
                words = []
                if self.word_timestamps and seg.words:
                    for w in seg.words:
                        words.append(WordTimestamp(
                            word=w.word,
                            start=w.start,
                            end=w.end,
                            probability=w.probability
                        ))

                segments.append(Segment(
                    id=seg.id,
                    text=seg.text,
                    start=seg.start,
                    end=seg.end,
                    words=words,
                    avg_logprob=seg.avg_logprob,
                    no_speech_prob=seg.no_speech_prob
                ))
                full_text_parts.append(seg.text.strip())

            if not segments:
                logger.warning(f"No transcription segments for {video_id}")
                return None

            last_end = segments[-1].end if segments else 0

            return Transcript(
                video_id=video_id,
                language=info.language or self.language,
                segments=segments,
                full_text=" ".join(full_text_parts),
                duration=last_end
            )

        except Exception as e:
            logger.error(f"Transcription error for {video_id}: {e}")
            return None
        finally:
            if audio_path and os.path.exists(audio_path):
                os.unlink(audio_path)

    def transcribe_cached(self, video_path: str, video_id: str, cache_dir: str = "data/processed") -> Optional[Transcript]:
        """
        Transcribe with JSON caching — skips re-transcription if cache exists.
        """
        import json
        from pathlib import Path

        cache_path = Path(cache_dir) / f"{video_id}_transcript.json"

        if cache_path.exists():
            logger.info(f"Loading cached transcript for {video_id}")
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                segments = []
                for s in data["segments"]:
                    words = [WordTimestamp(**w) for w in s.get("words", [])]
                    segments.append(Segment(
                        id=s["id"], text=s["text"], start=s["start"], end=s["end"],
                        words=words, avg_logprob=s.get("avg_logprob", 0),
                        no_speech_prob=s.get("no_speech_prob", 0)
                    ))
                return Transcript(
                    video_id=data["video_id"],
                    language=data["language"],
                    segments=segments,
                    full_text=data["full_text"],
                    duration=data["duration"]
                )
            except Exception as e:
                logger.warning(f"Cache load failed for {video_id}: {e}")

        transcript = self.transcribe(video_path, video_id)
        if transcript:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                data = {
                    "video_id": transcript.video_id,
                    "language": transcript.language,
                    "full_text": transcript.full_text,
                    "duration": transcript.duration,
                    "segments": [
                        {
                            "id": s.id, "text": s.text,
                            "start": s.start, "end": s.end,
                            "avg_logprob": s.avg_logprob,
                            "no_speech_prob": s.no_speech_prob,
                            "words": [
                                {"word": w.word, "start": w.start, "end": w.end, "probability": w.probability}
                                for w in s.words
                            ]
                        }
                        for s in transcript.segments
                    ]
                }
                cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info(f"Transcript cached to {cache_path}")
            except Exception as e:
                logger.warning(f"Cache write failed: {e}")

        return transcript
