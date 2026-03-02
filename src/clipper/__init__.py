"""
Virality Clip Scorer
Uses an LLM (Groq/Llama by default) to analyze transcript windows and score them
for virality potential, finding the best moments to clip.
"""

import os
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .audio_energy import analyze_audio_energy, AudioEnergyProfile

logger = logging.getLogger(__name__)


@dataclass
class ClipCandidate:
    video_id: str
    source_url: str
    start_time: float          # seconds
    end_time: float            # seconds
    duration: float
    transcript_text: str
    virality_score: float      # 0.0 - 1.0 overall score
    hook_score: float          # strength of opening hook
    emotional_score: float     # emotional intensity
    info_score: float          # information surprise value
    completeness_score: float  # standalone completeness
    trend_score: float         # relevance to trending topic
    hook_phrase: str = ""      # the opening words / hook text
    title_suggestion: str = ""
    reasoning: str = ""
    trend_keyword: str = ""
    metadata: dict = field(default_factory=dict)

    def __str__(self):
        return (f"[{self.video_id}] {self.start_time:.1f}s-{self.end_time:.1f}s "
                f"score={self.virality_score:.2f} | {self.hook_phrase[:50]}")


class ViralityScorer:
    def __init__(self, config: dict):
        self.cfg = config.get("clipper", {})
        self.model_name = self.cfg.get("llm_model", "llama-3.3-70b-versatile")
        self.threshold = self.cfg.get("virality_threshold", 0.65)
        self.clips_per_video = self.cfg.get("clips_per_video", 3)
        self.weights = self.cfg.get("score_weights", {
            "hook_strength": 0.30,
            "emotional_peak": 0.25,
            "information_density": 0.20,
            "completeness": 0.15,
            "trending_relevance": 0.10
        })
        self.min_clip = config.get("pipeline", {}).get("min_clip_length", 20)
        self.max_clip = config.get("pipeline", {}).get("max_clip_length", 58)
        self._llm_client = None

    def _get_client(self):
        if self._llm_client is None:
            from groq import Groq
            api_key = os.environ.get("GROQ_API_KEY", "")
            if not api_key:
                raise ValueError("GROQ_API_KEY environment variable not set")
            self._llm_client = Groq(api_key=api_key)
        return self._llm_client

    def find_clips(self, transcript, video_id: str, source_url: str,
                   trend_keyword: str = "", video_path: Optional[str] = None,
                   source_metadata: Optional[dict] = None) -> list[ClipCandidate]:
        """
        Analyze a transcript's segments using a sliding window approach,
        group into candidate clips, score them with Gemini, and return
        the top clips above the virality threshold.
        """
        if not transcript or not transcript.segments:
            return []

        # Analyze audio energy up front (provides reaction/laughter peak detection)
        audio_profile: Optional[AudioEnergyProfile] = None
        if video_path:
            try:
                audio_profile = analyze_audio_energy(video_path)
                peak_count = len(audio_profile.peak_moments)
                logger.info(f"Audio energy analyzed: {peak_count} peaks found")
            except Exception as e:
                logger.warning(f"Audio energy analysis failed: {e}")

        windows = self._build_windows(transcript)
        if not windows:
            return []

        logger.info(f"Scoring {len(windows)} clip windows for {video_id}...")
        candidates = []

        # Batch windows to reduce API calls (send multiple in one prompt)
        batch_size = 5
        for i in range(0, len(windows), batch_size):
            batch = windows[i:i + batch_size]
            try:
                scored = self._score_batch(batch, trend_keyword, video_id,
                                           audio_profile=audio_profile,
                                           source_metadata=source_metadata or {})
                candidates.extend(scored)
                time.sleep(0.5)  # small pause between batches
            except Exception as e:
                logger.warning(f"Batch scoring failed at index {i}: {e}")
                time.sleep(5)

        # Filter by threshold and deduplicate overlapping clips
        valid = [c for c in candidates if c.virality_score >= self.threshold]
        valid.sort(key=lambda c: c.virality_score, reverse=True)
        deduped = self._deduplicate_clips(valid)

        # Attach source metadata
        for clip in deduped:
            clip.video_id = video_id
            clip.source_url = source_url
            clip.trend_keyword = trend_keyword

        top = deduped[:self.clips_per_video]
        logger.info(f"Found {len(top)} viable clips for {video_id} (from {len(candidates)} candidates)")
        return top

    def _build_windows(self, transcript) -> list[dict]:
        """
        Build overlapping time windows of target clip length
        aligned to sentence/segment boundaries.
        """
        segs = transcript.segments
        windows = []
        target_dur = (self.min_clip + self.max_clip) / 2  # ~39s target
        step = target_dur * 0.5  # 50% overlap

        i = 0
        while i < len(segs):
            # Accumulate segments until we have enough duration
            window_segs = []
            start_time = segs[i].start
            current_dur = 0

            for j in range(i, len(segs)):
                seg = segs[j]
                # Skip low-confidence segments (silence / noise / uncertain speech)
                if not self._is_confident_segment(seg):
                    continue
                window_segs.append(seg)
                current_dur = segs[j].end - start_time
                if current_dur >= target_dur:
                    break

            if current_dur < self.min_clip:
                # Reached end without enough content
                break

            end_time = min(start_time + self.max_clip, window_segs[-1].end)
            text = " ".join(s.text.strip() for s in window_segs)

            windows.append({
                "start": round(start_time, 2),
                "end": round(end_time, 2),
                "duration": round(end_time - start_time, 2),
                "text": text
            })

            # Advance by roughly one step
            advance_time = start_time + step
            while i < len(segs) and segs[i].start < advance_time:
                i += 1

        return windows

    @staticmethod
    def _is_confident_segment(seg) -> bool:
        """Return False for segments Whisper flagged as unreliable."""
        no_speech = getattr(seg, "no_speech_prob", 0.0)
        avg_logprob = getattr(seg, "avg_logprob", 0.0)
        # Whisper marks background noise / silence with high no_speech_prob
        # and low avg_logprob (very negative = uncertain transcription)
        return no_speech <= 0.5 and avg_logprob >= -1.0

    def _score_batch(self, windows: list[dict], trend_keyword: str, video_id: str,
                     audio_profile: Optional[AudioEnergyProfile] = None,
                     source_metadata: Optional[dict] = None) -> list[ClipCandidate]:
        """Send a batch of windows to Gemini for scoring."""
        client = self._get_client()
        meta = source_metadata or {}

        # Build per-window audio energy scores
        audio_scores = {}
        if audio_profile:
            for idx, w in enumerate(windows):
                audio_scores[idx] = round(audio_profile.score_window(w["start"], w["end"]), 3)

        # Engagement context from source video
        views_per_day = meta.get("views_per_day", 0)
        like_ratio = meta.get("like_ratio", 0)  # likes / views
        comment_count = meta.get("comment_count", 0)
        trend_phase = meta.get("trend_phase", "unknown")
        breakout = meta.get("breakout_topic", False)

        engagement_ctx = (
            f"Source video context:\n"
            f"- Views/day: {views_per_day:,.0f} (higher = more viral momentum)\n"
            f"- Like ratio: {like_ratio:.2%} (>4% = very engaging)\n"
            f"- Comments: {comment_count:,}\n"
            f"- Trend phase: {trend_phase}" + (" [BREAKOUT - act fast!]" if breakout else "") + "\n"
        ) if views_per_day or like_ratio else ""

        windows_json = json.dumps([
            {
                "index": idx,
                "start": w["start"],
                "end": w["end"],
                "duration": w["duration"],
                "text": w["text"][:800],
                "audio_energy": audio_scores.get(idx, None),  # 0-1, higher = louder/more intense audio peaks
            }
            for idx, w in enumerate(windows)
        ], ensure_ascii=False, indent=2)

        prompt = f"""You are a YouTube Shorts virality expert. Analyze these transcript clips and score each for virality potential to rapidly grow a YouTube channel.

Trending topic context: "{trend_keyword}"
{engagement_ctx}
Proven viral hook patterns (score higher if present):
- Shocking statistic or counterintuitive fact ("99% of people don't know...")
- Personal revelation or confession ("I made $X doing this one thing...")
- Direct challenge or controversy ("Everyone is wrong about...")
- How-to with instant value ("Here's how to... in 30 seconds")
- Before/after transformation or result reveal
- Cliffhanger or unresolved tension requiring a conclusion
- Celebrity or well-known brand name reference

Transcript windows to score (audio_energy: 0-1, higher = emotional audio peak like laughter/cheers/gasp):
{windows_json}

For each window, evaluate:
1. hook_strength (0.0-1.0): Punchy opening — matches any proven hook pattern above?
2. emotional_peak (0.0-1.0): Emotional intensity — humor, shock, awe, inspiration, controversy? Weight audio_energy heavily if provided.
3. information_density (0.0-1.0): Surprising facts, statistics, revelations, counterintuitive ideas?
4. completeness (0.0-1.0): Makes sense as standalone short with clear arc/payoff?
5. trending_relevance (0.0-1.0): How relevant to trending topic "{trend_keyword}"?

Respond with ONLY valid JSON array, no markdown:
[
  {{
    "index": 0,
    "hook_score": 0.8,
    "emotional_score": 0.7,
    "info_score": 0.6,
    "completeness_score": 0.75,
    "trend_score": 0.9,
    "hook_phrase": "first 8-10 words of the clip",
    "title_suggestion": "viral youtube short title under 80 chars",
    "reasoning": "one sentence why this would go viral"
  }},
  ...
]"""

        response = client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        text = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:])
        if text.endswith("```"):
            text = "\n".join(text.split("\n")[:-1])

        scored_data = json.loads(text.strip())

        candidates = []
        weights = self.weights
        for item in scored_data:
            idx = item.get("index", 0)
            if idx >= len(windows):
                continue
            w = windows[idx]

            hook = float(item.get("hook_score", 0))
            emo = float(item.get("emotional_score", 0))
            info = float(item.get("info_score", 0))
            comp = float(item.get("completeness_score", 0))
            trend = float(item.get("trend_score", 0))

            # Audio energy weight: 0.10, redistributed from completeness (was 0.15 → 0.10)
            audio_e = float(audio_scores.get(idx, emo))  # fallback to emotional score

            overall = (
                hook * weights.get("hook_strength", 0.30)
                + emo * weights.get("emotional_peak", 0.25)
                + info * weights.get("information_density", 0.20)
                + comp * 0.10                                  # reduced from 0.15
                + trend * weights.get("trending_relevance", 0.10)
                + audio_e * 0.05                              # small audio energy bonus
            )

            candidates.append(ClipCandidate(
                video_id="",
                source_url="",
                start_time=w["start"],
                end_time=w["end"],
                duration=w["duration"],
                transcript_text=w["text"],
                virality_score=round(overall, 3),
                hook_score=hook,
                emotional_score=emo,
                info_score=info,
                completeness_score=comp,
                trend_score=trend,
                hook_phrase=item.get("hook_phrase", ""),
                title_suggestion=item.get("title_suggestion", ""),
                reasoning=item.get("reasoning", "")
            ))

        return candidates

    def _deduplicate_clips(self, clips: list[ClipCandidate]) -> list[ClipCandidate]:
        """Remove overlapping clips, keeping the higher-scored one."""
        if not clips:
            return []

        kept = [clips[0]]
        for clip in clips[1:]:
            overlap = False
            for k in kept:
                # If clips overlap by more than 50% of the shorter clip
                overlap_start = max(clip.start_time, k.start_time)
                overlap_end = min(clip.end_time, k.end_time)
                if overlap_end > overlap_start:
                    overlap_dur = overlap_end - overlap_start
                    shorter = min(clip.duration, k.duration)
                    if overlap_dur / shorter > 0.5:
                        overlap = True
                        break
            if not overlap:
                kept.append(clip)

        return kept
