"""
Metadata Generator
Uses Google Gemini to generate viral titles, descriptions,
tags, and hashtags for each YouTube Short.
"""

import os
import json
import logging
import time
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ShortMetadata:
    title: str
    description: str
    tags: list[str]
    hashtags: list[str]
    category_id: str
    default_language: str = "en"

    @property
    def full_description(self) -> str:
        """Description + hashtags combined."""
        hashtag_str = " ".join(f"#{h.lstrip('#')}" for h in self.hashtags)
        return f"{self.description}\n\n{hashtag_str}"

    def __str__(self):
        return f'"{self.title}" | {len(self.tags)} tags | {len(self.hashtags)} hashtags'


class MetadataGenerator:
    def __init__(self, config: dict):
        self.cfg = config.get("metadata", {})
        self.model_name = self.cfg.get("gemini_model", "gemini-1.5-flash")
        self.max_title = self.cfg.get("max_title_length", 100)
        self.max_desc = self.cfg.get("max_description_length", 500)
        self.hashtag_count = self.cfg.get("hashtag_count", 10)
        self.category_id = self.cfg.get("default_category", "22")
        self.default_tags = self.cfg.get("default_tags", ["shorts", "viral", "trending"])
        self._model = None

    def _get_model(self):
        if self._model is None:
            import google.generativeai as genai
            api_key = os.environ.get("GOOGLE_API_KEY", "")
            if not api_key:
                raise ValueError("GOOGLE_API_KEY not set")
            genai.configure(api_key=api_key)
            self._model = genai.GenerativeModel(self.model_name)
        return self._model

    def generate(self, clip_candidate, source_title: str = "") -> ShortMetadata:
        """
        Generate full metadata for a clip candidate using Gemini.
        Falls back to rule-based generation if API call fails.
        """
        try:
            return self._generate_with_gemini(clip_candidate, source_title)
        except Exception as e:
            logger.warning(f"Gemini metadata generation failed: {e} — using fallback")
            return self._fallback_metadata(clip_candidate, source_title)

    def _generate_with_gemini(self, clip_candidate, source_title: str) -> ShortMetadata:
        model = self._get_model()

        prompt = f"""You are a viral YouTube Shorts content strategist. Generate compelling metadata for a YouTube Short.

Context:
- Trending topic: {clip_candidate.trend_keyword}
- Source video title: {source_title}
- Clip opening hook: "{clip_candidate.hook_phrase}"
- Clip content summary: {clip_candidate.transcript_text[:400]}
- Suggested title from analysis: {clip_candidate.title_suggestion}
- Why it's viral: {clip_candidate.reasoning}

Generate YouTube Shorts metadata optimized for maximum discoverability and click-through rate.

Requirements:
- Title: Under {self.max_title} chars. Punchy, curiosity-driving, no clickbait promises you can't keep. Use power words.
- Description: Under {self.max_desc} chars. Natural language, key context, CTA like "Follow for more".
- Tags: 15-20 relevant SEO tags (single words or short phrases, no # prefix)
- Hashtags: Exactly {self.hashtag_count} trending hashtags (no # prefix in the list)

Respond ONLY with valid JSON (no markdown):
{{
  "title": "...",
  "description": "...",
  "tags": ["tag1", "tag2", ...],
  "hashtags": ["Shorts", "Viral", ...]
}}"""

        time.sleep(1)  # Gemini free tier rate limit
        response = model.generate_content(prompt)
        text = response.text.strip()

        # Strip markdown fences
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
        if text.endswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[:-1])

        data = json.loads(text.strip())

        title = data.get("title", clip_candidate.title_suggestion or source_title)[:self.max_title]
        description = data.get("description", "")[:self.max_desc]
        tags = list(dict.fromkeys(data.get("tags", []) + self.default_tags))[:30]
        hashtags = data.get("hashtags", ["Shorts", "Viral"])[:self.hashtag_count]

        # Ensure #Shorts is always included (required for YouTube Shorts classification)
        if "Shorts" not in hashtags and "shorts" not in [h.lower() for h in hashtags]:
            hashtags.insert(0, "Shorts")

        return ShortMetadata(
            title=title,
            description=description,
            tags=tags,
            hashtags=hashtags,
            category_id=self.category_id
        )

    def _fallback_metadata(self, clip_candidate, source_title: str) -> ShortMetadata:
        """Rule-based fallback if Gemini is unavailable."""
        title = clip_candidate.title_suggestion or source_title or clip_candidate.hook_phrase
        title = title[:self.max_title]

        description = (
            f"{clip_candidate.hook_phrase}\n\n"
            f"About: {clip_candidate.transcript_text[:200]}...\n\n"
            "Follow for more viral content daily!"
        )[:self.max_desc]

        # Extract simple keywords from hook + content
        text = f"{clip_candidate.trend_keyword} {clip_candidate.hook_phrase} {clip_candidate.transcript_text}"
        words = re.findall(r"\b[a-z]{4,}\b", text.lower())
        word_freq = {}
        for w in words:
            word_freq[w] = word_freq.get(w, 0) + 1
        top_words = sorted(word_freq, key=word_freq.get, reverse=True)[:15]
        tags = list(dict.fromkeys(self.default_tags + [clip_candidate.trend_keyword] + top_words))[:25]

        hashtags = ["Shorts", "Viral", "Trending", "FYP", clip_candidate.trend_keyword.replace(" ", "")]
        hashtags = [h for h in hashtags if h][:self.hashtag_count]

        return ShortMetadata(
            title=title, description=description, tags=tags, hashtags=hashtags,
            category_id=self.category_id
        )
