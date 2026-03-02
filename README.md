# Viral Clipper — YouTube Shorts Automation Pipeline

Fully automated system that monitors trending topics, finds viral source content on YouTube, clips the best moments, formats them as Shorts (9:16, captions, branding), and schedules uploads to your channel daily.

---

## How It Works

```
Google Trends ──┐
Reddit          ├──► TrendAggregator ──► ContentDiscovery ──► VideoDownloader
RSS Feeds       │         (niches)          (YouTube videos)     (yt-dlp)
HackerNews      │
YouTube Trending┘

VideoDownloader ──► WhisperTranscriber ──► ViralityScorer (Gemini)
                     (local, free)              (finds best moments)

ViralityScorer ──► VideoEditor ──► MetadataGenerator ──► ContentQueue ──► YouTubePublisher
                   (9:16 crop,    (Gemini: title,           (SQLite         (auto-upload
                   captions,       description,              schedule)       + local save)
                   watermark)      tags, hashtags)
```

---

## Quick Start

### 1. Install Prerequisites

**Python 3.11+**
```powershell
winget install Python.Python.3.11
```

**ffmpeg** (required for video processing)
```powershell
winget install ffmpeg
```

### 2. Install Python Dependencies
```powershell
cd C:\viral-clipper
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Set Up Credentials

Copy and fill in the credentials file:
```powershell
Copy-Item config\.env.example config\.env
notepad config\.env
```

#### Required API Keys:

| Service | Where to get it | Cost |
|---------|----------------|------|
| **Google Gemini** | [aistudio.google.com](https://aistudio.google.com/app/apikey) | Free (15 RPM, 1M tokens/day) |
| **YouTube Data API v3** | [console.cloud.google.com](https://console.cloud.google.com) → APIs → YouTube Data API v3 | Free (10,000 units/day ≈ 6 uploads) |
| **YouTube OAuth2 Client** | Google Cloud Console → Credentials → OAuth 2.0 Client ID | Free |
| **Reddit API** | [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) → Create App (script) | Free |

#### YouTube OAuth Setup (for uploads):
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a project → Enable "YouTube Data API v3"
3. Create OAuth 2.0 credentials → Download as `config/client_secrets.json`
4. First pipeline run will open a browser to authorize your YouTube channel

### 4. Run the Pipeline

**Single full run** (discover → clip → schedule → publish):
```powershell
python pipeline.py run
```

**Publish due clips only:**
```powershell
python pipeline.py publish
```

**Check queue status:**
```powershell
python pipeline.py status
```

**Preview discovered videos without processing:**
```powershell
python pipeline.py discover
```

**Run as daily daemon** (auto-runs at 6 AM UTC + publishes hourly):
```powershell
python pipeline.py schedule
```

---

## Configuration

Edit `config/settings.yaml` to customize:

| Setting | Description | Default |
|---------|-------------|---------|
| `pipeline.clips_per_day` | Target daily clips | 10 |
| `pipeline.max_clip_length` | Short max length (sec) | 58 |
| `pipeline.run_hour` | Daily run hour (UTC) | 6 |
| `trends.reddit.subreddits` | Subreddits to monitor | worldnews, tech, gaming... |
| `trends.rss.feeds` | RSS feeds to monitor | BBC, Reuters, TechCrunch... |
| `clipper.virality_threshold` | Min score to keep clip (0-1) | 0.65 |
| `clipper.clips_per_video` | Max clips per source video | 3 |
| `queue.posts_per_day` | Shorts to post per day | 3 |
| `queue.posting_times` | UTC hours to post | 14, 18, 23 |
| `editor.captions.enabled` | Burn-in captions | true |
| `editor.branding.watermark_text` | Your @handle watermark | null |
| `publisher.privacy_status` | Video privacy | public |

### Adding Background Music
Drop royalty-free MP3 files into `assets/music/` and set `editor.music.enabled: true` in settings.

### Adding Your Watermark
Set `editor.branding.watermark_text: "@YourHandle"` in settings.

---

## Trend Sources

| Source | Auth Required | Data |
|--------|--------------|------|
| Google Trends | None | Real-time trending searches |
| Reddit | Client ID + Secret | Hot/rising posts by subreddit |
| RSS Feeds | None | BBC, Reuters, TechCrunch, ESPN, etc. |
| YouTube Trending | API Key | Most popular videos by category |
| HackerNews | None | Top/best stories (100% free) |

---

## Directory Structure

```
viral-clipper/
├── pipeline.py              # Main entry point
├── config/
│   ├── settings.yaml        # All configuration
│   ├── .env                 # Your credentials (never commit)
│   └── client_secrets.json  # YouTube OAuth (never commit)
├── src/
│   ├── trends/              # Trend aggregation (Google, Reddit, RSS, YT, HN)
│   ├── discovery/           # YouTube video search
│   ├── downloader/          # yt-dlp video downloader
│   ├── transcription/       # faster-whisper local transcription
│   ├── clipper/             # Gemini virality scorer
│   ├── editor/              # ffmpeg 9:16 editor + captions
│   ├── metadata/            # Gemini title/tag generator
│   ├── queue/               # SQLite content queue + scheduler
│   └── publisher/           # YouTube Data API uploader
├── data/
│   ├── downloads/           # Raw source videos (auto-purged after 3 days)
│   ├── clips/               # Intermediate clips
│   ├── shorts/              # Final Shorts ready to post (organized by date)
│   └── queue.db             # SQLite queue database
└── assets/
    ├── fonts/               # Caption fonts (add Montserrat-Bold.ttf here)
    └── music/               # Background music MP3s
```

---

## Fonts

The caption system uses `assets/fonts/Montserrat-Bold.ttf` by default.

Download free: [fonts.google.com/specimen/Montserrat](https://fonts.google.com/specimen/Montserrat)

Or change the font in `config/settings.yaml` → `editor.captions.font`.

---

## Monetization Strategy

The pipeline is designed to maximize your path to brand deals:

1. **Volume**: 3 posts/day = 90+ Shorts/month, builds the backlog fast
2. **Niche targeting**: Multi-source trend aggregation catches niches 12-48h before they peak
3. **Discoverability**: Gemini generates SEO-optimized titles, 10 trending hashtags, 20 tags
4. **Watch time**: Word-level highlighted captions keep audience engagement high
5. **Consistency**: Scheduler posts at optimal times (9am, 1pm, 6pm EST) even while you sleep
6. **Scaling**: Once monetized, increase `posts_per_day` and `max_videos` in settings

**YouTube Partner Program threshold**: 500 subscribers + 3,000 watch hours (Shorts program)

---

## Costs

| Service | Free Tier | Typical daily usage |
|---------|-----------|-------------------|
| Google Gemini | 15 RPM, 1M tokens/day | ~50k tokens/day for 10 clips |
| YouTube API | 10,000 units/day | 1,600 units/upload → 6 uploads |
| Reddit API | Free | Well within limits |
| HackerNews | Free | Unlimited |
| faster-whisper | Free (local) | CPU uses ~2 min/video |
| ffmpeg | Free | Local processing |

**Total cost: $0/day** for standard operation (up to 6 uploads/day)

---

## Troubleshooting

**ffmpeg not found**: Install via `winget install ffmpeg` and restart terminal

**Quota exceeded**: YouTube free tier = 10k units/day. Reduce `posts_per_day` to 2-3.

**Gemini rate limit**: Free tier is 15 RPM. Pipeline auto-throttles with 1.5s delays.

**No clips found**: Lower `clipper.virality_threshold` to 0.50 in settings.

**yt-dlp blocked**: Add cookies.txt from your browser: set `downloader.cookies_file` in settings.
