"""
Setup script — run this once to verify environment and download assets.
Usage: python setup.py
"""

import sys
import os
import subprocess
from pathlib import Path


def check(label: str, ok: bool, hint: str = ""):
    status = "✓" if ok else "✗"
    print(f"  [{status}] {label}")
    if not ok and hint:
        print(f"       → {hint}")
    return ok


def main():
    print("=" * 55)
    print("  Viral Clipper — Environment Check")
    print("=" * 55)
    all_ok = True

    # Python version
    py_ok = sys.version_info >= (3, 11)
    all_ok &= check("Python 3.11+", py_ok, f"Current: {sys.version.split()[0]}. Install Python 3.11+")

    # ffmpeg
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        ffmpeg_ok = r.returncode == 0
    except FileNotFoundError:
        ffmpeg_ok = False
    all_ok &= check("ffmpeg installed", ffmpeg_ok, "Run: winget install ffmpeg  (then restart terminal)")

    # Python packages
    packages = [
        ("yt_dlp", "yt-dlp"),
        ("faster_whisper", "faster-whisper"),
        ("pytrends", "pytrends"),
        ("praw", "praw"),
        ("feedparser", "feedparser"),
        ("google.generativeai", "google-generativeai"),
        ("googleapiclient", "google-api-python-client"),
        ("google_auth_oauthlib", "google-auth-oauthlib"),
        ("yaml", "pyyaml"),
        ("dotenv", "python-dotenv"),
        ("apscheduler", "apscheduler"),
        ("PIL", "Pillow"),
    ]

    print("\n  Python packages:")
    for module, pkg in packages:
        try:
            __import__(module)
            check(f"  {pkg}", True)
        except ImportError:
            check(f"  {pkg}", False, f"pip install {pkg}")
            all_ok = False

    # Credentials file
    print("\n  Credentials:")
    env_file = Path("config/.env")
    env_ok = env_file.exists() and "your_gemini_api_key_here" not in env_file.read_text()
    if not env_file.exists():
        from shutil import copyfile
        copyfile("config/.env.example", "config/.env")
        print("  [!] Created config/.env from example — please fill in your API keys")
    all_ok &= check("config/.env configured", env_ok, "Edit config/.env and add your API keys")

    secrets_ok = Path("config/client_secrets.json").exists()
    check("config/client_secrets.json", secrets_ok,
          "Download OAuth2 credentials from Google Cloud Console")

    # Font
    print("\n  Assets:")
    font_path = Path("assets/fonts/Montserrat-Bold.ttf")
    font_ok = font_path.exists()
    if not font_ok:
        print("  [!] Montserrat-Bold.ttf not found — attempting download...")
        try:
            _download_font(font_path)
            font_ok = font_path.exists()
        except Exception as e:
            print(f"       Could not auto-download: {e}")
            print("       Manual download: https://fonts.google.com/specimen/Montserrat")
            print(f"       Save to: {font_path}")
    check("Montserrat-Bold.ttf font", font_ok,
          "Download from fonts.google.com/specimen/Montserrat → save to assets/fonts/")

    # Data dirs
    for d in ["data/downloads", "data/clips", "data/shorts", "data/processed", "logs"]:
        Path(d).mkdir(parents=True, exist_ok=True)
    check("Data directories", True)

    print("\n" + "=" * 55)
    if all_ok:
        print("  ✓ All checks passed! Run: python pipeline.py run")
    else:
        print("  Some checks failed. Fix the issues above, then run:")
        print("    pip install -r requirements.txt")
        print("    python setup.py")
    print("=" * 55)


def _download_font(dest: Path):
    """Attempt to download Montserrat Bold from Google Fonts."""
    import urllib.request
    import zipfile
    import io

    dest.parent.mkdir(parents=True, exist_ok=True)
    url = "https://fonts.gstatic.com/s/montserrat/v26/JTUHjIg1_i6t8kCHKm4532VJOt5-QNFgpCtr6Uw-.ttf"
    print(f"  Downloading from Google Fonts...")
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        dest.write_bytes(resp.read())
    print(f"  Font saved to {dest}")


if __name__ == "__main__":
    main()
