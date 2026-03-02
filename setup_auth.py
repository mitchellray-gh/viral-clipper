"""
YouTube OAuth2 Setup Helper
Run this once to authorize the pipeline to upload to your YouTube channel.
After completing this, the pipeline can publish clips fully automatically.

Usage:
    python setup_auth.py
"""

import os
import sys
import json
import webbrowser
from pathlib import Path

# ── Make sure we're running from the project root ────────────────────────────
os.chdir(Path(__file__).parent)
from dotenv import load_dotenv
load_dotenv(".env")

SECRETS_FILE = os.environ.get("YOUTUBE_CLIENT_SECRETS_FILE", "config/client_secrets.json")
TOKEN_FILE   = os.environ.get("YOUTUBE_TOKEN_FILE",          "config/youtube_token.json")
SCOPES       = ["https://www.googleapis.com/auth/youtube.upload",
                "https://www.googleapis.com/auth/youtube.readonly"]

BOLD  = "\033[1m"
GREEN = "\033[32m"
CYAN  = "\033[36m"
YELLOW = "\033[33m"
RED   = "\033[31m"
RESET = "\033[0m"


def banner(msg: str):
    print(f"\n{BOLD}{CYAN}{'═'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'═'*60}{RESET}\n")


def ok(msg):  print(f"  {GREEN}✓{RESET}  {msg}")
def info(msg): print(f"  {CYAN}→{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET}  {msg}")
def err(msg):  print(f"  {RED}✗{RESET}  {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Check for client_secrets.json
# ─────────────────────────────────────────────────────────────────────────────
def step1_check_secrets():
    banner("STEP 1 / 3 — Google Cloud OAuth2 Credentials")

    if Path(SECRETS_FILE).exists():
        try:
            data = json.loads(Path(SECRETS_FILE).read_text())
            kind = "web" if "web" in data else "installed"
            client_id = data.get(kind, {}).get("client_id", "")
            ok(f"client_secrets.json found ({kind} client, id: {client_id[:30]}...)")
            return True
        except Exception as e:
            warn(f"client_secrets.json exists but is invalid: {e}")

    err(f"client_secrets.json not found at: {SECRETS_FILE}")
    print(f"""
  You need to create an OAuth2 Desktop client in Google Cloud Console.
  This is a one-time setup. Here's how:

  {BOLD}1.{RESET} Open this URL in your browser:
     {CYAN}https://console.cloud.google.com/apis/credentials?project=gen-lang-client-0650732405{RESET}

  {BOLD}2.{RESET} Click {BOLD}+ CREATE CREDENTIALS{RESET} → {BOLD}OAuth client ID{RESET}

  {BOLD}3.{RESET} Application type: {BOLD}Desktop app{RESET}
     Name: {BOLD}Viral Clipper{RESET} (or anything)
     Click {BOLD}Create{RESET}

  {BOLD}4.{RESET} Click {BOLD}DOWNLOAD JSON{RESET} on the credentials that appear

  {BOLD}5.{RESET} Save the downloaded file as:
     {BOLD}{SECRETS_FILE}{RESET}

  {BOLD}6.{RESET} Also make sure {BOLD}YouTube Data API v3{RESET} is enabled:
     {CYAN}https://console.cloud.google.com/apis/api/youtube.googleapis.com/overview?project=gen-lang-client-0650732405{RESET}
""")
    input("  Press ENTER once you've saved client_secrets.json to continue... ")

    if not Path(SECRETS_FILE).exists():
        err("Still not found. Please save the file and run this script again.")
        sys.exit(1)

    ok("client_secrets.json found!")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Run OAuth2 flow (opens browser once)
# ─────────────────────────────────────────────────────────────────────────────
def step2_oauth_flow():
    banner("STEP 2 / 3 — Authorize YouTube Access (browser will open)")

    if Path(TOKEN_FILE).exists():
        # Try to load and validate existing token
        try:
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            if creds.valid:
                ok("Existing token is still valid — skipping browser auth")
                return creds
            if creds.expired and creds.refresh_token:
                info("Token expired — refreshing silently...")
                creds.refresh(Request())
                Path(TOKEN_FILE).write_text(creds.to_json())
                ok("Token refreshed and saved")
                return creds
        except Exception as e:
            warn(f"Existing token unusable ({e}) — re-authorizing...")

    from google_auth_oauthlib.flow import InstalledAppFlow

    info("Opening browser for YouTube authorization...")
    info("Log in and click Allow when asked about YouTube uploads.")
    print()

    flow = InstalledAppFlow.from_client_secrets_file(SECRETS_FILE, SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)

    Path(TOKEN_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(TOKEN_FILE).write_text(creds.to_json())
    ok(f"Token saved to {TOKEN_FILE}")
    return creds


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Verify the token works
# ─────────────────────────────────────────────────────────────────────────────
def step3_verify(creds):
    banner("STEP 3 / 3 — Verifying Channel Access")

    from googleapiclient.discovery import build

    youtube = build("youtube", "v3", credentials=creds)

    response = youtube.channels().list(
        part="snippet,statistics",
        mine=True
    ).execute()

    items = response.get("items", [])
    if not items:
        err("No YouTube channel found for this account.")
        err("Make sure you logged into an account that owns a YouTube channel.")
        sys.exit(1)

    channel = items[0]
    snippet = channel["snippet"]
    stats   = channel.get("statistics", {})
    channel_id = channel["id"]

    ok(f"Channel:      {BOLD}{snippet['title']}{RESET}")
    ok(f"Channel ID:   {channel_id}")
    ok(f"Subscribers:  {int(stats.get('subscriberCount', 0)):,}")
    ok(f"Total videos: {int(stats.get('videoCount', 0)):,}")

    # Save channel ID to .env if not already set
    env_path = Path(".env")
    env_text = env_path.read_text()
    if f"YOUTUBE_CHANNEL_ID={channel_id}" not in env_text:
        updated = "\n".join(
            line if not line.startswith("YOUTUBE_CHANNEL_ID=") else f"YOUTUBE_CHANNEL_ID={channel_id}"
            for line in env_text.splitlines()
        )
        if "YOUTUBE_CHANNEL_ID=" not in env_text:
            updated += f"\nYOUTUBE_CHANNEL_ID={channel_id}"
        env_path.write_text(updated)
        ok(f"YOUTUBE_CHANNEL_ID saved to .env")

    return channel_id


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print(f"""
{BOLD}╔══════════════════════════════════════════════════════════╗
║  VIRAL CLIPPER — YouTube Auth Setup                      ║
║  Run this once. After this, publishing is fully automated.║
╚══════════════════════════════════════════════════════════╝{RESET}
""")

    try:
        step1_check_secrets()
        creds = step2_oauth_flow()
        channel_id = step3_verify(creds)
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
        sys.exit(0)
    except Exception as e:
        err(f"Setup failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    print(f"""
{BOLD}{GREEN}╔══════════════════════════════════════════════════════════╗
║  ✓  Setup complete! Publishing is now fully automated.   ║
╚══════════════════════════════════════════════════════════╝{RESET}

  {BOLD}Next steps:{RESET}

  Run the pipeline manually:
    {CYAN}python pipeline.py run{RESET}

  Start the autonomous daily daemon (runs forever):
    {CYAN}python pipeline.py schedule{RESET}

  Or use the Streamlit dashboard:
    {CYAN}python -m streamlit run app.py --server.port 7860{RESET}

  The token is saved at {BOLD}{TOKEN_FILE}{RESET}
  It auto-refreshes — you never need to run this again.
""")


if __name__ == "__main__":
    main()
