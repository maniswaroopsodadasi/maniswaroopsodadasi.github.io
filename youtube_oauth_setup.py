#!/usr/bin/env python3
"""
YouTube OAuth2 Setup — run ONCE locally to get your refresh token.

Steps
-----
1. Go to Google Cloud Console → APIs & Services → Credentials
2. Create an OAuth 2.0 Client ID → Desktop app → Download JSON
3. Save it as  youtube_client_secret.json  next to this file
4. Run:  python youtube_oauth_setup.py
5. Copy the two values into GitHub Secrets:
      YOUTUBE_CLIENT_SECRET_B64   (printed here)
      YOUTUBE_REFRESH_TOKEN       (printed here)
"""

import base64
import json
import sys
import webbrowser
import urllib.parse
import http.server
import threading
from pathlib import Path

SECRET_FILE = Path("youtube_client_secret.json")
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
REDIRECT_URI = "http://localhost:8080"

# ── helpers ───────────────────────────────────────────────────────────────────

def load_client_secret() -> dict:
    if not SECRET_FILE.exists():
        print(f"❌  {SECRET_FILE} not found.")
        print("    Download it from Google Cloud Console → APIs & Services → Credentials")
        print("    (Create OAuth 2.0 Client ID → Desktop app → Download JSON)")
        sys.exit(1)
    data = json.loads(SECRET_FILE.read_text())
    return data.get("installed") or data.get("web", {})


def build_auth_url(client_id: str) -> str:
    params = {
        "client_id":     client_id,
        "redirect_uri":  REDIRECT_URI,
        "response_type": "code",
        "scope":         " ".join(SCOPES),
        "access_type":   "offline",
        "prompt":        "consent",  # forces refresh_token to be returned
    }
    return "https://accounts.google.com/o/oauth2/auth?" + urllib.parse.urlencode(params)


def exchange_code_for_tokens(client_id: str, client_secret: str, auth_code: str) -> dict:
    import urllib.request
    data = urllib.parse.urlencode({
        "code":          auth_code,
        "client_id":     client_id,
        "client_secret": client_secret,
        "redirect_uri":  REDIRECT_URI,
        "grant_type":    "authorization_code",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# ── tiny local server to capture the auth code ───────────────────────────────

_auth_code: list = []  # shared state between threads


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        code   = params.get("code", [""])[0]
        _auth_code.append(code)
        self.send_response(200)
        self.end_headers()
        msg = b"<h2>Authorized! You can close this tab.</h2>"
        self.wfile.write(msg)

    def log_message(self, *args):
        pass  # silence request logs


def wait_for_auth_code() -> str:
    server = http.server.HTTPServer(("localhost", 8080), _Handler)
    t = threading.Thread(target=server.handle_request)
    t.daemon = True
    t.start()
    t.join(timeout=120)
    return _auth_code[0] if _auth_code else ""


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  YouTube OAuth2 Setup for Fabric 100 Days Automation")
    print("=" * 60)
    print()

    ci = load_client_secret()
    client_id     = ci["client_id"]
    client_secret = ci["client_secret"]

    auth_url = build_auth_url(client_id)
    print("Opening browser for Google authorization…")
    print(f"\n  {auth_url}\n")
    webbrowser.open(auth_url)

    print("Waiting for authorization (up to 120 s)…")
    auth_code = wait_for_auth_code()
    if not auth_code:
        print("❌  No authorization code received. Try again.")
        sys.exit(1)

    print("✅  Authorization code received. Exchanging for tokens…")
    tokens = exchange_code_for_tokens(client_id, client_secret, auth_code)

    refresh_token = tokens.get("refresh_token", "")
    if not refresh_token:
        print("❌  No refresh_token in response. Make sure you used prompt=consent.")
        print(json.dumps(tokens, indent=2))
        sys.exit(1)

    # Base64-encode the entire client_secret.json file
    b64_secret = base64.b64encode(SECRET_FILE.read_bytes()).decode().rstrip("=")

    print()
    print("=" * 60)
    print("  Add these two secrets to GitHub:")
    print("  (Settings → Secrets and variables → Actions → New repository secret)")
    print("=" * 60)
    print()
    print(f"Secret name : YOUTUBE_CLIENT_SECRET_B64")
    print(f"Secret value: {b64_secret}")
    print()
    print(f"Secret name : YOUTUBE_REFRESH_TOKEN")
    print(f"Secret value: {refresh_token}")
    print()
    print("Done! YouTube automation is ready.")

    # Also save locally so you can verify
    Path("youtube_tokens_local.json").write_text(json.dumps({
        "client_secret_b64": b64_secret,
        "refresh_token": refresh_token,
    }, indent=2))
    print(f"\n(Also saved to youtube_tokens_local.json — do NOT commit this file!)")


if __name__ == "__main__":
    main()
