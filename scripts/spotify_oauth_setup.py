"""
One-time Spotify OAuth (Authorization Code) setup.

Produces a long-lived refresh token for a USER account, which the seeder uses
to read playlist search + tracks via the Web API (the web-player cookie token
and client-credentials no longer work for playlist track reads).

Usage:
    # 1. Add this redirect URI to the app at developer.spotify.com:
    #        http://127.0.0.1:8888/callback
    # 2. Print the authorize URL, open it, approve with the THROWAWAY account:
    python scripts/spotify_oauth_setup.py auth
    # 3. Copy the full URL you get redirected to (starts with http://127.0.0.1:8888/…)
    #    and exchange it for tokens (also tests a real playlist read):
    python scripts/spotify_oauth_setup.py exchange "<paste redirect URL or just the code>"
"""
import os
import sys
import base64
import urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

CLIENT_ID     = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
REDIRECT_URI  = "http://127.0.0.1:8888/callback"
SCOPES        = "playlist-read-private playlist-read-collaborative"
# A public, user-owned playlist to smoke-test track reads after auth.
TEST_UGC_PLAYLIST = "51NIMAKN9Qpe1nzsXGfFH8"


def auth_url():
    q = urllib.parse.urlencode({
        "client_id":     CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  REDIRECT_URI,
        "scope":         SCOPES,
        "show_dialog":   "true",
    })
    url = f"https://accounts.spotify.com/authorize?{q}"
    print("\n1. Make sure this redirect URI is registered on the app:")
    print(f"     {REDIRECT_URI}")
    print("\n2. Open this URL, log in with the THROWAWAY account, click Agree:\n")
    print(url)
    print("\n3. Your browser will fail to load 127.0.0.1:8888 — that's expected.")
    print("   Copy the FULL URL from the address bar and run:")
    print('     python scripts/spotify_oauth_setup.py exchange "<that URL>"\n')


def _extract_code(arg: str) -> str:
    if arg.startswith("http"):
        qs = urllib.parse.urlparse(arg).query
        params = urllib.parse.parse_qs(qs)
        if "error" in params:
            sys.exit(f"Authorization error from Spotify: {params['error'][0]}")
        return params["code"][0]
    return arg.strip()


def exchange(redirect_arg: str):
    code = _extract_code(redirect_arg)
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type":   "authorization_code",
            "code":         code,
            "redirect_uri": REDIRECT_URI,
        },
        headers={"Authorization": f"Basic {basic}"},
        timeout=15,
    )
    if not r.ok:
        sys.exit(f"Token exchange failed [{r.status_code}]: {r.text}")
    tok = r.json()
    access  = tok["access_token"]
    refresh = tok.get("refresh_token")
    print(f"\n✓ Got tokens. access_token len={len(access)}  refresh_token present={bool(refresh)}")

    # Smoke test: can this USER token read a public playlist's tracks?
    print(f"\nTesting playlist track read with the user token ...")
    tr = requests.get(
        f"https://api.spotify.com/v1/playlists/{TEST_UGC_PLAYLIST}/tracks",
        params={"limit": 3, "fields": "items(track(name,artists(name)))"},
        headers={"Authorization": f"Bearer {access}"},
        timeout=15,
    )
    print(f"  GET /v1/playlists/{TEST_UGC_PLAYLIST}/tracks -> {tr.status_code}")
    if tr.ok:
        items = tr.json().get("items", [])
        for it in items:
            t = it.get("track") or {}
            print(f"    - {t.get('name')} — {(t.get('artists') or [{}])[0].get('name')}")
        print("\n✅ USER OAUTH TOKEN CAN READ PLAYLIST TRACKS — this fixes the seeder.")
    else:
        print(f"    body: {tr.text[:200]}")
        print("\n❌ Even the user token was refused — the app is likely on a "
              "restricted access tier. We'll need to register a fresh app.")

    if refresh:
        print("\n" + "=" * 68)
        print("Set this in the Railway spotify service (and local .env):")
        print(f"  SPOTIFY_REFRESH_TOKEN={refresh}")
        print("=" * 68)


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("auth", "exchange"):
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] == "auth":
        auth_url()
    else:
        if len(sys.argv) < 3:
            sys.exit('Usage: spotify_oauth_setup.py exchange "<redirect URL or code>"')
        exchange(sys.argv[2])
