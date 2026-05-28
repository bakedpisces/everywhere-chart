"""
Label classification utilities
-------------------------------
Classifies a Spotify label string into one of four tiers:
  'major'    — Big Three (UMG, Sony, Warner) and their direct subsidiaries
  'indie'    — Identifiable independent label
  'unsigned' — Self-released / distributor-as-label (DistroKid, TuneCore, etc.)
  'unknown'  — Label string absent or unrecognised

Usage:
    from collectors.label_utils import classify_label_tier, fetch_album_label
"""

import re
import time
import logging
import requests
from typing import Optional

log = logging.getLogger(__name__)

# ── Major label patterns ───────────────────────────────────────────────────────
# Covers the Big Three and their primary imprints / subsidiaries.
# Intentionally excludes distribution arms (The Orchard, Ingrooves) that serve
# indie artists — those flow to 'indie'.

_MAJOR = re.compile(
    r"\b("
    # Universal Music Group
    r"universal music|umg|republic records|interscope|def jam|capitol records|"
    r"island records|polydor|geffen|mercury records|motown|cash money|young money|"
    r"aftermath|shady records|virgin music|virgin emi|emi records|harvest records|"
    r"decca|blue note|verve records|spinefarm|lava records|cherrytree|"
    r"fontana north|19 recordings|"
    # Sony Music Entertainment
    r"sony music|columbia records|rca records|epic records|arista|jive records|"
    r"zomba|legacy recordings|kemosabe|masterworks|provident music|"
    r"reunion records|so so def|polo ground|awal|"
    # Warner Music Group
    r"warner records|warner bros records|atlantic records|elektra|parlophone|"
    r"reprise records|nonesuch|rhino|fueled by ramen|big beat|roadrunner|"
    r"spinnin records|east west|loma vista|canvasback|sire records|asylum records|"
    r"300 entertainment|elektra records"
    r")\b",
    re.IGNORECASE,
)

# ── Unsigned / self-release distributor patterns ───────────────────────────────
# These are distribution platforms artists use when they have NO label deal.
# Presence of these names as the "label" means effectively unsigned.

_UNSIGNED = re.compile(
    r"\b("
    r"distrokid|tunecore|cd baby|cdbaby|amuse|stem disintermedia|"
    r"united masters|unitedmasters|too lost|onerock|ditto music|landr|"
    r"songtradr|soundrop|loudr|reverbnation|bandcamp|"
    r"self.?released|self.?published|independent release|not on label"
    r")\b",
    re.IGNORECASE,
)


def classify_label_tier(label: Optional[str]) -> str:
    """
    Classify a label string into 'major', 'indie', 'unsigned', or 'unknown'.

    Rules (in order):
      1. Null / empty  → 'unknown'
      2. Matches major → 'major'
      3. Matches unsigned distributor → 'unsigned'
      4. Anything else → 'indie'  (named label, just not a major)
    """
    if not label or not label.strip():
        return "unknown"
    if _MAJOR.search(label):
        return "major"
    if _UNSIGNED.search(label):
        return "unsigned"
    return "indie"


def fetch_album_label(album_id: str, token: str) -> Optional[str]:
    """
    Fetch the label string for a Spotify album.
    Returns None on any error or if label field is absent.
    """
    if not album_id or not token:
        return None
    try:
        resp = requests.get(
            f"https://api.spotify.com/v1/albums/{album_id}",
            headers={"Authorization": f"Bearer {token}"},
            params={"fields": "label"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("label") or None
        log.debug(f"Album label fetch returned {resp.status_code} for {album_id}")
    except Exception as e:
        log.debug(f"Album label fetch failed for {album_id}: {e}")
    return None
