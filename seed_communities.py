"""
Community Seeder
----------------
Seeds the communities table with the initial ~300 subreddits
across all taxonomy types. Fetches live subscriber counts and
descriptions from Reddit's about.json endpoint for each subreddit.

Run once before the Reddit collector starts.
Safe to re-run — all inserts are ON CONFLICT DO NOTHING.

Usage:
    python seed_communities.py
    python seed_communities.py --dry-run     # print what would be inserted
    python seed_communities.py --verify      # check which subs are unreachable
"""

import os
import sys
import time
import logging
import argparse
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("seeder")

DB_URL = os.environ.get("DATABASE_URL", "")
REDDIT_USER_AGENT = os.environ.get(
    "REDDIT_USER_AGENT",
    "everywhere-chart-seeder/0.1 (contact: your@email.com)"
)

# ── Community definitions ────────────────────────────────────────────────────
#
# Each entry: (subreddit_name, community_type, casual_weight_override_or_None)
# casual_weight=None → use the type default
# Types and their default weight ranges:
#   artist        0.15
#   genre         0.40
#   general_music 0.60
#   entertainment 0.80
#   sports        0.85
#   lifestyle     0.88
#   non_music     0.92
#
# Weights can be overridden per-subreddit when the default doesn't fit
# (e.g. r/LetterboxD is entertainment but skews more casual than r/movies)

TYPE_DEFAULT_WEIGHTS = {
    "artist":        0.15,
    "genre":         0.40,
    "general_music": 0.60,
    "entertainment": 0.80,
    "sports":        0.85,
    "lifestyle":     0.88,
    "non_music":     0.92,
}

SEED_COMMUNITIES = [

    # ── GENRE (weight 0.30–0.50) ────────────────────────────────────────────
    ("hiphopheads",         "genre",         0.38),
    ("indieheads",          "genre",         0.38),
    ("popheads",            "genre",         0.42),
    ("rnb",                 "genre",         0.40),
    ("electronicmusic",     "genre",         0.40),
    ("Metal",               "genre",         0.35),
    ("country",             "genre",         0.38),
    ("LatinMusic",          "genre",         0.40),
    ("kpop",                "genre",         0.35),
    ("punk",                "genre",         0.38),
    ("Jazz",                "genre",         0.38),
    ("ClassicalMusic",      "genre",         0.35),
    ("trap",                "genre",         0.40),
    ("DJs",                 "genre",         0.42),
    ("EDM",                 "genre",         0.42),
    ("Blues",               "genre",         0.38),
    ("folk",                "genre",         0.38),
    ("AlternativeRock",     "genre",         0.40),
    ("reggae",              "genre",         0.40),
    ("afrobeats",           "genre",         0.40),
    ("shoegaze",            "genre",         0.35),
    ("PostRock",            "genre",         0.35),
    ("ambientmusic",        "genre",         0.35),
    ("rap",                 "genre",         0.42),
    ("brandnewband",        "genre",         0.35),  # emo/alt adjacent
    ("emo",                 "genre",         0.35),
    ("poppunkers",          "genre",         0.38),
    ("soulmusic",           "genre",         0.40),
    ("mexicanmusic",        "genre",         0.40),
    ("kpopthoughts",        "genre",         0.38),

    # ── GENERAL MUSIC (weight 0.55–0.65) ────────────────────────────────────
    ("Music",               "general_music", 0.58),
    ("listentothis",        "general_music", 0.62),
    ("MusicRecommendations","general_music", 0.65),
    ("ifyoulikeblank",      "general_music", 0.65),
    ("spotify",             "general_music", 0.62),
    ("AppleMusic",          "general_music", 0.62),
    ("lastfm",              "general_music", 0.58),
    ("Music_discovery",     "general_music", 0.65),
    ("SongwritingAndMusic", "general_music", 0.60),
    ("WeAreTheMusicMakers", "general_music", 0.55),
    ("weddingsongs",        "general_music", 0.80),  # higher — curated casual
    ("MusicInTheMaking",    "general_music", 0.58),
    ("musictheory",         "general_music", 0.50),
    ("vinyl",               "general_music", 0.55),
    ("recordcollecting",    "general_music", 0.55),
    ("playlists",           "general_music", 0.65),

    # ── ENTERTAINMENT (weight 0.75–0.85) ────────────────────────────────────
    ("movies",              "entertainment", 0.78),
    ("television",          "entertainment", 0.78),
    ("netflix",             "entertainment", 0.80),
    ("hulu",                "entertainment", 0.80),
    ("Letterboxd",          "entertainment", 0.82),
    ("Oscars",              "entertainment", 0.78),
    ("criterion",           "entertainment", 0.75),
    ("TrueOffMyChest",      "entertainment", 0.92),  # personal stories — higher
    ("reactiongifs",        "entertainment", 0.85),
    ("Showerthoughts",      "entertainment", 0.90),
    ("mildlyinteresting",   "entertainment", 0.88),
    ("interestingasfuck",   "entertainment", 0.88),
    ("nextfuckinglevel",    "entertainment", 0.88),
    ("gaming",              "entertainment", 0.80),
    ("pcgaming",            "entertainment", 0.80),
    ("videogames",          "entertainment", 0.80),
    ("anime",               "entertainment", 0.80),
    ("comicbooks",          "entertainment", 0.80),
    ("marvelstudios",       "entertainment", 0.80),
    ("DCcomics",            "entertainment", 0.80),
    ("horror",              "entertainment", 0.80),
    ("scifi",               "entertainment", 0.80),
    ("fantasy",             "entertainment", 0.80),
    ("bookclub",            "entertainment", 0.82),
    ("books",               "entertainment", 0.82),
    ("popculturechat",      "entertainment", 0.85),
    ("popculture",          "entertainment", 0.85),
    ("entertainment",       "entertainment", 0.82),
    ("tiktokcringe",        "entertainment", 0.90),  # high casual, viral signals
    ("PublicFreakout",      "entertainment", 0.88),

    # ── SPORTS (weight 0.80–0.90) ────────────────────────────────────────────
    ("nfl",                 "sports",        0.85),
    ("nba",                 "sports",        0.85),
    ("soccer",              "sports",        0.85),
    ("baseball",            "sports",        0.85),
    ("hockey",              "sports",        0.85),
    ("tennis",              "sports",        0.85),
    ("formula1",            "sports",        0.85),
    ("SquaredCircle",       "sports",        0.85),  # wrestling — high crossover
    ("running",             "sports",        0.88),
    ("Fitness",             "sports",        0.88),
    ("weightlifting",       "sports",        0.88),
    ("crossfit",            "sports",        0.88),
    ("cycling",             "sports",        0.87),
    ("golf",                "sports",        0.85),
    ("mma",                 "sports",        0.85),
    ("Boxing",              "sports",        0.85),
    ("CFB",                 "sports",        0.83),  # college football
    ("CollegeBasketball",   "sports",        0.83),
    ("sports",              "sports",        0.85),
    ("nflstreams",          "sports",        0.84),
    ("olympics",            "sports",        0.86),
    ("swimming",            "sports",        0.87),
    ("yoga",                "sports",        0.90),  # very lifestyle-adjacent
    ("Rowing",              "sports",        0.87),
    ("triathlon",           "sports",        0.87),

    # ── LIFESTYLE (weight 0.85–0.92) ─────────────────────────────────────────
    ("MealPrepSunday",      "lifestyle",     0.95),
    ("weddingplanning",     "lifestyle",     0.95),
    ("dating_advice",       "lifestyle",     0.93),
    ("AskWomen",            "lifestyle",     0.90),
    ("AskMen",              "lifestyle",     0.90),
    ("femalefashionadvice", "lifestyle",     0.90),
    ("malefashionadvice",   "lifestyle",     0.88),
    ("Cooking",             "lifestyle",     0.92),
    ("food",                "lifestyle",     0.90),
    ("roadtrip",            "lifestyle",     0.93),
    ("camping",             "lifestyle",     0.92),
    ("travel",              "lifestyle",     0.90),
    ("solotravel",          "lifestyle",     0.91),
    ("hiking",              "lifestyle",     0.92),
    ("relationship_advice", "lifestyle",     0.93),
    ("teenagers",           "lifestyle",     0.92),
    ("teens",               "lifestyle",     0.92),
    ("college",             "lifestyle",     0.90),
    ("productivity",        "lifestyle",     0.88),
    ("GetMotivated",        "lifestyle",     0.90),
    ("selfimprovement",     "lifestyle",     0.88),
    ("Parenting",           "lifestyle",     0.90),
    ("BabyBumps",           "lifestyle",     0.92),
    ("tattoos",             "lifestyle",     0.90),
    ("datingoverthirty",    "lifestyle",     0.93),
    ("AskWomenOver30",      "lifestyle",     0.92),
    ("xxfitness",           "lifestyle",     0.90),
    ("loseit",              "lifestyle",     0.90),
    ("progresspics",        "lifestyle",     0.90),
    ("weddingplans",        "lifestyle",     0.94),
    ("Baking",              "lifestyle",     0.92),
    ("cocktails",           "lifestyle",     0.90),
    ("bartenders",          "lifestyle",     0.88),
    ("nightlife",           "lifestyle",     0.90),
    ("party",               "lifestyle",     0.92),
    ("homeimprovement",     "lifestyle",     0.88),
    ("InteriorDesign",      "lifestyle",     0.88),
    ("gardening",           "lifestyle",     0.90),

    # ── NON-MUSIC / GENERAL (weight 0.88–0.96) ───────────────────────────────
    # Top ~100 subreddits by activity — filtered for music signal potential
    ("AskReddit",           "non_music",     0.90),
    ("funny",               "non_music",     0.92),
    ("memes",               "non_music",     0.92),
    ("todayilearned",       "non_music",     0.92),
    ("worldnews",           "non_music",     0.88),
    ("news",                "non_music",     0.88),
    ("science",             "non_music",     0.88),
    ("technology",          "non_music",     0.86),
    ("nottheonion",         "non_music",     0.92),
    ("tifu",                "non_music",     0.93),
    ("confessions",         "non_music",     0.93),
    ("AmItheAsshole",       "non_music",     0.92),
    ("unpopularopinion",    "non_music",     0.90),
    ("changemyview",        "non_music",     0.88),
    ("AskScience",          "non_music",     0.88),
    ("ELI5",                "non_music",     0.90),
    ("LifeAdvice",          "non_music",     0.92),
    ("NoStupidQuestions",   "non_music",     0.90),
    ("TrueAskReddit",       "non_music",     0.92),
    ("mildlyinfuriating",   "non_music",     0.90),
    ("Unexpected",          "non_music",     0.92),
    ("HumansBeingBros",     "non_music",     0.93),
    ("MadeMeSmile",         "non_music",     0.93),
    ("aww",                 "non_music",     0.90),
    ("gifs",                "non_music",     0.90),
    ("videos",              "non_music",     0.90),
    ("Damnthatsinteresting","non_music",     0.92),
    ("WTF",                 "non_music",     0.90),
    ("OldSchoolCool",       "non_music",     0.90),
    ("nostalgia",           "non_music",     0.92),
    ("90s",                 "non_music",     0.92),
    ("2000s",               "non_music",     0.92),
    ("GenZ",                "non_music",     0.93),
    ("Millennials",         "non_music",     0.92),
    ("BlackPeopleTwitter",  "non_music",     0.93),
    ("facepalm",            "non_music",     0.90),
    ("trashy",              "non_music",     0.88),
    ("cringe",              "non_music",     0.90),
    ("CasualConversation",  "non_music",     0.92),
    ("socialskills",        "non_music",     0.90),
    ("Anxiety",             "non_music",     0.93),
    ("depression",          "non_music",     0.93),
    ("mentalhealth",        "non_music",     0.93),
    ("BreakUps",            "non_music",     0.95),
    ("grief",               "non_music",     0.95),
    ("ExNoContact",         "non_music",     0.95),
    ("GriefSupport",        "non_music",     0.95),
    ("survivorsofabuse",    "non_music",     0.94),
    ("therapy",             "non_music",     0.93),
    ("offmychest",          "non_music",     0.94),
    ("vent",                "non_music",     0.94),
    ("Meditation",          "non_music",     0.92),
    ("spirituality",        "non_music",     0.92),
    ("socialanxiety",       "non_music",     0.93),
    ("lonely",              "non_music",     0.94),
    ("ForeverAlone",        "non_music",     0.93),
]

# ── Reddit metadata fetch ─────────────────────────────────────────────────────

def fetch_subreddit_meta(subreddit: str) -> dict:
    """
    Fetch subreddit about.json for subscriber count and description.
    Returns {} if subreddit doesn't exist or is private.
    """
    try:
        resp = requests.get(
            f"https://www.reddit.com/r/{subreddit}/about.json",
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "subscribers":   data.get("subscribers", 0),
                "description":   (data.get("public_description") or
                                  data.get("description", ""))[:500],
                "display_name":  data.get("display_name_prefixed", f"r/{subreddit}"),
                "url":           f"https://reddit.com/r/{subreddit}",
            }
        elif resp.status_code == 404:
            log.warning(f"r/{subreddit} — not found (404)")
        elif resp.status_code == 403:
            log.warning(f"r/{subreddit} — private or banned (403)")
        else:
            log.warning(f"r/{subreddit} — HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"r/{subreddit} — request failed: {e}")
    return {}

# ── Database operations ───────────────────────────────────────────────────────

def seed_community(cur, subreddit: str, community_type: str,
                   casual_weight: float, meta: dict, dry_run: bool) -> bool:
    """
    Insert community into DB. Returns True if inserted, False if already existed.
    """
    if dry_run:
        subs = meta.get("subscribers", "unknown")
        log.info(f"[DRY RUN] Would insert r/{subreddit} "
                 f"({community_type}, weight={casual_weight}, subs={subs})")
        return True

    cur.execute("""
        INSERT INTO communities (
            platform, external_id, display_name, description, url,
            community_type, casual_weight, weight_source,
            subscriber_count, classified_at, auto_discovered
        )
        VALUES (
            'reddit', %s, %s, %s, %s,
            %s, %s, 'manual',
            %s, NOW(), FALSE
        )
        ON CONFLICT (platform, external_id) DO UPDATE
            SET subscriber_count = EXCLUDED.subscriber_count,
                description      = EXCLUDED.description,
                classified_at    = NOW()
        RETURNING (xmax = 0) AS inserted   -- TRUE if new row, FALSE if updated
    """, (
        subreddit.lower(),
        meta.get("display_name", f"r/{subreddit}"),
        meta.get("description", ""),
        meta.get("url", f"https://reddit.com/r/{subreddit}"),
        community_type,
        casual_weight,
        meta.get("subscribers"),
    ))
    row = cur.fetchone()
    return row["inserted"] if row else False

# ── Taxonomy distance seeding ─────────────────────────────────────────────────

TAXONOMY_DISTANCES = [
    # (type_a, type_b, distance)
    # Same type → stored as-needed, not pre-seeded (distance=0.0 or type default)
    ("artist",        "genre",         0.20),
    ("artist",        "general_music", 0.35),
    ("artist",        "entertainment", 0.65),
    ("artist",        "sports",        0.80),
    ("artist",        "lifestyle",     0.85),
    ("artist",        "non_music",     0.92),
    ("genre",         "general_music", 0.25),
    ("genre",         "entertainment", 0.55),
    ("genre",         "sports",        0.75),
    ("genre",         "lifestyle",     0.80),
    ("genre",         "non_music",     0.90),
    ("general_music", "entertainment", 0.50),
    ("general_music", "sports",        0.70),
    ("general_music", "lifestyle",     0.75),
    ("general_music", "non_music",     0.88),
    ("entertainment", "sports",        0.55),
    ("entertainment", "lifestyle",     0.60),
    ("entertainment", "non_music",     0.80),
    ("sports",        "lifestyle",     0.50),
    ("sports",        "non_music",     0.75),
    ("lifestyle",     "non_music",     0.65),
]

def seed_type_distances(cur, dry_run: bool):
    """
    Pre-seed community_distances with taxonomy-rule distances between
    representative communities of each type pair.
    Real behavioral distances will overwrite these as data accumulates.
    """
    if dry_run:
        log.info(f"[DRY RUN] Would seed {len(TAXONOMY_DISTANCES)} type-pair distances")
        return

    # Get one representative community per type
    cur.execute("""
        SELECT DISTINCT ON (community_type)
            id, community_type
        FROM communities
        WHERE platform = 'reddit'
        ORDER BY community_type, subscriber_count DESC NULLS LAST
    """)
    reps = {row["community_type"]: row["id"] for row in cur.fetchall()}

    inserted = 0
    for type_a, type_b, distance in TAXONOMY_DISTANCES:
        id_a = reps.get(type_a)
        id_b = reps.get(type_b)
        if not id_a or not id_b:
            continue

        # ensure a < b for undirected constraint
        pair = tuple(sorted([str(id_a), str(id_b)]))
        cur.execute("""
            INSERT INTO community_distances (community_a_id, community_b_id, distance, distance_method)
            VALUES (%s, %s, %s, 'taxonomy_rule')
            ON CONFLICT (community_a_id, community_b_id) DO NOTHING
        """, (pair[0], pair[1], distance))
        inserted += 1

    log.info(f"Seeded {inserted} taxonomy-rule distances")

# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, verify: bool = False):
    if not dry_run and not DB_URL:
        log.error("DATABASE_URL not set")
        sys.exit(1)

    conn = None if dry_run else psycopg2.connect(DB_URL)
    if conn:
        conn.autocommit = False
        psycopg2.extras.register_uuid()

    total      = len(SEED_COMMUNITIES)
    inserted   = 0
    updated    = 0
    unreachable= 0

    log.info(f"Seeding {total} communities — dry_run={dry_run}")

    for i, (subreddit, community_type, weight_override) in enumerate(SEED_COMMUNITIES, 1):
        casual_weight = weight_override if weight_override is not None \
                        else TYPE_DEFAULT_WEIGHTS[community_type]

        # fetch live metadata from Reddit
        meta = fetch_subreddit_meta(subreddit)
        if not meta:
            unreachable += 1
            if verify:
                log.warning(f"UNREACHABLE: r/{subreddit}")
            if not dry_run:
                # still insert with no metadata — better to have it in the table
                meta = {}

        if not dry_run and conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                is_new = seed_community(cur, subreddit, community_type,
                                        casual_weight, meta, dry_run=False)
                if is_new:
                    inserted += 1
                else:
                    updated += 1
            conn.commit()
        else:
            seed_community(None, subreddit, community_type,
                           casual_weight, meta, dry_run=True)
            inserted += 1

        # progress
        if i % 25 == 0:
            log.info(f"Progress: {i}/{total}")

        # polite rate limiting — Reddit asks for ~1 req/sec for unauthenticated
        time.sleep(1.1)

    # seed taxonomy distances after communities are inserted
    if not dry_run and conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            seed_type_distances(cur, dry_run=False)
        conn.commit()
        conn.close()
    elif dry_run:
        seed_type_distances(None, dry_run=True)

    log.info(
        f"\nSeeding complete:\n"
        f"  {inserted} new communities inserted\n"
        f"  {updated} existing communities updated\n"
        f"  {unreachable} subreddits unreachable (private/deleted)\n"
        f"  Total: {total}"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed communities table")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print what would be inserted without writing to DB")
    parser.add_argument("--verify",   action="store_true",
                        help="Flag unreachable subreddits in output")
    args = parser.parse_args()
    run(dry_run=args.dry_run, verify=args.verify)
