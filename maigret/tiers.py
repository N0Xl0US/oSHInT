"""
tiers.py — Platform signal classification constants.

Separated from ppf.py for maintainability.  Every set and mapping used by
the PPF scoring engine lives here.
"""

from __future__ import annotations

# ── Platform tier sets ───────────────────────────────────────────────────────
# HIGH   = strong identity signal (code, professional, creator)
# MEDIUM = social/hobby — real but noisy, username collision likely
# LOW    = dead/high-noise platforms
# DROP   = API endpoints masquerading as profiles — never include

TIER_HIGH: frozenset[str] = frozenset({
    "github", "githubgist", "gitlab", "gitee", "bitbucket", "docker", "kaggle",
    "hackernews", "dev", "devto",
    "youtube", "twitch", "linkedin",
    "producthunt", "speakerdeck", "hackster",
    "hackernoon", "codeforces", "trello",
})

TIER_MEDIUM: frozenset[str] = frozenset({
    "chess", "lichess", "duolingo",
    "dribbble", "behance", "codepen", "codewars", "codecademy",
    "patreon", "kofi", "bandcamp", "soundcloud", "spotify",
    "myinstants", "freesound",
    "flickr", "500px", "deviantart", "artstation", "picsart",
    "reddit", "twitter", "instagram", "mastodon",
    "discord", "slack",
    "wordpress", "wordpressorg", "blogger", "weebly", "tilda",
    "medium", "substack", "note", "genius", "kinja",
    "flipboard", "bibsonomy",
    "goodreads", "librarything",
    "steam", "gog", "osu!", "itch", "kongregate", "pokemon",
    "ebay", "etsy", "mercadolivre",
    "academia", "researchgate",
    "scribd", "authorstream",
    "myminifactory", "thingiverse",
    "habrcareer", "habr",
    "pastebin",
    "beacons", "linktree", "lnk.bio",
    "moddb", "3ddd",
    "flightradar24",
    "untappd",
    "plurk",
    "telegram",
    "giphy", "imgur", "imageshack",
    "opensea", "cent", "steemit",
    "ariva",
    "twitchtracker",
    "kaskus", "filmweb", "otzovik", "irecommend",
    "buzzfeed",
    "codecanyon", "themeforest", "videohive", "audiojungle",
    "warrior", "forums",
    "disqus",
    "advego",
    "last.fm", "reverbnation", "smule",
    "wattpad", "livejournal", "myanimelist", "redbubble",
    "discogs",
    "hashnode", "slides", "teletype",
    "instructables", "wikidot",
    "geocaching",
    "kwork", "truelancer",
    "coub", "pikabu", "rutracker",
    "donationalerts",
    "blu-ray", "techpowerup",
    "vgtimes", "jeuxvideo",
    "about.me",
    "zomato", "venmo", "ifttt", "mix",
    "drive2",
    "archiveofourown",
})

TIER_DROP: frozenset[str] = frozenset({
    "freelancer.com",
    "diigo",
    "pr0gramm",
    "op.gg",
    "npm-package",
})

TIER_LOW: frozenset[str] = frozenset({
    "couchsurfing", "hi5", "myspace", "ebaumsworld",
    "voices", "weedmaps", "getmyuni", "livemaster",
    "jigsawplanet", "sporcle", "thesimsresource", "itemfix",
    "moscowflamp", "seoclerks", "hubpages", "smart-lab",
    "skyscrapercity",
})

ADULT_DATING_PLATFORMS: frozenset[str] = frozenset({
    "chaturbate", "eporner", "xvideos", "adultfriendfinder",
    "pornhub", "xhamster", "onlyfans", "livejasmin",
    "tinder", "badoo", "okcupid", "interpals",
})

TRUSTED_MEDIUM_PLATFORMS: frozenset[str] = frozenset({
    "reddit", "twitter", "instagram", "mastodon", "telegram",
    "dribbble", "behance", "codepen", "codewars", "codecademy",
    "soundcloud", "spotify", "flickr", "deviantart", "artstation",
    "wordpress", "wordpressorg", "medium", "substack",
    "researchgate", "academia", "pastebin", "hashnode",
    "imgur", "giphy", "kofi",
})

HIGH_COLLISION_MEDIUM_PLATFORMS: frozenset[str] = frozenset({
    "steam", "patreon", "untappd", "forums", "forum", "plurk",
    "myanimelist", "wattpad", "myspace", "wikidot", "otzovik",
    "filmweb", "kaskus", "buzzfeed", "codecanyon", "themeforest",
    "videohive", "audiojungle", "warrior", "disqus",
})

# Map multi-URL variants → canonical bucket key for deduplication
MULTI_URL_PLATFORMS: dict[str, str] = {
    "steam":      "steam",
    "github":     "github",
    "githubgist": "github",
}

MULTI_REGION_PLATFORMS: frozenset[str] = frozenset({"op.gg"})


# ── URL classification patterns ──────────────────────────────────────────────

API_URL_PATTERNS: tuple[str, ...] = (
    "/api/",
    "?usernames%5B%5D=",
    "/interact_api/",
    "/api/profile/",
    "/users/filter?search=",
    "members/?username=",
)

WORKSPACE_URL_PATTERNS: tuple[str, ...] = (
    ".slack.com",
)

SEARCH_PAGE_PATTERNS: tuple[str, ...] = (
    "/summoners/search",
    "/users/filter",
    "members/?username=",
    "/forum/profile.php?mode=viewprofile",
)

STRICT_URL_BLACKLIST: tuple[str, ...] = (
    "search?q=",
    "api/",
    "members/?username=",
    "filter",
    "/groups/",
    "{username}",
    "%7busername%7d",
)


# ── Confidence scoring weights ───────────────────────────────────────────────

TIER_SCORE: dict[str, float] = {
    "high":   0.6,
    "medium": 0.3,
    "low":    0.1,
    "drop":   0.0,
}

HTTP_SCORE: dict[int, float] = {
    200: 0.4,
    301: 0.2,
    302: 0.2,
    403: 0.1,
    404: 0.0,
    0:   0.0,
}
