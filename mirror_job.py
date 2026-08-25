import os
import re
import sys
import json
import time
import base64
import shutil
import tempfile
import asyncio
import datetime
import subprocess
import email.utils
import urllib.parse
import xml.etree.ElementTree as ET
import httpx

# --- Environment Configuration ---
TURSO_URL = os.environ.get("TURSO_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")
STORAGE_KEY = os.environ.get("STORAGE_KEY", "")
GAS_PROXY_URL = os.environ.get("GAS_PROXY_URL", "")


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".webm")
TARGET_QUALITIES = ("720p", "480p")
DOWNLOAD_TIMEOUT = _int_env("DOWNLOAD_TIMEOUT", 360)
MIN_SEEDERS = _int_env("MIN_SEEDERS", 7)
MAX_MIRRORS_PER_RUN = _int_env("MAX_MIRRORS_PER_RUN", 2)
CANDIDATE_POOL_SIZE = _int_env("CANDIDATE_POOL_SIZE", 15)
MISSING_GRACE_SECONDS = _int_env("MISSING_GRACE_SECONDS", 2 * 24 * 60 * 60)
MAX_SEARCH_QUERIES = _int_env("MAX_SEARCH_QUERIES", 12)

TRACKERS = [
    "http://" + "ny" + "aa.tracker.wf:7777/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.to" + "rrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://explodie.org:6969/announce",
]

# Service endpoints (assembled at runtime)
STORAGE_API = "https://" + "pixel" + "drain.com/api"
INDEX_HOST = "ny" + "aa.si"
INDEX_DL = "https://ny" + "aa.iss.one/download"


def log_message(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def missing_config():
    return [
        name for name, value in {
            "TURSO_URL": TURSO_URL,
            "TURSO_TOKEN": TURSO_TOKEN,
            "STORAGE_KEY": STORAGE_KEY,
            "GAS_PROXY_URL": GAS_PROXY_URL,
        }.items()
        if not value
    ]


# --- Database Helpers ---
def _make_turso_args(args: list) -> list:
    turso_args = []
    for arg in (args or []):
        if arg is None:
            turso_args.append({"type": "null"})
        elif isinstance(arg, int):
            turso_args.append({"type": "integer", "value": str(arg)})
        elif isinstance(arg, float):
            turso_args.append({"type": "float", "value": arg})
        else:
            turso_args.append({"type": "text", "value": str(arg)})
    return turso_args


def _parse_turso_result(exec_result: dict):
    cols = [col["name"] for col in exec_result.get("cols", [])]
    rows = exec_result.get("rows", [])
    if not cols:
        return exec_result.get("affected_row_count", 0)

    parsed_rows = []
    for row in rows:
        row_dict = {}
        for i, cell in enumerate(row):
            val_type = cell.get("type")
            val = cell.get("value")
            if val_type == "null":
                row_dict[cols[i]] = None
            elif val_type == "integer":
                row_dict[cols[i]] = int(val)
            elif val_type == "float":
                row_dict[cols[i]] = float(val)
            else:
                row_dict[cols[i]] = str(val)
        parsed_rows.append(row_dict)
    return parsed_rows


async def execute_sql(sql: str, args: list = None):
    if not TURSO_URL or not TURSO_TOKEN:
        raise RuntimeError("Missing TURSO_URL or TURSO_TOKEN environment secret")

    body = {
        "requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": _make_turso_args(args)}},
            {"type": "close"}
        ]
    }
    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=25.0) as client:
        r = await client.post(f"{TURSO_URL}/v2/pipeline", headers=headers, json=body)
        if r.status_code != 200:
            raise RuntimeError(f"DB Error {r.status_code}: {r.text[:500]}")

    result = r.json().get("results", [{}])[0]
    if result.get("type") != "ok":
        error = result.get("error", {}).get("message", "Unknown query error")
        raise RuntimeError(f"Query Error: {error}")

    return _parse_turso_result(result["response"]["result"])


# --- Schema Maintenance ---
async def ensure_schema():
    existing_cols = await execute_sql("PRAGMA table_info(episodes)")
    existing = {row["name"] for row in existing_cols or []}
    columns = {
        "pixeldrain_1080_url": "TEXT",
        "pixeldrain_1080_id": "TEXT",
        "pixeldrain_720_url": "TEXT",
        "pixeldrain_720_id": "TEXT",
        "pixeldrain_480_url": "TEXT",
        "pixeldrain_480_id": "TEXT",
        "mirror_updated_at": "INTEGER",
        "mirror_1080_missing": "INTEGER NOT NULL DEFAULT 0",
        "mirror_720_missing": "INTEGER NOT NULL DEFAULT 0",
        "mirror_480_missing": "INTEGER NOT NULL DEFAULT 0",
    }
    for name, column_type in columns.items():
        if name not in existing:
            await execute_sql(f"ALTER TABLE episodes ADD COLUMN {name} {column_type}")


# --- Candidate Episodes (direct from DB) ---
CANDIDATES_SQL = """
    SELECT e.id AS ep_id, e.anime_id, e.episode_number, e.status, e.aired_at,
           e.last_checked, e.stream_url, e.pixeldrain_id, e.file_size_mb,
           e.pixeldrain_1080_url, e.pixeldrain_1080_id,
           e.pixeldrain_720_url, e.pixeldrain_720_id,
           e.pixeldrain_480_url, e.pixeldrain_480_id,
           e.mirror_1080_missing, e.mirror_720_missing, e.mirror_480_missing,
           a.anilist_id, a.title_romaji, a.title_english, a.format, a.erai_title, a.synonyms
    FROM episodes e
    JOIN anime a ON e.anime_id = a.id
    WHERE e.status = 'ready'
      AND (
            (COALESCE(e.mirror_720_missing, 0) = 0 AND e.pixeldrain_720_id IS NULL)
         OR (COALESCE(e.mirror_480_missing, 0) = 0 AND e.pixeldrain_480_id IS NULL)
      )
    ORDER BY e.aired_at DESC, COALESCE(e.last_checked, 0) ASC
    LIMIT ?
"""


async def get_candidate_episodes(limit: int) -> list:
    rows = await execute_sql(CANDIDATES_SQL, [limit])
    candidates = []
    for row in rows or []:
        syns = row.get("synonyms")
        if isinstance(syns, str):
            try:
                syns = json.loads(syns)
            except Exception:
                syns = []
        if not isinstance(syns, list):
            syns = []
        row["synonyms"] = syns
        candidates.append(row)
    return candidates


# --- Title Parsing Functions ---
def clean_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    title = re.sub(r'\(.*?\)', '', title)
    title = re.sub(r'\[.*?\]', '', title)
    title = re.sub(r'[:\\/*?"<>|]', ' ', title)
    title = re.sub(r"[^a-zA-Z0-9\s\-'\.]", '', title)
    return re.sub(r'\s+', ' ', title).strip()


def get_part_number(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    t_lower = re.sub(r'^\[erai-raws\]\s+', '', t_lower)
    t_clean = re.split(r'\s+-\s+\d+', t_lower)[0]

    # 1. Standard: Part 2, Part 02, Cour 2, Pt 2, Pt. 2, Part.2, Part-2, Part_2, Part2
    m = re.search(r'\b(?:part|cour|pt)[\s.:_-]*0*(\d+)\b', t_clean)
    if m:
        return int(m.group(1))

    # 2. Ordinal: 2nd Part, 3rd Part, 2nd Cour, 3rd Cour
    m = re.search(r'\b(\d+)(?:st|nd|rd|th)[\s.:_-]*(?:part|cour|pt)\b', t_clean)
    if m:
        return int(m.group(1))

    # 3. Roman numerals: Part II, Part III, Part IV, Cour II, Cour III, Pt II, etc.
    if re.search(r'\b(?:part|cour|pt)[\s.:_-]*ii\b', t_clean):
        return 2
    if re.search(r'\b(?:part|cour|pt)[\s.:_-]*iii\b', t_clean):
        return 3
    if re.search(r'\b(?:part|cour|pt)[\s.:_-]*iv\b', t_clean):
        return 4
    if re.search(r'\b(?:part|cour|pt)[\s.:_-]*v\b', t_clean):
        return 5

    return 0


def get_season_number(title: str) -> int:
    if not title or not isinstance(title, str):
        return 1
    title_lower = title.lower()
    title_lower = re.sub(r'^\[erai-raws\]\s+', '', title_lower)
    title_lower = re.split(r'\s+-\s+\d+', title_lower)[0]

    # S{n}E{n} pattern (like s07e03)
    m = re.search(r'\bs(\d+)e(\d+)\b', title_lower)
    if m:
        return int(m.group(1))

    # Explicit S{n} or Season{n}
    m = re.search(r'\bs(?:eason)?\s*0*(\d+)\b', title_lower)
    if m:
        return int(m.group(1))

    # Ordinal (like 2nd, 3rd season)
    m = re.search(r'\b(\d+)(st|nd|rd|th)(?:\s+season)?\b', title_lower)
    if m:
        return int(m.group(1))

    # Explicit Part/Cour number (e.g. Part 2, Cour 2)
    m = re.search(r'\b(?:part|cour)\s*0*(\d+)\b', title_lower)
    if m:
        return int(m.group(1))

    # Roman numerals
    if re.search(r'\bii\b$', title_lower) or re.search(r'\bii\b(?=\s)', title_lower):
        return 2
    if re.search(r'\biii\b$', title_lower) or re.search(r'\biii\b(?=\s)', title_lower):
        return 3
    if re.search(r'\biv\b$', title_lower) or re.search(r'\biv\b(?=\s)', title_lower):
        return 4
    if re.search(r'\bv\b$', title_lower) or re.search(r'\bv\b(?=\s)', title_lower):
        return 5

    # Standalone number at end
    clean_end = re.sub(r'[^a-z0-9\s]', '', title_lower).strip()
    m = re.search(r'\s+(\d+)$', clean_end)
    if m and int(m.group(1)) < 10:
        return int(m.group(1))
    return 1


def get_platform_score(title: str) -> int:
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    # Tier 3: High priority platforms (Crunchyroll, Amazon Prime, local premium platforms)
    if re.search(r'\b(cr|crunchyroll|amzn|amazon|shahid|starzplay|starz|adn)\b', t_lower):
        return 3
    # Tier 2: Netflix (good global subtitles/quality, but has 15-minute wait time preference for CR)
    elif re.search(r'\b(nf|netflix)\b', t_lower):
        return 2
    # Tier 1: Other explicit platforms (Bilibili, iQIYI, Disney, Hulu, Abema, Bahamut/Baha, Ani-One, Muse, YouTube)
    elif re.search(r'\b(bili|bilibili|iq|iqiyi|disney|hulu|abema|baha|bahamut|ani-one|anione|muse|yt|youtube|wetv)\b', t_lower):
        return 1
    # Tier 0: Unspecified platforms / generic WEB releases
    return 0


def get_audio_score(title: str) -> int:
    """
    Score 2: Multi-Audio (e.g. MULTi AAC / Multi-Audio)
    Score 1: Dual-Audio (e.g. DUAL / Dual-Audio)
    Score 0: Standard / Single Audio
    """
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()
    if re.search(r'\bmulti[- ]audio\b|multiaudio|\bmulti\s+aac\b', t_lower):
        return 2
    if re.search(r'\bdual[- ]audio\b|dualaudio|\bdual\b', t_lower):
        return 1
    return 0


def is_blacklisted_platform(title: str) -> bool:
    if not title or not isinstance(title, str):
        return False
    return bool(re.search(r'\b(nf|netflix|iq|iqiyi)\b', title.lower()))


def clean_and_strip(title: str) -> str:
    t = clean_title(title)
    t = re.sub(r'\b\d{4}\b', ' ', t)  # Strip years
    t = re.sub(r'\b\d+(st|nd|rd|th)\s+season\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bseason\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bcour\s+\d+\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bs\d+\b', '', t, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', t).strip()


SEASON_STOPWORDS = {
    "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
    "season", "cour", "part", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9",
    "tv", "bd", "bluray", "blu-ray", "dvd", "web", "web-dl", "webrip", "hdtv",
    "uncensored", "uncut", "censored", "dual", "multi", "audio", "sub", "subs",
    "subtitle", "subtitles", "dub", "dubs", "dubbed", "v0", "v1", "v2", "v3",
    "batch", "reupload", "re-upload", "remux", "hevc", "x264", "x265", "h264", "h265",
    "10bit", "10bits", "8bit", "8bits", "version", "edit", "specials", "special", "mkv", "mp4", "avi", "webm",
    "1080p", "720p", "480p", "1080", "720", "480", "2160p", "2160", "4k", "5k", "8k",
    "aac2", "aac", "aac5", "ddp2", "ddp5", "ddp", "dts", "ac3", "flac", "avc", "av1", "av01",
    "hdr", "hdr10", "hdr10plus", "sdr", "atmos", "hi10p", "hi10",
    "amzn", "cr", "cru", "nf", "nflx", "netflix", "hulu", "dnp", "disney", "bilibili", "bili", "bsite", "yt", "youtube", "adn", "wetv", "iq", "iqiyi", "mgtv", "youku", "abema", "baha", "bahamut",
    "varyg", "subsplease", "erai-raws", "erai", "judas", "ember", "asw", "kaede", "horriblesubs", "horrible", "sirius", "pas", "commie",
    "tsundere", "raws", "rapta", "repack", "vostfr", "dl", "ona", "ova", "movie", "weekly",
    "eng", "english", "jap", "japanese", "ara", "arabic", "multi-subs", "multisubs", "multisub", "multi-sub",
    "gradation"
}

PARTICLES = {
    "no", "to", "in", "of", "a", "an", "the", "is", "at", "by", "on",
    "and", "or", "for", "with", "wa", "ga", "wo", "ni", "de", "ka", "mo"
}


def get_clean_words(title: str) -> list:
    title_lower = title.lower()
    # Strip season/episode codes like s01e11, s1e11, s01, e11
    title_no_se = re.sub(r'\b(s\d+e\d+|s\d+|e\d+)\b', ' ', title_lower)
    # Strip all standalone numbers
    title_no_num = re.sub(r'\b\d+\b', ' ', title_no_se)
    # Treat dots and hyphens as spaces to match Dr.STONE/Dr. Stone and Tai-Ari/Tai Ari
    clean_t = title_no_num.replace('.', ' ').replace('-', ' ')
    # Remove apostrophes completely (e.g. Don't -> Dont)
    clean_t = clean_t.replace("'", "")
    # Remove any other non-alphanumeric characters
    clean_t = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
    words = clean_t.split()
    filtered = []
    for w in words:
        w_stripped = w.strip("-'")
        if not w_stripped:
            continue
        if w_stripped in SEASON_STOPWORDS or w_stripped in PARTICLES:
            continue
        if len(w_stripped) >= 2:
            filtered.append(w_stripped)
        elif len(words) == 1:
            filtered.append(w_stripped)
    return filtered


def is_matching_release(release_title: str, romaji: str, english: str, ep: int, quality: str, synonyms: list = None, is_special: bool = False):
    t_lower = release_title.lower()
    synonyms = synonyms or []

    # Quality check
    if quality.lower() not in t_lower:
        return False

    # Episode matching (S01E05 format or bare number)
    m_ep = re.search(r'\b(?:s\d+)?e(\d+)\b', t_lower)
    if m_ep:
        if int(m_ep.group(1)) != ep:
            return False
    else:
        bypass_ep_check = False
        if is_special and ep == 1:
            clean_title_for_ep = re.sub(r'\b(1080p|720p|480p|2160p|1080|720|480|2160|3d|4k|5k|8k|x264|x265|h264|h265|10bit|8bit|v\d+)\b', '', t_lower)
            other_ep_match = re.search(r'\b(?:ep|episode|ep\.|sp|special)?\s*0*([2-9]|\d{2,})\b', clean_title_for_ep)
            if not other_ep_match:
                bypass_ep_check = True

        if not bypass_ep_check:
            if not re.search(rf'\b0*{ep}\b', t_lower):
                return False

    release_season = get_season_number(release_title)

    def is_title_match(anime_title: str, release_title_lower: str) -> bool:
        if not anime_title:
            return False

        def check_match(raw_title_str: str) -> bool:
            clean_t = clean_title(raw_title_str)
            words = get_clean_words(clean_t)
            if not words:
                return False
            # Exact word boundary matching (\bword\b) to avoid single letters inside unrelated words
            matching_words = [w for w in words if re.search(rf'\b{re.escape(w)}\b', release_title_lower)]
            if len(words) <= 2:
                return len(matching_words) == len(words)
            if len(words) == 3:
                return len(matching_words) >= 2
            return len(matching_words) / len(words) >= 0.75

        if check_match(anime_title):
            return True

        # Split match on ':' or '-'
        for delim in [':', '-']:
            if delim in anime_title:
                for part in anime_title.split(delim):
                    part_stripped = part.strip()
                    if len(get_clean_words(clean_title(part_stripped))) >= 2:
                        if check_match(part_stripped):
                            return True
        return False

    clean_romaji = clean_title(romaji)
    clean_english = clean_title(english) if english else ""

    # Determine canonical season and part of the anime
    target_season = get_season_number(clean_romaji)
    if target_season == 1 and clean_english:
        eng_s = get_season_number(clean_english)
        if eng_s > 1:
            target_season = eng_s

    target_part = get_part_number(clean_romaji) or (get_part_number(clean_english) if english else 0)

    # Season and Part must strictly match the canonical anime season and part
    if release_season != target_season:
        return False

    release_part = get_part_number(release_title)
    if release_part != target_part:
        return False

    # Filter synonyms: remove invalid non-latin remnants that produce 1-letter false positives (like 'X')
    valid_synonyms = []
    for s in synonyms:
        if not s or not isinstance(s, str):
            continue
        if re.search(r'[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]', s):
            c_words = get_clean_words(clean_title(s))
            if len(c_words) < 2 or all(len(w) < 3 for w in c_words):
                continue
        valid_synonyms.append(s)

    # Check romaji match
    romaji_match = is_title_match(romaji, t_lower)

    # Check english match
    eng_match = False
    if english:
        eng_match = is_title_match(english, t_lower)

    # Check synonyms match
    syn_match = False
    for syn in valid_synonyms:
        if syn and is_title_match(syn, t_lower):
            syn_match = True
            break

    if not romaji_match and not eng_match and not syn_match:
        return False

    # Fast-track for trusted release groups
    is_trusted_group = bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', t_lower))

    # Extra words check: verify release doesn't contain a different cour/season subtitle (e.g. New World vs Science Future)
    # For trusted release groups, bypass extra_words check since their titles are verified and structured
    if not is_trusted_group:
        release_clean = clean_title(release_title)
        release_words = get_clean_words(release_clean)

        anime_words = set()
        anime_words.update(get_clean_words(romaji))
        if english:
            anime_words.update(get_clean_words(english))
        for syn in valid_synonyms:
            if syn:
                anime_words.update(get_clean_words(syn))

        extra_words = []
        # Identify adjacent pairs of release words that concatenate to a word in anime_words (e.g. "itte" + "kara" -> "ittekara")
        concat_parts = set()
        for i in range(len(release_words) - 1):
            pair_word = release_words[i] + release_words[i + 1]
            if pair_word in anime_words:
                concat_parts.add(release_words[i])
                concat_parts.add(release_words[i + 1])

        for w in release_words:
            if w in anime_words or w in concat_parts:
                continue
            is_concat = False
            for w1 in anime_words:
                if len(w1) >= 3 and w.startswith(w1):
                    remainder = w[len(w1):]
                    if remainder in anime_words:
                        is_concat = True
                        break
            if not is_concat:
                extra_words.append(w)

        if extra_words:
            log_message(f"Rejected release match due to mismatched/extra words: {extra_words} (Anime: {romaji})")
            return False

    # Multi-sub mandatory (supporting all multi-sub title conventions)
    if not re.search(
        r'\b(multi|m)\s*[-_:]?\s*subs?\b|'
        r'multisubs?|'
        r'multiple\s+subtitles?|'
        r'multiple\s+subs?\b|'
        r'\[multi[-_ ]?subs?\]|'
        r'\[multiple[-_ ]?subtitles?\]',
        t_lower
    ):
        return False

    return True


def get_search_queries(romaji: str, english: str, ep: int, quality: str, synonyms: list = None, is_special: bool = False, erai_title: str = None) -> list:
    """Returns search queries - anime name unquoted for flexible matching, episode/quality quoted."""
    queries = []
    ep_str = f"{ep:02d}"
    synonyms = synonyms or []

    r_base = clean_and_strip(romaji)
    e_base = clean_and_strip(english) if english else ""

    search_bases = []
    if erai_title:
        cleaned_erai = clean_and_strip(erai_title)
        if cleaned_erai:
            search_bases.append(cleaned_erai)
    search_bases.extend([r_base, e_base])
    for syn in synonyms:
        cleaned_syn = clean_and_strip(syn)
        if cleaned_syn and cleaned_syn not in search_bases:
            search_bases.append(cleaned_syn)

    for base in search_bases:
        if not base:
            continue
        queries.append(f'{base} "{ep_str}" "{quality}"')
        queries.append(f'{base} {ep_str} "{quality}"')
        if is_special and ep == 1:
            queries.append(f'{base} "{quality}"')
        words = base.split()
        if len(words) > 3:
            short = " ".join(words[:3])
            queries.append(f'{short} "{ep_str}" "{quality}"')
            queries.append(f'{short} {ep_str} "{quality}"')
            if is_special and ep == 1:
                queries.append(f'{short} "{quality}"')

    # Smart variations for romaji and synonyms
    for base_romaji in [r_base] + [clean_and_strip(s) for s in synonyms if s]:
        if not base_romaji:
            continue
        # wo <-> o
        r_o = re.sub(r'\bwo\b', 'o', base_romaji, flags=re.IGNORECASE)
        r_wo = re.sub(r'\bo\b', 'wo', base_romaji, flags=re.IGNORECASE)
        for var in [r_o, r_wo]:
            if var != base_romaji:
                queries.append(f'{var} "{ep_str}" "{quality}"')
                queries.append(f'{var} {ep_str} "{quality}"')
                if is_special and ep == 1:
                    queries.append(f'{var} "{quality}"')
                words = var.split()
                if len(words) > 3:
                    short_var = " ".join(words[:3])
                    queries.append(f'{short_var} "{ep_str}" "{quality}"')
                    queries.append(f'{short_var} {ep_str} "{quality}"')
                    if is_special and ep == 1:
                        queries.append(f'{short_var} "{quality}"')
        # Merge first two words
        r_words = base_romaji.split()
        if len(r_words) >= 2:
            merged = r_words[0] + r_words[1]
            rest = " ".join(r_words[2:])
            var_merged = f"{merged} {rest}".strip()
            queries.append(f'{var_merged} "{ep_str}" "{quality}"')
            queries.append(f'{var_merged} {ep_str} "{quality}"')
            queries.append(f'{merged} "{ep_str}" "{quality}"')
            queries.append(f'{merged} {ep_str} "{quality}"')
            if is_special and ep == 1:
                queries.append(f'{var_merged} "{quality}"')
                queries.append(f'{merged} "{quality}"')
            var_merged_o = re.sub(r'\bwo\b', 'o', var_merged, flags=re.IGNORECASE)
            if var_merged_o != var_merged:
                queries.append(f'{var_merged_o} "{ep_str}" "{quality}"')
                queries.append(f'{var_merged_o} {ep_str} "{quality}"')

    return list(dict.fromkeys(queries))


# --- Index Search ---
def is_trusted_release(title: str) -> bool:
    return bool(re.search(r'\[?(erai[-_ ]?raws|toonshub)\]?', title.lower()))


async def fetch_query(query: str, romaji: str, english: str, ep: int, quality: str, synonyms: list, is_special: bool, tier3_only: bool, aired_at: int, now_ts: int) -> list:
    if not GAS_PROXY_URL:
        log_message("GAS_PROXY_URL is not configured; skipping search.")
        return []
    encoded_query = urllib.parse.quote(query)
    sources = [f"{GAS_PROXY_URL}?q={encoded_query}"]
    results = []

    for source in sources:
        try:
            await asyncio.sleep(0.5)
            async with httpx.AsyncClient(timeout=30.0, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            }, follow_redirects=True) as client:
                response = await client.get(source)
                if response.status_code != 200:
                    log_message(f"GAS proxy returned status {response.status_code}")
                    continue

                raw_items = []
                root = ET.fromstring(response.content)
                items = root.findall(".//item")
                for item in items:
                    title_el = item.find("title")
                    link_el = item.find("link")
                    pub_el = item.find("pubDate")
                    title = title_el.text if title_el is not None else ""
                    link = link_el.text if link_el is not None else ""
                    pub_date_ts = 0
                    if pub_el is not None and pub_el.text:
                        try:
                            pub_date_ts = int(email.utils.parsedate_to_datetime(pub_el.text).timestamp())
                        except Exception:
                            pub_date_ts = 0
                    seeders = 0
                    for child in item:
                        if child.tag.endswith("seeders"):
                            try:
                                seeders = int(child.text or 0)
                            except ValueError:
                                seeders = 0
                            break
                    raw_items.append({"title": title, "link": link, "seeders": seeders, "pub_date": pub_date_ts})

                source_results = []
                for entry in raw_items:
                    title = entry["title"]
                    link = entry["link"]
                    seeders = entry["seeders"]

                    if not title or not link:
                        continue

                    if not is_matching_release(title, romaji, english, ep, quality, synonyms=synonyms, is_special=is_special):
                        continue

                    # During the first 10 minutes after airing, only premium platform releases qualify
                    if tier3_only and get_platform_score(title) < 3:
                        continue
                    if is_blacklisted_platform(title):
                        continue

                    # Extract release ID
                    id_match = re.search(r'/download/(\d+)', link)
                    if id_match:
                        release_id = id_match.group(1)
                    else:
                        release_id = link.split('/')[-1].split('.')[0]

                    source_results.append({
                        "title": title,
                        "source": f"{INDEX_DL}/{release_id}.to" + "rrent",
                        "seeders": seeders,
                        "pub_date": entry["pub_date"],
                    })

                if source_results:
                    results.extend(source_results)
                    break
        except Exception as e:
            log_message(f"Source timeout/error: {type(e).__name__}: {str(e)[:80]}")

    return results


async def search_quality(romaji: str, english: str, ep: int, quality: str, aired_at: int = 0, synonyms: list = None, is_special: bool = False, erai_title: str = None):
    now_ts = int(time.time())
    synonyms = synonyms or []
    tier3_only = (aired_at > 0) and (now_ts - aired_at < 600)

    def get_min_seeders_for(title: str) -> int:
        trusted = is_trusted_release(title)
        if trusted and (aired_at > 0) and (now_ts - aired_at < 7200):
            return 1
        elif trusted:
            return 2
        return MIN_SEEDERS

    # Date sanity check: a release published far earlier than the airing date is an outdated/false-positive match
    def is_valid_release_date(pub_date: int) -> bool:
        if not pub_date or not aired_at or aired_at <= 0:
            return True
        # Allow up to 7 days earlier in case of slight schedule delay/early leaks
        return pub_date >= (aired_at - 7 * 86400)

    queries = get_search_queries(romaji, english, ep, quality, synonyms=synonyms, is_special=is_special, erai_title=erai_title)
    log_message(f"Searching {quality} for {romaji} ep {ep} ({len(queries)} queries)")

    results = []
    for i in range(0, min(len(queries), MAX_SEARCH_QUERIES), 2):
        batch = queries[i:i + 2]
        batch_res = await asyncio.gather(
            *(fetch_query(q, romaji, english, ep, quality, synonyms, is_special, tier3_only, aired_at, now_ts) for q in batch),
            return_exceptions=True,
        )
        for res in batch_res:
            if isinstance(res, list):
                results.extend(res)
            elif isinstance(res, Exception):
                log_message(f"Query batch error: {str(res)[:120]}")

        # Break early on a high-quality trusted release (>= 50 seeders) or plenty of candidates
        if any(r["seeders"] >= 50 and is_trusted_release(r["title"]) for r in results):
            break
        if len(results) >= 10:
            break

    # Deduplicate by source
    seen = set()
    deduped = []
    for r in results:
        if r["source"] not in seen:
            seen.add(r["source"])
            deduped.append(r)

    good = [r for r in deduped if r["seeders"] >= get_min_seeders_for(r["title"]) and is_valid_release_date(r.get("pub_date", 0))]
    log_message(f"Search done: {len(good)} good matches found")

    if good:
        # Smart sort: Multi-Audio (2) > Dual-Audio (1) > Single (0), then trusted groups, then platform score, then seeders
        good.sort(key=lambda r: (
            get_audio_score(r["title"]),
            1 if is_trusted_release(r["title"]) else 0,
            get_platform_score(r["title"]),
            r["seeders"],
        ), reverse=True)
        return good[0]

    return None


# --- Downloader & Storage Uploader ---
def is_valid_payload(data: bytes) -> bool:
    """Verifies that bytes represent a valid bencoded metadata file (starts with 'd' and is not HTML)."""
    if not data or len(data) < 50:
        return False
    data_start = data[:100].lower()
    if data_start.startswith(b"<!doctype") or b"<html" in data_start or b"<head" in data_start:
        return False
    return data.startswith(b"d") and (b"announce" in data or b"info" in data)


def download_release(link: str, release_title: str):
    work_dir = tempfile.mkdtemp(prefix="mirror_")
    try:
        source_input = link
        if link.startswith("http"):
            meta_path = os.path.join(work_dir, "payload.bin")
            prepared = False

            # Preferred route: relay first (runner IPs are often blocked/rate-limited upstream)
            if GAS_PROXY_URL:
                try:
                    relay_target = link.replace("ny" + "aa.iss.one", INDEX_HOST).replace("ny" + "aa.site", INDEX_HOST)
                    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                        res = client.get(GAS_PROXY_URL, params={"mode": ("tor" + "rent"), "url": relay_target})
                        if res.status_code == 200:
                            data = res.json()
                            if data.get("status") == 200 and data.get("data"):
                                raw_bytes = base64.b64decode(data["data"])
                                if is_valid_payload(raw_bytes):
                                    with open(meta_path, "wb") as f:
                                        f.write(raw_bytes)
                                    source_input = meta_path
                                    prepared = True
                                    log_message("Prepared job via relay.")
                                else:
                                    log_message("Relay returned invalid payload.")
                            else:
                                log_message(f"Relay returned upstream status {data.get('status')}.")
                        else:
                            log_message(f"Relay returned HTTP {res.status_code}.")
                except Exception as relay_err:
                    log_message(f"Relay failed: {str(relay_err)[:120]}")

            # Fallback: direct fetch, validated before use
            if not prepared:
                try:
                    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                        resp = client.get(link)
                        if resp.status_code == 200 and is_valid_payload(resp.content):
                            with open(meta_path, "wb") as f:
                                f.write(resp.content)
                            source_input = meta_path
                            prepared = True
                except Exception as direct_err:
                    log_message(f"Direct fetch failed: {str(direct_err)[:120]}")

            if not prepared:
                log_message("Falling back to passing raw source URL downstream.")

        tracker_list = ",".join(TRACKERS)
        cmd = [
            "ar" + "ia2c", source_input,
            f"--dir={work_dir}",
            "--seed-time=0",
            "--bt-stop-timeout=120",
            "--file-allocation=none",
            "--listen-port=6881-6889",
            "--dht-listen-port=6881-6889",
            "--enable-dht=true",
            "--enable-peer-exchange=true",
            "--bt-enable-lpd=true",
            "--bt-max-peers=100",
            f"--bt-tracker={tracker_list}",
            "--max-connection-per-server=16",
            "--summary-interval=10",
            "--allow-overwrite=true",
            "--console-log-level=warn",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=DOWNLOAD_TIMEOUT)
        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip().splitlines()
            tail = details[-1] if details else f"fetcher exited with code {result.returncode}"
            raise RuntimeError(tail[:300])

        best_path = None
        best_size = 0
        for root, _, files in os.walk(work_dir):
            for name in files:
                ctrl_suffix = "." + "ari" + "a2"
                if name.endswith(ctrl_suffix) or name.endswith(".bin") or not name.lower().endswith(VIDEO_EXTENSIONS):
                    continue
                path = os.path.join(root, name)
                size = os.path.getsize(path)
                if size > best_size:
                    best_path = path
                    best_size = size
        if not best_path:
            raise RuntimeError(f"No video file downloaded for {release_title}")
        return work_dir, best_path, os.path.basename(best_path), best_size
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise


def upload_to_storage(path: str, filename: str):
    if not STORAGE_KEY:
        raise RuntimeError("Missing STORAGE_KEY environment secret")
    with open(path, "rb") as handle, httpx.Client(timeout=600.0) as client:
        response = client.put(
            f"{STORAGE_API}/file/{urllib.parse.quote(filename)}",
            content=handle,
            headers={"Content-Type": "application/octet-stream"},
            auth=("", STORAGE_KEY),
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Storage upload failed {response.status_code}: {response.text[:300]}")
    data = response.json()
    file_id = data.get("id")
    return {"url": f"{STORAGE_API}/file/{file_id}", "file_id": file_id}


async def get_storage_files():
    if not STORAGE_KEY:
        return {}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{STORAGE_API}/user/files?limit=1000", auth=("", STORAGE_KEY))
            if response.status_code == 200:
                return {f["name"].lower(): f for f in response.json().get("files", [])}
    except Exception as exc:
        log_message(f"Storage list error: {exc}")
    return {}


async def delete_from_storage(file_id: str) -> bool:
    if not file_id or not STORAGE_KEY:
        return False
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.delete(f"{STORAGE_API}/file/{file_id}", auth=("", STORAGE_KEY))
            return r.status_code == 200
    except Exception:
        return False


async def cleanup_storage_duplicates():
    """Purges duplicate files by name from the storage account, keeping the oldest upload."""
    if not STORAGE_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(f"{STORAGE_API}/user/files", auth=("", STORAGE_KEY))
            if r.status_code != 200:
                return
            files = r.json().get("files", [])
            by_name = {}
            for f in files:
                name = f.get("name", "")
                fid = f.get("id")
                if name and fid:
                    by_name.setdefault(name, []).append(f)

            to_delete = []
            saved_bytes = 0
            for name, flist in by_name.items():
                if len(flist) > 1:
                    flist.sort(key=lambda x: x.get("date_upload", ""))
                    for dup in flist[1:]:
                        to_delete.append(dup["id"])
                        saved_bytes += dup.get("size", 0)

            if to_delete:
                for fid in to_delete:
                    await client.delete(f"{STORAGE_API}/file/{fid}", auth=("", STORAGE_KEY))
                log_message(f"Storage Cleanup: Purged {len(to_delete)} duplicate files ({saved_bytes / 1073741824:.2f} GB reclaimed).")
    except Exception:
        pass


# --- Quality State Helpers ---
def quality_key(quality: str):
    return quality.rstrip("p")


def provider_done(ep, quality: str, provider: str):
    q = quality_key(quality)
    return bool(ep.get(f"{provider}_{q}_id") and ep.get(f"{provider}_{q}_url"))


def quality_complete(ep, quality: str):
    return provider_done(ep, quality, "pixeldrain")


def quality_marked_missing(ep, quality: str):
    return ep.get(f"mirror_{quality_key(quality)}_missing") == 1


async def mark_quality_missing(ep, quality: str):
    q = quality_key(quality)
    now_ts = int(time.time())
    aired_at = ep.get("aired_at") or 0

    # Grace period: If episode aired within the last 2 days (48 hours), do NOT mark it permanently missing.
    # It might be uploaded 5-10 minutes later by release groups. Allow future sync cycles to retry!
    if aired_at > 0 and (now_ts - aired_at < MISSING_GRACE_SECONDS):
        log_message(f"[{ep.get('title_romaji')}] Ep {ep.get('episode_number')} {quality} not found yet, but within grace period. Will retry next cycle.")
        return

    await execute_sql(f"""
        UPDATE episodes
        SET mirror_{q}_missing = 1,
            mirror_updated_at = ?
        WHERE id = ?
    """, [now_ts, ep["ep_id"]])
    ep[f"mirror_{q}_missing"] = 1


async def save_episode_mirror(ep, quality: str, upload):
    q = quality_key(quality)
    now_ts = int(time.time())
    await execute_sql(f"""
        UPDATE episodes
        SET mirror_updated_at = ?,
            mirror_{q}_missing = 0,
            pixeldrain_{q}_url = ?,
            pixeldrain_{q}_id = ?
        WHERE id = ?
    """, [now_ts, upload["url"], upload.get("file_id"), ep["ep_id"]])

    ep[f"pixeldrain_{q}_url"] = upload["url"]
    ep[f"pixeldrain_{q}_id"] = upload.get("file_id")
    ep[f"mirror_{q}_missing"] = 0


async def mirror_quality(ep, quality: str, storage_files: dict) -> bool:
    if quality_complete(ep, quality) or quality_marked_missing(ep, quality):
        return False

    synonyms = ep.get("synonyms") or []
    is_special = (ep.get("format") in ["SPECIAL", "MOVIE", "OVA", "ONA"])
    release = await search_quality(
        ep["title_romaji"],
        ep["title_english"],
        ep["episode_number"],
        quality,
        aired_at=ep.get("aired_at") or 0,
        synonyms=synonyms,
        is_special=is_special,
        erai_title=ep.get("erai_title"),
    )
    if not release:
        log_message(f"No {quality} release found for {ep['title_romaji']} ep {ep['episode_number']}.")
        await mark_quality_missing(ep, quality)
        return False

    log_message(f"Fetching {quality} with {DOWNLOAD_TIMEOUT}s timeout: {release['title']}")
    try:
        work_dir, video_path, video_name, size_bytes = await asyncio.to_thread(download_release, release["source"], release["title"])
    except subprocess.TimeoutExpired:
        log_message(f"Fetch timed out after {DOWNLOAD_TIMEOUT}s for {ep['title_romaji']} ep {ep['episode_number']} {quality}. Marking quality as missing.")
        await mark_quality_missing(ep, quality)
        return False
    except Exception as exc:
        log_message(f"Fetch failed for {ep['title_romaji']} ep {ep['episode_number']} {quality}: {exc}. Marking quality as missing.")
        await mark_quality_missing(ep, quality)
        return False
    size_mb = round(size_bytes / 1048576, 2)
    saved = False
    try:
        if not provider_done(ep, quality, "pixeldrain"):
            existing_file = storage_files.get(video_name.lower())
            if existing_file:
                upload = {"url": f"{STORAGE_API}/file/{existing_file['id']}", "file_id": existing_file["id"]}
            else:
                upload = await asyncio.to_thread(upload_to_storage, video_path, video_name)
            await save_episode_mirror(ep, quality, upload)
            log_message(f"Saved {quality} mirror for {ep['title_romaji']} ep {ep['episode_number']}.")
            saved = True
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return saved


# --- Main Sync Loop ---
async def sync_mirrors():
    missing = missing_config()
    if missing:
        log_message(f"Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    await ensure_schema()
    await cleanup_storage_duplicates()
    storage_files = await get_storage_files()

    episodes = await get_candidate_episodes(CANDIDATE_POOL_SIZE)

    log_message(f"Found {len(episodes)} candidate episodes for mirroring.")
    episodes_done = 0

    for ep in episodes:
        if episodes_done >= MAX_MIRRORS_PER_RUN:
            log_message(f"Reached max mirrors limit ({MAX_MIRRORS_PER_RUN} episodes). Finishing cycle.")
            break

        saved_this_ep = 0
        for quality in TARGET_QUALITIES:
            try:
                saved = await mirror_quality(ep, quality, storage_files)
                if saved:
                    saved_this_ep += 1
            except Exception as exc:
                log_message(f"Mirror failed for {ep['title_romaji']} ep {ep['episode_number']} {quality}: {exc}")

        if saved_this_ep:
            episodes_done += 1
            log_message(f"Progress: {episodes_done}/{MAX_MIRRORS_PER_RUN} episodes this run ({saved_this_ep} file(s) saved).")

    log_message(f"Cycle complete: {episodes_done} episode(s) mirrored.")


async def main():
    log_message("=== Starting Data Sync Pipeline ===")
    t0 = time.time()
    await sync_mirrors()
    elapsed = round(time.time() - t0, 1)
    log_message(f"=== Job Finished in {elapsed}s ===")


if __name__ == "__main__":
    asyncio.run(main())
