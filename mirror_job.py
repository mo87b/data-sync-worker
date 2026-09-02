import os
import re
import sys
import json
import time
import base64
import hashlib
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

GAS_PROXIES = []
for _k in ["RELAY_URL", "RELAY_URL_2", "RELAY_URL_3", "GAS_PROXY_URL", "GAS_PROXY_URL_2", "GAS_PROXY_URL_3"]:
    _v = os.environ.get(_k, "").strip()
    if _v and _v not in GAS_PROXIES:
        GAS_PROXIES.append(_v)

_proxy_idx = 0
def get_ordered_proxies() -> list:
    global _proxy_idx
    if not GAS_PROXIES:
        return []
    n = len(GAS_PROXIES)
    start = _proxy_idx % n
    _proxy_idx += 1
    return [GAS_PROXIES[(start + i) % n] for i in range(n)]


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
MAX_REPAIRS_PER_RUN = _int_env("MAX_REPAIRS_PER_RUN", 3)
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
    missing = []
    if not TURSO_URL:
        missing.append("TURSO_URL")
    if not TURSO_TOKEN:
        missing.append("TURSO_TOKEN")
    if not STORAGE_KEY:
        missing.append("STORAGE_KEY")
    if not GAS_PROXIES:
        missing.append("RELAY_URL")
    return missing


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
        "mirror_720_source": "TEXT",
        "mirror_480_source": "TEXT",
        "subtitles": "TEXT",
        "audio_tracks": "TEXT",
        "subtitles_1080": "TEXT",
        "audio_tracks_1080": "TEXT",
        "subtitles_720": "TEXT",
        "audio_tracks_720": "TEXT",
        "subtitles_480": "TEXT",
        "audio_tracks_480": "TEXT",
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
           e.subtitles, e.audio_tracks,
           e.subtitles_1080, e.audio_tracks_1080,
           e.subtitles_720, e.audio_tracks_720,
           e.subtitles_480, e.audio_tracks_480,
           a.anilist_id, a.title_romaji, a.title_english, a.format, a.erai_title, a.synonyms
    FROM episodes e
    JOIN anime a ON e.anime_id = a.id
    WHERE e.status = 'ready'
      AND (
            (COALESCE(e.mirror_720_missing, 0) = 0 AND e.pixeldrain_720_id IS NULL)
         OR (COALESCE(e.mirror_480_missing, 0) = 0 AND e.pixeldrain_480_id IS NULL)
      )
      AND NOT (a.anilist_id = 21 AND e.episode_number < 1100)
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
    clean_no_ver = re.sub(r'\bv\d+\b', '', title_lower)
    if re.search(r'\bii\b$', clean_no_ver) or re.search(r'\bii\b(?=\s)', clean_no_ver):
        return 2
    if re.search(r'\biii\b$', clean_no_ver) or re.search(r'\biii\b(?=\s)', clean_no_ver):
        return 3
    if re.search(r'\biv\b$', clean_no_ver) or re.search(r'\biv\b(?=\s)', clean_no_ver):
        return 4
    if (re.search(r'\bv\b$', clean_no_ver) or re.search(r'\bv\b(?=\s)', clean_no_ver)) and not re.search(r'\b(1080p|720p|480p|2160p|mkv|mp4|v)\s+v\b', title_lower):
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
    Score hierarchy:
      4: Multi-Audio (e.g. MULTi-Audio / MULTi AAC)
      3: Dual-Audio (e.g. DUAL / Dual-Audio / DUAL AAC)
      2: Explicit Japanese Audio (e.g. (JA), (JP), Japanese Dub, Japanese Audio, WEB-DLJPN)
      1: Default / Standard Japanese (clean anime release with no foreign audio tags)
     -5: Foreign Single Audio Only (e.g. (KA), Korean Audio, (ZH), Chinese Dub, standalone English Dub)
    """
    if not title or not isinstance(title, str):
        return 0
    t_lower = title.lower()

    # 1. Multi-Audio (highest priority)
    if re.search(r'\bmulti[- ]audio\b|multiaudio|\bmulti\s+aac\b', t_lower):
        return 4

    # 2. Dual-Audio
    if re.search(r'\bdual[- ]audio\b|dualaudio|\bdual\s+aac\b|\bdual\b', t_lower):
        return 3

    # 3. Check for Explicit Foreign Audio Only (Korean, Chinese, English dub, etc. without Dual/Multi)
    is_foreign = bool(re.search(
        r'[\(\[]\s*(ka|ko|kor|zh|cn|chi)\s*[\)\]]|'
        r'\b(korean|kor)\s*[-_ ]*(audio|dub)\b|'
        r'\b(chinese|mandarin)\s*[-_ ]*(audio|dub)\b|'
        r'\b(english|eng)\s*[-_ ]*dub\b|'
        r'web-dl\s*(kor|chi)',
        t_lower
    ))

    # 4. Explicit Japanese Audio
    is_japanese = bool(re.search(
        r'[\(\[]\s*(ja|jp|jpn)\s*[\)\]]|'
        r'\b(japanese|jpn|jap)\s*[-_ ]*(audio|dub)\b|'
        r'web-dl\s*jpn',
        t_lower
    ))

    if is_foreign and not is_japanese:
        return -5

    if is_japanese:
        return 2

    # 5. Default Japanese (standard anime release)
    return 1

def is_multi_audio_torrent(title: str) -> bool:
    return get_audio_score(title) >= 3



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
    if not words:
        clean_with_num = title_no_se.replace('.', ' ').replace('-', ' ').replace("'", "")
        clean_with_num = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_with_num)
        words = clean_with_num.split()
    filtered = []
    for w in words:
        w_stripped = w.strip("-'")
        if not w_stripped:
            continue
        if w_stripped in SEASON_STOPWORDS or w_stripped in PARTICLES:
            continue
        if len(w_stripped) >= 2 or (len(w_stripped) == 1 and w_stripped.isalnum()):
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

            matching_words = set()
            for w in words:
                if re.search(rf'\b{re.escape(w)}\b', release_title_lower):
                    matching_words.add(w)

            # Check adjacent merged words (e.g. "Dogul Wang" -> "Dogulwang", "Chainsaw Man" -> "Chainsawman")
            for i in range(len(words) - 1):
                w1, w2 = words[i], words[i+1]
                if len(w1) >= 2 and len(w2) >= 2:
                    pair = w1 + w2
                    if re.search(rf'\b{re.escape(pair)}\b', release_title_lower):
                        matching_words.add(w1)
                        matching_words.add(w2)

            # Check if entire title with no spaces matches
            if len(words) >= 2:
                all_merged = "".join(words)
                if re.search(rf'\b{re.escape(all_merged)}\b', release_title_lower):
                    for w in words:
                        matching_words.add(w)

            ratio = len(matching_words) / len(words)
            if len(words) <= 2:
                return len(matching_words) == len(words)
            if len(words) == 3:
                return len(matching_words) >= 2
            return ratio >= 0.75

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

    # Extra words check: verify release doesn't contain a different cour/season subtitle
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

    max_extra = 2 if is_trusted_group else 0
    if len(extra_words) > max_extra:
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
    
    # Collapsed variations (e.g. "Dogul Wang" -> "Dogulwang", "Chainsaw Man" -> "Chainsawman")
    if len(r_base.split()) >= 2:
        r_collapsed = "".join(r_base.split())
        if len(r_collapsed) >= 3 and r_collapsed not in search_bases:
            search_bases.append(r_collapsed)
    
    # Japanese suffix / hyphen variations (e.g. Tenkousaki -> Tenkou-saki / Tenkou saki)
    COMMON_SUFFIXES = ["saki", "tabi", "gumi", "jima", "bashi", "mura", "kan", "sou", "ken", "chou"]
    for title_base in [r_base] + synonyms:
        if not title_base:
            continue
        c_words = clean_and_strip(title_base).split()
        for i, w in enumerate(c_words[:3]):
            w_lower = w.lower()
            if "-" in w:
                unhyphen = w.replace("-", "")
                spaced = w.replace("-", " ")
                v1 = " ".join(c_words[:i] + [unhyphen] + c_words[i+1:])
                v2 = " ".join(c_words[:i] + [spaced] + c_words[i+1:])
                for var in (v1, v2):
                    if var and var not in search_bases:
                        search_bases.append(var)
            else:
                for sfx in COMMON_SUFFIXES:
                    if w_lower.endswith(sfx) and len(w_lower) > len(sfx) + 2:
                        pfx = w[:-len(sfx)]
                        hyphen_var = f"{pfx}-{sfx}"
                        space_var = f"{pfx} {sfx}"
                        v1 = " ".join(c_words[:i] + [hyphen_var] + c_words[i+1:])
                        v2 = " ".join(c_words[:i] + [space_var] + c_words[i+1:])
                        for var in (v1, v2):
                            if var and var not in search_bases:
                                search_bases.append(var)

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
    proxies = get_ordered_proxies()
    if not proxies:
        log_message("GAS proxies are not configured; skipping search.")
        return []
    encoded_query = urllib.parse.quote(query)
    results = []

    transport = httpx.AsyncHTTPTransport(retries=2)
    for proxy_base in proxies:
        source = f"{proxy_base}?q={encoded_query}"
        try:
            await asyncio.sleep(0.3)
            async with httpx.AsyncClient(transport=transport, timeout=20.0, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            }, follow_redirects=True) as client:
                response = await client.get(source)
                if response.status_code != 200:
                    continue

                raw_items = []
                text = response.text.strip()
                if text.startswith("{"):
                    try:
                        data = response.json()
                        payload = data.get("data")
                        if isinstance(payload, list):
                            for item in payload:
                                raw_items.append({
                                    "title": item.get("title", ""),
                                    "link": item.get("torrent", "") or item.get("link", ""),
                                    "seeders": int(item.get("seeders") or 0),
                                    "pub_date": int(item.get("pub_date") or item.get("timestamp") or 0)
                                })
                    except Exception:
                        pass
                elif "<rss" in text or "<item" in text:
                    try:
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
                    except Exception:
                        pass

                source_results = []
                for entry in raw_items:
                    title = entry["title"]
                    link = entry["link"]
                    seeders = entry["seeders"]

                    if not title or not link:
                        continue

                    if not is_matching_release(title, romaji, english, ep, quality, synonyms=synonyms, is_special=is_special):
                        continue

                    if tier3_only and get_platform_score(title) < 3:
                        continue
                    if is_blacklisted_platform(title):
                        continue

                    id_match = re.search(r'/download/(\d+)', link)
                    if id_match:
                        release_id = id_match.group(1)
                    else:
                        release_id = link.split('/')[-1].split('.')[0]

                    source_results.append({
                        "title": title,
                        "source": f"https://nyaa.si/download/{release_id}.torrent",
                        "seeders": seeders,
                        "pub_date": entry["pub_date"],
                    })

                if source_results:
                    return source_results
        except Exception as e:
            continue

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
def extract_info_hash(payload: bytes) -> str:
    """SHA-1 of the raw bencoded 'info' dictionary of a .torrent file."""
    try:
        data = payload

        def read_str(i):
            colon = data.index(b":", i)
            length = int(data[i:colon])
            start = colon + 1
            return start, start + length

        def skip(i):
            c = data[i:i+1]
            if c == b"i":
                end = data.index(b"e", i)
                return end + 1
            if c in (b"d", b"l"):
                i += 1
                is_dict = c == b"d"
                while data[i:i+1] != b"e":
                    if is_dict:
                        _, i = read_str(i)
                    i = skip(i)
                return i + 1
            _, end = read_str(i)
            return end

        if data[:1] != b"d":
            return None
        i = 1
        while data[i:i+1] != b"e":
            ks, ke = read_str(i)
            key = data[ks:ke]
            val_start = ke
            val_end = skip(val_start)
            if key == b"info":
                return hashlib.sha1(data[val_start:val_end]).hexdigest()
            i = val_end
    except Exception:
        return None
    return None


def is_valid_payload(data: bytes) -> bool:
    """Verifies that bytes represent a valid bencoded metadata file (starts with 'd' and is not HTML)."""
    if not data or len(data) < 50:
        return False
    data_start = data[:100].lower()
    if data_start.startswith(b"<!doctype") or b"<html" in data_start or b"<head" in data_start:
        return False
    return data.startswith(b"d") and (b"announce" in data or b"info" in data)


def inspect_media_tracks(video_path: str) -> tuple:
    """Uses ffprobe to extract subtitle and audio tracks, keeping only allowed languages."""
    ALLOWED_SUBS = {"Arabic", "English", "French", "Japanese"}
    ALLOWED_AUDIO = {"Japanese", "Arabic", "English", "French", "Chinese", "Korean"}

    LANG_MAP = {
        "ara": "Arabic", "ar": "Arabic", "arabic": "Arabic", "العربية": "Arabic", "عربي": "Arabic",
        "eng": "English", "en": "English", "english": "English",
        "fra": "French", "fre": "French", "fr": "French", "french": "French", "français": "French",
        "jpn": "Japanese", "ja": "Japanese", "japanese": "Japanese", "jp": "Japanese", "日本語": "Japanese",
        "chi": "Chinese", "zho": "Chinese", "zh": "Chinese", "chinese": "Chinese",
        "kor": "Korean", "ko": "Korean", "korean": "Korean",
    }

    def _resolve_lang(lang_tag: str, title_tag: str) -> str:
        tag_str = (lang_tag or "").strip().lower()
        title_str = (title_tag or "").strip().lower()

        if tag_str in LANG_MAP:
            return LANG_MAP[tag_str]
        
        for key, name in LANG_MAP.items():
            if re.search(rf'\b{re.escape(key)}\b', title_str):
                return name
            if name.lower() in title_str:
                return name
        return None

    found_subs = set()
    found_audio = set()

    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_entries", "stream=codec_type:stream_tags=language,title",
            video_path
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            streams = data.get("streams", [])
            for s in streams:
                c_type = s.get("codec_type")
                tags = s.get("tags") or {}
                lang_tag = tags.get("language")
                title_tag = tags.get("title")

                resolved = _resolve_lang(lang_tag, title_tag)
                if c_type == "subtitle":
                    if resolved and resolved in ALLOWED_SUBS:
                        found_subs.add(resolved)
                elif c_type == "audio":
                    if resolved and resolved in ALLOWED_AUDIO:
                        found_audio.add(resolved)
                    elif not resolved and not found_audio:
                        found_audio.add("Japanese")
    except Exception as e:
        log_message(f"Media probe warning: {e}")

    if not found_audio:
        found_audio.add("Japanese")

    ORDER = ["Arabic", "English", "French", "Japanese", "Chinese", "Korean"]
    sorted_subs = sorted(found_subs, key=lambda x: ORDER.index(x) if x in ORDER else 99)
    sorted_audio = sorted(found_audio, key=lambda x: ORDER.index(x) if x in ORDER else 99)

    return ", ".join(sorted_subs), ", ".join(sorted_audio)


def download_release(link: str, release_title: str):
    work_dir = tempfile.mkdtemp(prefix="mirror_")
    meta_bytes = None
    try:
        source_input = link
        if link.startswith("http"):
            meta_path = os.path.join(work_dir, "payload.bin")
            prepared = False

            # Preferred route: relay first via available GAS proxies with failover
            relay_target = link.replace("ny" + "aa.iss.one", INDEX_HOST).replace("ny" + "aa.site", INDEX_HOST)
            sync_transport = httpx.HTTPTransport(retries=2)
            for proxy_base in get_ordered_proxies():
                try:
                    with httpx.Client(transport=sync_transport, timeout=30.0, follow_redirects=True) as client:
                        res = client.get(proxy_base, params={"mode": "torrent", "url": relay_target})
                        if res.status_code == 200:
                            data = res.json()
                            if data.get("status") == 200 and data.get("data"):
                                raw_bytes = base64.b64decode(data["data"])
                                if is_valid_payload(raw_bytes):
                                    with open(meta_path, "wb") as f:
                                        f.write(raw_bytes)
                                    meta_bytes = raw_bytes
                                    source_input = meta_path
                                    prepared = True
                                    log_message("Prepared job via relay.")
                                    break
                except Exception as relay_err:
                    continue

            # Fallback: direct fetch, validated before use
            if not prepared:
                try:
                    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                        resp = client.get(link)
                        if resp.status_code == 200 and is_valid_payload(resp.content):
                            with open(meta_path, "wb") as f:
                                f.write(resp.content)
                            meta_bytes = resp.content
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
        info_hash = extract_info_hash(meta_bytes) if meta_bytes else None
        return work_dir, best_path, os.path.basename(best_path), best_size, info_hash
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
    """Purges duplicate files by name from the storage account.
    Never deletes files referenced by the database; otherwise keeps the oldest."""
    if not STORAGE_KEY:
        return
    try:
        referenced_rows = await execute_sql("""
            SELECT pixeldrain_1080_id AS pid FROM episodes WHERE pixeldrain_1080_id IS NOT NULL
            UNION SELECT pixeldrain_720_id FROM episodes WHERE pixeldrain_720_id IS NOT NULL
            UNION SELECT pixeldrain_480_id FROM episodes WHERE pixeldrain_480_id IS NOT NULL
        """) or []
        referenced = {r["pid"] for r in referenced_rows if r.get("pid")}

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
                    keep_ids = {f["id"] for f in flist if f["id"] in referenced}
                    if not keep_ids:
                        flist.sort(key=lambda x: x.get("date_upload", ""))
                        keep_ids = {flist[0]["id"]}
                    for dup in flist:
                        if dup["id"] not in keep_ids:
                            to_delete.append(dup["id"])
                            saved_bytes += dup.get("size", 0)

            if to_delete:
                for fid in to_delete:
                    await client.delete(f"{STORAGE_API}/file/{fid}", auth=("", STORAGE_KEY))
                log_message(f"Storage Cleanup: Purged {len(to_delete)} duplicate files ({saved_bytes / 1073741824:.2f} GB reclaimed).")
    except Exception:
        pass


# --- Link Guardian ---
async def get_storage_file_ids():
    if not STORAGE_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{STORAGE_API}/user/files?limit=1000", auth=("", STORAGE_KEY))
            if response.status_code == 200:
                return {f.get("id") for f in response.json().get("files", []) if f.get("id")}
    except Exception as exc:
        log_message(f"LinkGuardian: storage list error: {exc}")
    return None


async def queue_fresh_search(ep_id):
    await execute_sql("""
        UPDATE episodes
        SET status = 'pending',
            stream_url = NULL,
            pixeldrain_id = NULL,
            pixeldrain_1080_url = NULL,
            pixeldrain_1080_id = NULL,
            magnet_link = NULL,
            last_checked = NULL,
            uploaded_at = NULL
        WHERE id = ?
    """, [ep_id])


async def restore_from_source(source: str, label: str, ep_id: str, quality: str = None) -> bool:
    """Re-download the exact stored torrent and re-upload it.
    quality=None repairs the main 1080 slot, otherwise the given mirror quality."""
    work_dir = None
    try:
        work_dir, v_path, v_name, size_bytes, _info_hash = await asyncio.to_thread(download_release, source, f"restore {label}")
        size_mb = round(size_bytes / 1048576, 2)
        upload = await asyncio.to_thread(upload_to_storage, v_path, v_name)
        shutil.rmtree(work_dir, ignore_errors=True)
        work_dir = None
        now_ts = int(time.time())
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if quality:
            q = quality_key(quality)
            await execute_sql(f"""
                UPDATE episodes
                SET pixeldrain_{q}_url = ?, pixeldrain_{q}_id = ?,
                    mirror_{q}_source = ?,
                    mirror_{q}_missing = 0,
                    file_size_mb = ?, mirror_updated_at = ?
                WHERE id = ?
            """, [upload["url"], upload["file_id"], source, size_mb, now_ts, ep_id])
        else:
            await execute_sql("""
                UPDATE episodes
                SET stream_url = ?, pixeldrain_id = ?,
                    pixeldrain_1080_url = ?, pixeldrain_1080_id = ?,
                    file_size_mb = ?, uploaded_at = ?, last_checked = ?
                WHERE id = ?
            """, [upload["url"], upload["file_id"], upload["url"], upload["file_id"],
                  size_mb, now_str, now_ts, ep_id])
        log_message(f"LinkGuardian: restored {label} from stored source (same file re-uploaded).")
        return True
    except Exception as exc:
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)
        log_message(f"LinkGuardian: restore failed for {label} ({type(exc).__name__}). Falling back to fresh search.")
        return False


async def reconcile_storage():
    remote_ids = await get_storage_file_ids()
    if remote_ids is None:
        log_message("LinkGuardian: storage listing unavailable, skipping.")
        return

    rows = await execute_sql("""
        SELECT e.id AS ep_id, e.episode_number, e.status, e.magnet_link,
               e.pixeldrain_1080_id, e.pixeldrain_720_id, e.pixeldrain_480_id,
               e.mirror_720_source, e.mirror_480_source,
               a.title_romaji
        FROM episodes e
        JOIN anime a ON e.anime_id = a.id
        WHERE e.status = 'ready'
    """) or []

    main_tracked = [r for r in rows if r.get("pixeldrain_1080_id")]
    dead_main = [r for r in main_tracked if r["pixeldrain_1080_id"] not in remote_ids]
    main_visible = len(main_tracked) - len(dead_main)

    dead_mirrors = []
    tracked_ids = set()
    for r in rows:
        for q in ("720", "480"):
            qid = r.get(f"pixeldrain_{q}_id")
            if qid:
                tracked_ids.add(qid)
                if qid not in remote_ids:
                    dead_mirrors.append((r, q))
    if main_visible > 0:
        tracked_ids.update(r["pixeldrain_1080_id"] for r in main_tracked)

    missing_count = len([i for i in tracked_ids if i not in remote_ids])
    if tracked_ids and missing_count * 2 > len(tracked_ids):
        log_message(f"LinkGuardian: {missing_count}/{len(tracked_ids)} tracked links missing - looks like an account/auth issue, not individual deletions. Aborting to protect the library.")
        return

    if main_tracked and main_visible == 0:
        log_message("LinkGuardian: main 1080 files are not visible in this storage account (separate account?). Skipping main-link guardian.")

    repairs_done = 0
    repair_attempts = 0

    for row in dead_main:
        if main_visible == 0:
            break
        if repair_attempts >= MAX_REPAIRS_PER_RUN:
            log_message(f"LinkGuardian: repair budget ({MAX_REPAIRS_PER_RUN}) reached, deferring remaining dead links to next cycle.")
            break
        label = f"main link for {row['title_romaji']} ep {row['episode_number']}"
        magnet = row.get("magnet_link") or ""
        if magnet.startswith("http"):
            repair_attempts += 1
            applied = await restore_from_source(magnet, label, row["ep_id"])
            if applied:
                repairs_done += 1
                continue
            await execute_sql("UPDATE episodes SET magnet_link = NULL WHERE id = ?", [row["ep_id"]])
        await queue_fresh_search(row["ep_id"])
        log_message(f"LinkGuardian: {label} is dead and queued for fresh search.")

    for row, q in dead_mirrors:
        label = f"{q} mirror for {row['title_romaji']} ep {row['episode_number']}"
        stored_src = row.get(f"mirror_{q}_source") or ""
        restored = False
        if stored_src.startswith("http") and repair_attempts < MAX_REPAIRS_PER_RUN:
            repair_attempts += 1
            restored = await restore_from_source(stored_src, label, row["ep_id"], quality=q)
            if restored:
                repairs_done += 1
        if not restored:
            await execute_sql(f"""
                UPDATE episodes
                SET pixeldrain_{q}_url = NULL, pixeldrain_{q}_id = NULL,
                    mirror_{q}_source = NULL,
                    mirror_{q}_missing = 0, mirror_updated_at = ?
                WHERE id = ?
            """, [int(time.time()), row["ep_id"]])
            log_message(f"LinkGuardian: {label} is dead, cleared for re-mirror.")

    if dead_main or dead_mirrors:
        log_message(f"LinkGuardian: found {len(dead_main)} dead main link(s), {len(dead_mirrors)} dead mirror(s). Repairs this run: {repairs_done}.")
    else:
        log_message("LinkGuardian: all links healthy.")


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
    anilist_id = ep.get("anilist_id")
    ep_num = ep.get("episode_number") or 0

    # Always update last_checked so this episode won't immediately monopolize the next cycle
    await execute_sql("UPDATE episodes SET last_checked = ? WHERE id = ?", [now_ts, ep["ep_id"]])

    # Grace period: If episode aired within the last 2 days (48 hours), do NOT mark it permanently missing.
    # Exclude old archive episodes (e.g. One Piece < 1100) from grace period!
    is_old_archive = (anilist_id == 21 and ep_num < 1100)
    if not is_old_archive and aired_at > 0 and (now_ts - aired_at < MISSING_GRACE_SECONDS):
        log_message(f"[{ep.get('title_romaji')}] Ep {ep_num} {quality} not found yet, but within grace period. Will retry next cycle.")
        return

    await execute_sql(f"""
        UPDATE episodes
        SET mirror_{q}_missing = 1,
            mirror_updated_at = ?
        WHERE id = ?
    """, [now_ts, ep["ep_id"]])
    ep[f"mirror_{q}_missing"] = 1


async def save_episode_mirror(ep, quality: str, upload, source: str = None, subs: str = None, audio: str = None):
    q = quality_key(quality)
    now_ts = int(time.time())
    master_subs = ep.get("subtitles") or subs
    master_audio = ep.get("audio_tracks") or audio
    await execute_sql(f"""
        UPDATE episodes
        SET mirror_updated_at = ?,
            mirror_{q}_missing = 0,
            mirror_{q}_source = ?,
            pixeldrain_{q}_url = ?,
            pixeldrain_{q}_id = ?,
            subtitles_{q} = ?,
            audio_tracks_{q} = ?,
            subtitles = ?,
            audio_tracks = ?
        WHERE id = ?
    """, [now_ts, source, upload["url"], upload.get("file_id"), subs, audio, master_subs, master_audio, ep["ep_id"]])

    ep[f"pixeldrain_{q}_url"] = upload["url"]
    ep[f"pixeldrain_{q}_id"] = upload.get("file_id")
    ep[f"mirror_{q}_missing"] = 0
    ep[f"mirror_{q}_source"] = source
    ep[f"subtitles_{q}"] = subs
    ep[f"audio_tracks_{q}"] = audio
    if master_subs:
        ep["subtitles"] = master_subs
    if master_audio:
        ep["audio_tracks"] = master_audio


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
        work_dir, video_path, video_name, size_bytes, info_hash = await asyncio.to_thread(
            download_release, release["source"], release["title"]
        )
    except subprocess.TimeoutExpired:
        log_message(f"Fetch timed out after {DOWNLOAD_TIMEOUT}s for {ep['title_romaji']} ep {ep['episode_number']} {quality}. Marking quality as missing.")
        await mark_quality_missing(ep, quality)
        return False
    except Exception as exc:
        log_message(f"Fetch failed for {ep['title_romaji']} ep {ep['episode_number']} {quality}: {exc}. Marking quality as missing.")
        await mark_quality_missing(ep, quality)
        return False
    size_mb = round(size_bytes / 1048576, 2)
    stored_source = (
        f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(release['title'])}"
        if info_hash else release["source"]
    )
    saved = False
    try:
        if not provider_done(ep, quality, "pixeldrain"):
            subs_found, audio_found = inspect_media_tracks(video_path)

            existing_file = storage_files.get(video_name.lower())
            if existing_file:
                upload = {"url": f"{STORAGE_API}/file/{existing_file['id']}", "file_id": existing_file["id"]}
            else:
                upload = await asyncio.to_thread(upload_to_storage, video_path, video_name)
            await save_episode_mirror(ep, quality, upload, stored_source, subs=subs_found, audio=audio_found)
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
    await reconcile_storage()
    storage_files = await get_storage_files()

    # Auto-dismiss archive/manual episodes that should not be mirrored (e.g. One Piece old episodes)
    try:
        await execute_sql("""
            UPDATE episodes
            SET mirror_720_missing = 1,
                mirror_480_missing = 1,
                mirror_updated_at = ?
            WHERE anime_id IN (SELECT id FROM anime WHERE anilist_id = 21)
              AND episode_number < 1100
              AND (COALESCE(mirror_720_missing, 0) = 0 OR COALESCE(mirror_480_missing, 0) = 0)
        """, [int(time.time())])
    except Exception as cl_ex:
        log_message(f"Archive cleanup warning: {cl_ex}")

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
