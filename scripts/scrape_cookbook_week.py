import argparse
import csv
import json
import re
import sys
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag
import time


COOKBOOK_BASE_URL = "https://cookbook.openai.com/"
DEFAULT_OUT_RECENT = str(Path("data") / "openai_cookbook_last_7_days.csv")
DEFAULT_OUT_ALL = str(Path("data") / "openai_cookbook_all.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OpenAI Cookbook posts (recent or all) and save as CSV."
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["recent", "all"],
        default="recent",
        help="recent: 최근 N일, all: 전체 글 (기본: recent)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days back from now to include (default: 7)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="YYYY-MM-DD 형식. all 모드/크롤 시 이 날짜 이후 글만 포함.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=DEFAULT_OUT_RECENT,
        help="Output CSV path (default: data/openai_cookbook_last_7_days.csv)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=COOKBOOK_BASE_URL,
        help="Cookbook root URL (default: https://cookbook.openai.com/)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2000,
        help="크롤 시 최대 방문 페이지 수 (기본 2000)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="크롤 시 최대 깊이 (기본 4)",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="크롤 진행상황을 주기적으로 STDERR로 출력",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=25,
        help="진행 로그 출력 주기(방문 페이지 기준, 기본 25)",
    )
    # recent 모드 가속 옵션: 기존 ALL CSV에서 필터링만
    parser.add_argument(
        "--recent-source",
        type=str,
        choices=["crawl", "from_all_csv", "auto"],
        default="crawl",
        help="recent 수집 소스: crawl(기본) | from_all_csv(ALL CSV에서 필터) | auto(둘 다 병합)",
    )
    parser.add_argument(
        "--all-csv",
        type=str,
        default=str(Path("data") / "openai_cookbook_all.csv"),
        help="--recent-source from_all_csv 사용 시 참조할 ALL CSV 경로",
    )
    args = parser.parse_args()
    # If mode is 'all' and user did not override out path, switch to all default
    if args.mode == "all" and args.out == DEFAULT_OUT_RECENT:
        args.out = DEFAULT_OUT_ALL
    return args


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        }
    )
    return session


def fetch_html(session: requests.Session, url: str, timeout: int = 20) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


DATE_PATTERN = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b",
    re.IGNORECASE,
)


def sanitize_title(raw_title: str) -> str:
    """Remove trailing/inline date fragments like '... Aug 29, 2025' from titles.

    Also trims doubled spaces.
    """
    t = (raw_title or "").strip()
    # Remove trailing date even if attached without space before month
    t = re.sub(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\s*$", "", t, flags=re.IGNORECASE)
    # Remove inline ' - OpenAI Cookbook' suffix if present (extra safety)
    t = re.sub(r"\s*-\s*OpenAI Cookbook\s*$", "", t, flags=re.IGNORECASE)
    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t).strip()

    # Deduplicate accidental repeated tail phrase even if concatenated without separator
    # e.g., '... Agents SDKAgents SDK' -> '... Agents SDK'
    # Match a tail phrase of 2-8 words repeated twice at end of string
    repeated_tail_pattern = re.compile(
        r"^(?P<head>.*?)(?P<phrase>(?:[A-Za-z][A-Za-z0-9\-]*\s+){1,7}[A-Za-z0-9\-]+)\2$"
    )
    m = repeated_tail_pattern.match(t)
    if m:
        t = (m.group("head") + m.group("phrase")).strip()

    # Also handle generic repeated suffix on character basis (guarded length)
    max_k = min(80, len(t) // 2)
    for k in range(max_k, 5, -1):
        half = t[-2 * k : -k]
        if half and t.endswith(half + half):
            t = t[:-k]
            break
    return t


def try_parse_date(date_str: str) -> Optional[datetime]:
    date_str = date_str.strip()
    # Normalize month capitalization (e.g., aug -> Aug)
    try:
        parsed = datetime.strptime(date_str.title(), "%b %d, %Y")
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def try_parse_iso8601(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    s = date_str.strip()
    try:
        # Handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_since_date(since_str: Optional[str]) -> Optional[datetime]:
    if not since_str:
        return None
    try:
        dt = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def is_within_days(target_dt: datetime, now_utc: datetime, days: int) -> bool:
    window_start = now_utc - timedelta(days=days)
    # Include boundary (>= window_start)
    return target_dt >= window_start


def get_nearest_anchor(node: Tag) -> Optional[Tag]:
    # 1) If node itself contains an anchor descendant
    anchor = node.find("a", href=True)
    if anchor:
        return anchor
    # 2) Search siblings
    for sibling in list(node.previous_siblings) + list(node.next_siblings):
        if isinstance(sibling, Tag):
            a = sibling.find("a", href=True)
            if a:
                return a
    # 3) Climb up a few ancestors and search within
    parent = node.parent
    climb = 0
    while parent is not None and climb < 4:
        a = parent.find("a", href=True)
        if a:
            return a
        parent = parent.parent
        climb += 1
    return None


def normalize_url(href: str, base_url: str) -> str:
    return urljoin(base_url, href)


def extract_posts(html: str, base_url: str, now_utc: datetime, days: int) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")

    results: List[Dict[str, str]] = []
    seen_urls: set[str] = set()

    for text_node in soup.find_all(string=DATE_PATTERN):
        date_match = DATE_PATTERN.search(str(text_node))
        if not date_match:
            continue

        date_str = date_match.group(0)
        parsed_dt = try_parse_date(date_str)
        if not parsed_dt:
            continue

        if not is_within_days(parsed_dt, now_utc, days):
            continue

        container: Optional[Tag] = text_node.parent if isinstance(text_node.parent, Tag) else None
        if not container:
            continue

        anchor = get_nearest_anchor(container)
        if not anchor:
            continue

        href = anchor.get("href", "").strip()
        if not href:
            continue

        url = normalize_url(href, base_url)
        title = anchor.get_text(strip=True)

        if not title:
            # Try using title attribute or fallback
            title = anchor.get("title") or ""

        if not title:
            # As a last resort, use the URL path as title
            title = url.rstrip("/").split("/")[-1].replace("-", " ").title()

        # Sanitize title (remove inline dates etc.)
        title = sanitize_title(title)

        if url in seen_urls:
            continue
        seen_urls.add(url)

        results.append(
            {
                "date": parsed_dt.date().isoformat(),
                "title": title,
                "url": url,
            }
        )

    # Sort by date desc, then title
    results.sort(key=lambda x: (x["date"], x["title"].lower()), reverse=True)
    return results


def parse_page_metadata(html: str) -> Tuple[Optional[str], Optional[datetime]]:
    soup = BeautifulSoup(html, "lxml")
    # Title preference: h1 > title > fallback
    title_text: Optional[str] = None
    h1 = soup.find(["h1", "h2"], string=True)
    if h1 and isinstance(h1, Tag):
        title_text = sanitize_title(h1.get_text(strip=True))
    if not title_text:
        title_tag = soup.find("title")
        if title_tag and title_tag.string:
            raw_title = title_tag.string.strip()
            title_text = sanitize_title(raw_title)
    if title_text:
        title_text = sanitize_title(title_text.strip())

    # Date candidates: meta tags, JSON-LD, visible text pattern
    page_dt: Optional[datetime] = None

    # meta article:published_time, og:updated_time, date
    meta_candidates = [
        ("property", "article:published_time"),
        ("property", "article:modified_time"),
        ("property", "og:updated_time"),
        ("name", "date"),
        ("itemprop", "datePublished"),
    ]
    for attr, val in meta_candidates:
        tag = soup.find("meta", attrs={attr: val})
        if tag and tag.get("content"):
            page_dt = try_parse_iso8601(tag.get("content")) or page_dt
            if page_dt:
                break

    if not page_dt:
        # JSON-LD
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.get_text(strip=True) or "{}")
            except Exception:
                continue
            # Could be dict or list
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict):
                    for key in ["datePublished", "dateModified"]:
                        page_dt = try_parse_iso8601(obj.get(key)) or page_dt
                        if page_dt:
                            break
                if page_dt:
                    break
            if page_dt:
                break

    if not page_dt:
        text_with_dates = soup.get_text(" ")
        date_match = DATE_PATTERN.search(text_with_dates)
        if date_match:
            page_dt = try_parse_date(date_match.group(0))

    return title_text, page_dt


def extract_posts_from_next_data(html: str, base_url: str) -> List[Dict[str, str]]:
    """Parse __NEXT_DATA__ (if present) to extract posts list with title, url, date.

    This is heuristic and searches for objects that look like posts.
    """
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return []
    try:
        data = json.loads(script.string)
    except Exception:
        return []

    results: List[Dict[str, str]] = []

    def build_url_from_item(item: Dict[str, object]) -> Optional[str]:
        href = item.get("href") or item.get("url") or item.get("path") or item.get("slug")
        if isinstance(href, str) and href:
            return urljoin(base_url, href)
        return None

    def parse_date_from_item(item: Dict[str, object]) -> Optional[datetime]:
        for key in [
            "date",
            "publishedAt",
            "published_at",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
            "datePublished",
            "dateModified",
            "lastmod",
        ]:
            val = item.get(key)
            if isinstance(val, str):
                dt = try_parse_iso8601(val)
                if dt:
                    return dt
        return None

    def walk(obj: object) -> None:
        if isinstance(obj, dict):
            # Heuristic: looks like a post-like object
            title = obj.get("title")
            if isinstance(title, str) and title.strip():
                url = build_url_from_item(obj)
                if url:
                    dt = parse_date_from_item(obj)
                    title_clean = title.strip()
                    if not dt:
                        # Try to extract date embedded in title and strip it out
                        m = DATE_PATTERN.search(title_clean)
                        if m:
                            dt = try_parse_date(m.group(0))
                            # remove that substring from title
                            title_clean = title_clean.replace(m.group(0), "").strip()
                    title_clean = sanitize_title(title_clean)

                    results.append(
                        {
                            "date": (dt.date().isoformat() if dt else ""),
                            "title": title_clean,
                            "url": url,
                        }
                    )
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(data)

    # Deduplicate by URL and prefer rows with a date
    url_to_row: Dict[str, Dict[str, str]] = {}
    for r in results:
        u = r["url"]
        if u not in url_to_row:
            url_to_row[u] = r
        else:
            if r.get("date") and not url_to_row[u].get("date"):
                url_to_row[u] = r

    final_rows = list(url_to_row.values())
    final_rows.sort(key=lambda x: (x["date"] or "0000-01-01", x["title"].lower()))
    return final_rows


ASSET_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".svg",
    ".gif",
    ".webp",
    ".ico",
    ".css",
    ".js",
    ".mjs",
    ".json",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".mp4",
    ".webm",
)


def clean_url(url: str) -> str:
    # remove fragments and query for dedup
    parts = urlparse(url)
    clean = parts._replace(fragment="", query="")
    return urlunparse(clean)


def is_internal_url(url: str, base_netloc: str) -> bool:
    parts = urlparse(url)
    return (not parts.netloc) or (parts.netloc == base_netloc)


def should_skip_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(ASSET_EXTENSIONS)


def canonicalize_post_url(url: str) -> str:
    """Normalize URLs to avoid duplicates (e.g., http vs https, trailing slashes)."""
    parts = urlparse(url)
    scheme = "https"
    netloc = parts.netloc or "cookbook.openai.com"
    # Normalize host
    if netloc.endswith("cookbook.openai.com"):
        netloc = "cookbook.openai.com"
    path = parts.path.rstrip("/") or "/"
    return urlunparse((scheme, netloc, path, "", "", ""))


def _parse_date_safe(date_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def canonicalize_title(title: str) -> str:
    t = sanitize_title((title or "").strip()).lower()
    t = re.sub(r"\s+", " ", t)
    return t


def dedupe_rows_by_title(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Deduplicate rows by canonical title, preferring newer dated entries and non-empty titles."""
    title_to_row: Dict[str, Dict[str, str]] = {}

    def score(row: Dict[str, str]) -> tuple:
        ds = (row.get("date") or "").strip()
        dt = _parse_date_safe(ds)
        has_date = 1 if dt else 0
        date_key = dt.isoformat() if dt else ""
        title_len = len((row.get("title") or "").strip())
        # Prefer newer and shorter (clean) titles when deduping
        return (has_date, date_key, -title_len)

    for r in rows:
        title_key = canonicalize_title(r.get("title") or "")
        if not title_key:
            # If title missing, fallback to URL-based key to avoid dropping content entirely
            title_key = canonicalize_title(r.get("url") or "")
        prev = title_to_row.get(title_key)
        if not prev or score(r) > score(prev):
            title_to_row[title_key] = r

    deduped = list(title_to_row.values())
    deduped.sort(key=lambda x: (x.get("date") or "0000-01-01", (x.get("title") or "").lower()), reverse=True)
    return deduped


def dedupe_rows_by_url_prefer_clean_title(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Final safety: dedupe rows by URL, preferring rows with a date and shorter (clean) titles."""
    url_to_row: Dict[str, Dict[str, str]] = {}
    def score(row: Dict[str, str]) -> tuple:
        ds = (row.get("date") or "").strip()
        dt = _parse_date_safe(ds)
        has_date = 1 if dt else 0
        date_key = dt.isoformat() if dt else ""
        title_len = len((row.get("title") or "").strip())
        return (has_date, date_key, -title_len)
    for r in rows:
        u = (r.get("url") or "").strip()
        if not u:
            continue
        prev = url_to_row.get(u)
        if not prev or score(r) > score(prev):
            # Ensure title is sanitized in case caller forgot
            r["title"] = sanitize_title(r.get("title") or "")
            url_to_row[u] = r
    return list(url_to_row.values())


def crawl_site_and_collect(
    session: requests.Session,
    base_url: str,
    max_pages: int,
    max_depth: int,
    since_dt: Optional[datetime],
    progress: bool = False,
    progress_interval: int = 25,
) -> List[Dict[str, str]]:
    start_urls = [base_url]
    # Add common hub pages that list many posts
    for suffix in [
        "examples/",
        "articles/",
    ]:
        start_urls.append(urljoin(base_url, suffix))

    base_netloc = urlparse(base_url).netloc
    visited: set[str] = set()
    queue: deque[Tuple[str, int]] = deque()
    for u in start_urls:
        queue.append((clean_url(u), 0))

    rows: List[Dict[str, str]] = []
    processed = 0
    start_ts = time.time()
    while queue and len(visited) < max_pages:
        current_url, depth = queue.popleft()
        if current_url in visited:
            continue
        visited.add(current_url)
        processed += 1

        if should_skip_url(current_url):
            continue

        try:
            html = fetch_html(session, current_url)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] fetch fail {current_url}: {exc}", file=sys.stderr)
            continue

        title_text, page_dt = parse_page_metadata(html)

        final_date_str = page_dt.date().isoformat() if page_dt else ""
        if since_dt is None or (page_dt and page_dt >= since_dt):
            rows.append(
                {
                    "date": final_date_str,
                    "title": (title_text or current_url.rstrip("/").split("/")[-1].replace("-", " ").title()),
                    "url": current_url,
                }
            )

        if depth < max_depth:
            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a.get("href", "").strip()
                if not href or href.startswith("#"):
                    continue
                next_url = clean_url(urljoin(current_url, href))
                if not is_internal_url(next_url, base_netloc):
                    continue
                if should_skip_url(next_url):
                    continue
                if next_url in visited:
                    continue
                queue.append((next_url, depth + 1))

        if progress and (processed % max(1, progress_interval) == 0):
            elapsed = max(1e-6, time.time() - start_ts)
            rate = processed / elapsed  # pages/sec
            remaining = len(queue)
            eta_secs = int(remaining / max(1e-6, rate))
            eta_min, eta_sec = divmod(eta_secs, 60)
            eta_hr, eta_min = divmod(eta_min, 60)
            pct = f"{(len(visited)/max(1, max_pages))*100:.1f}%" if max_pages > 0 else "-"
            print(
                f"[PROGRESS] visited={len(visited)} queued={len(queue)} collected={len(rows)} depth={depth} rate={rate:.2f}/s ETA={eta_hr:02d}:{eta_min:02d}:{eta_sec:02d} ({pct})",
                file=sys.stderr,
            )

    # Deduplicate rows by URL and keep earliest date when duplicates
    url_to_row: Dict[str, Dict[str, str]] = {}
    for row in rows:
        prev = url_to_row.get(row["url"])  # type: ignore[index]
        if not prev:
            url_to_row[row["url"]] = row
        else:
            # Keep the earliest non-empty date if available
            if row.get("date") and ((not prev.get("date")) or row["date"] < prev["date"]):
                url_to_row[row["url"]] = row

    final_rows = list(url_to_row.values())
    final_rows.sort(key=lambda x: (x["date"] or "0000-01-01", x["title"].lower()))
    return final_rows


def fetch_all_urls_from_sitemap(session: requests.Session, base_url: str, max_sitemaps: int = 50) -> List[Tuple[str, Optional[datetime]]]:
    sitemap_url = urljoin(base_url, "sitemap.xml")
    try:
        xml_text = fetch_html(session, sitemap_url)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Failed to fetch sitemap: {sitemap_url}: {exc}", file=sys.stderr)
        return []

    def parse_sitemap(xml: str) -> Tuple[List[str], List[Tuple[str, Optional[str]]]]:
        doc = BeautifulSoup(xml, "xml")
        sitemap_nodes = doc.find_all("sitemap")
        if sitemap_nodes:
            sitemap_urls = [n.find_text("loc") for n in sitemap_nodes if n.find("loc")]
            return sitemap_urls, []
        # urlset
        url_nodes = doc.find_all("url")
        entries: List[Tuple[str, Optional[str]]] = []
        for n in url_nodes:
            loc_tag = n.find("loc")
            if not loc_tag or not loc_tag.text:
                continue
            loc = loc_tag.text.strip()
            lastmod_tag = n.find("lastmod")
            lastmod_text = lastmod_tag.text.strip() if lastmod_tag and lastmod_tag.text else None
            entries.append((loc, lastmod_text))
        return [], entries

    sitemap_urls, url_entries = parse_sitemap(xml_text)
    all_entries: List[Tuple[str, Optional[str]]] = []
    if sitemap_urls:
        for i, sm_url in enumerate(sitemap_urls[:max_sitemaps]):
            try:
                sm_xml = fetch_html(session, sm_url)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] Failed to fetch sub-sitemap {sm_url}: {exc}", file=sys.stderr)
                continue
            _, entries = parse_sitemap(sm_xml)
            all_entries.extend(entries)
    else:
        all_entries = url_entries

    # Deduplicate by URL, keep latest lastmod when duplicates
    loc_to_lastmod: Dict[str, Optional[str]] = {}
    for loc, lastmod in all_entries:
        if loc not in loc_to_lastmod or (lastmod and (loc_to_lastmod[loc] or "") < lastmod):
            loc_to_lastmod[loc] = lastmod

    result: List[Tuple[str, Optional[datetime]]] = []
    for loc, lastmod in loc_to_lastmod.items():
        result.append((loc, try_parse_iso8601(lastmod)))
    # Sort ascending by lastmod if present, otherwise by URL
    result.sort(key=lambda t: (t[1] or datetime.min.replace(tzinfo=timezone.utc), t[0]))
    return result


def ensure_parent_dir(path: Path) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(rows: Iterable[Dict[str, str]], out_path: Path) -> Tuple[int, Path]:
    ensure_parent_dir(out_path)
    fieldnames = ["date", "title", "url"]
    count = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
            count += 1
    return count, out_path


def read_csv_rows(in_path: Path) -> List[Dict[str, str]]:
    if not in_path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with in_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def collect_recent_posts(
    days: int,
    out: Optional[str],
    base_url: str = COOKBOOK_BASE_URL,
    recent_source: str = "auto",
    all_csv: Optional[str] = None,
    max_pages: int = 2000,
    max_depth: int = 4,
    progress: bool = False,
    progress_interval: int = 25,
) -> Tuple[int, Path, List[Dict[str, str]]]:
    """High-level recent collector used by external tools.

    Returns: (count, saved_path, rows)
    """
    session = create_session()
    base_url = base_url.rstrip("/") + "/"
    now_utc = datetime.now(timezone.utc)
    since_dt = now_utc - timedelta(days=days)

    rows: List[Dict[str, str]]
    def from_all_csv() -> List[Dict[str, str]]:
        all_path = Path(all_csv or DEFAULT_OUT_ALL)
        rows_all = read_csv_rows(all_path)
        rows_local: List[Dict[str, str]] = []
        for r in rows_all:
            ds = (r.get("date") or "").strip()
            if not ds:
                continue
            try:
                rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if rdt >= since_dt:
                r["title"] = sanitize_title(r.get("title") or "")
                rows_local.append(r)
        return dedupe_rows_by_title(rows_local)

    def from_crawl() -> List[Dict[str, str]]:
        html = fetch_html(session, base_url)
        posts_home = extract_posts(html, base_url, now_utc, days)
        next_rows = extract_posts_from_next_data(html, base_url)
        filtered_next: List[Dict[str, str]] = []
        for r in next_rows:
            ds = r.get("date") or ""
            keep = True
            if ds:
                try:
                    rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    keep = rdt >= since_dt
                except Exception:
                    keep = True
            if keep:
                r["title"] = sanitize_title(r.get("title") or "")
                filtered_next.append(r)

        crawl_rows = crawl_site_and_collect(
            session=session,
            base_url=base_url,
            max_pages=max_pages,
            max_depth=max_depth,
            since_dt=since_dt,
            progress=progress,
            progress_interval=progress_interval,
        )
        combined = posts_home + filtered_next + crawl_rows

        for r in combined:
            if not (r.get("date") or "").strip():
                try:
                    page_html = fetch_html(session, r.get("url") or base_url)
                    t_text, p_dt = parse_page_metadata(page_html)
                    if p_dt:
                        r["date"] = p_dt.date().isoformat()
                    if (not r.get("title")) and t_text:
                        r["title"] = sanitize_title(t_text)
                except Exception:
                    pass

        final_rows = dedupe_rows_by_title(combined)
        # final filter
        def _keep_since(row: Dict[str, str]) -> bool:
            ds = (row.get("date") or "").strip()
            if not ds:
                return False
            try:
                rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return rdt >= since_dt
            except Exception:
                return False

        final_rows = [r for r in final_rows if _keep_since(r)]
        return final_rows

    if recent_source == "from_all_csv":
        final_rows = from_all_csv()
    elif recent_source == "crawl":
        final_rows = from_crawl()
    else:  # auto -> merge both and dedupe
        set_all = from_all_csv()
        set_crawl = from_crawl()
        final_rows = dedupe_rows_by_title(set_all + set_crawl)

    # Final safety: dedupe by URL as well (prefer clean title)
    final_rows = dedupe_rows_by_url_prefer_clean_title(final_rows)

    out_path = Path(out or (Path("data") / f"openai_cookbook_last_{days}_days.csv"))
    count, saved_path = write_csv(final_rows, out_path)
    return count, saved_path, final_rows


def main() -> None:
    args = parse_args()
    base_url = args.base_url.rstrip("/") + "/"

    session = create_session()
    if args.mode == "recent":
        if getattr(args, "recent_source", "crawl") == "from_all_csv":
            # Fast path: filter from existing ALL CSV
            all_path = Path(args.all_csv)
            rows = read_csv_rows(all_path)
            now_utc = datetime.now(timezone.utc)
            since_dt = now_utc - timedelta(days=args.days)
            filtered: List[Dict[str, str]] = []
            for r in rows:
                ds = (r.get("date") or "").strip()
                if not ds:
                    continue
                try:
                    rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    continue
                if rdt >= since_dt:
                    r["title"] = sanitize_title(r.get("title") or "")
                    filtered.append(r)
            final_rows = dedupe_rows_by_title(filtered)
            out_path = Path(args.out)
            count, saved_path = write_csv(final_rows, out_path)
            print(
                f"Saved {count} posts from the last {args.days} days to {saved_path.as_posix()} (from_all_csv)"
            )
            return
        try:
            html = fetch_html(session, base_url)
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] Failed to fetch {base_url}: {exc}", file=sys.stderr)
            sys.exit(1)

        now_utc = datetime.now(timezone.utc)
        since_dt = now_utc - timedelta(days=args.days)

        # 1) Homepage heuristic (existing)
        posts_home = extract_posts(html, base_url, now_utc, args.days)

        # 2) __NEXT_DATA__ extraction from homepage
        next_rows = extract_posts_from_next_data(html, base_url)
        filtered_next: List[Dict[str, str]] = []
        for r in next_rows:
            ds = r.get("date") or ""
            keep = True
            if ds:
                try:
                    rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    keep = rdt >= since_dt
                except Exception:
                    keep = True
            if keep:
                # sanitize title in case next data embeds date
                r["title"] = sanitize_title(r.get("title") or "")
                filtered_next.append(r)

        # 3) Limited crawl with since filter to catch posts not visible on homepage
        crawl_rows = crawl_site_and_collect(
            session=session,
            base_url=base_url,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            since_dt=since_dt,
            progress=getattr(args, "progress", False),
            progress_interval=getattr(args, "progress_interval", 25),
        )

        combined = posts_home + filtered_next + crawl_rows

        # 4) For rows without date, fetch page to populate date if possible
        for r in combined:
            if not (r.get("date") or "").strip():
                try:
                    page_html = fetch_html(session, r.get("url") or base_url)
                    t_text, p_dt = parse_page_metadata(page_html)
                    if p_dt:
                        r["date"] = p_dt.date().isoformat()
                    if (not r.get("title")) and t_text:
                        r["title"] = sanitize_title(t_text)
                except Exception:
                    pass

        # Dedupe by title and re-filter by since just in case
        final_rows = dedupe_rows_by_title(combined)
        def _keep_since(row: Dict[str, str]) -> bool:
            ds = (row.get("date") or "").strip()
            if not ds:
                return False
            try:
                rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return rdt >= since_dt
            except Exception:
                return False
        final_rows = [r for r in final_rows if _keep_since(r)]

        out_path = Path(args.out)
        count, saved_path = write_csv(final_rows, out_path)
        print(
            f"Saved {count} posts from the last {args.days} days to {saved_path.as_posix()}"
        )
    else:  # all
        url_items = fetch_all_urls_from_sitemap(session, base_url)
        rows: List[Dict[str, str]] = []
        if url_items:
            for url, lastmod_dt in url_items:
                try:
                    page_html = fetch_html(session, url)
                except Exception as exc:  # noqa: BLE001
                    print(f"[WARN] skip {url}: {exc}", file=sys.stderr)
                    continue
                title_text, page_dt = parse_page_metadata(page_html)
                final_dt = page_dt or lastmod_dt
                rows.append(
                    {
                        "date": (final_dt.date().isoformat() if final_dt else ""),
                        "title": (title_text or url.rstrip("/").split("/")[-1].replace("-", " ").title()),
                        "url": url,
                    }
                )
            # Dedupe by canonical title then sort desc
            rows = dedupe_rows_by_title(rows)
            out_path = Path(args.out)
            count, saved_path = write_csv(rows, out_path)
            print(f"Saved {count} posts (ALL) to {saved_path.as_posix()}")
        else:
            # Fallback: no sitemap available. Try extracting from homepage __NEXT_DATA__ first, then BFS crawl.
            print(
                "[INFO] sitemap.xml not found. Trying __NEXT_DATA__ then site crawl (BFS).",
                file=sys.stderr,
            )
            since_dt = parse_since_date(args.since)
            combined_rows: List[Dict[str, str]] = []
            try:
                home_html = fetch_html(session, base_url)
                next_rows = extract_posts_from_next_data(home_html, base_url)
                if since_dt:
                    filtered: List[Dict[str, str]] = []
                    for r in next_rows:
                        ds = r.get("date") or ""
                        keep = True
                        if ds:
                            try:
                                rdt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                                keep = rdt >= since_dt
                            except Exception:
                                keep = True
                        if keep:
                            filtered.append(r)
                    next_rows = filtered
                combined_rows.extend(next_rows)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] __NEXT_DATA__ parse failed: {exc}", file=sys.stderr)

            crawl_rows = crawl_site_and_collect(
                session=session,
                base_url=base_url,
                max_pages=args.max_pages,
                max_depth=args.max_depth,
                since_dt=since_dt,
                progress=getattr(args, "progress", False),
                progress_interval=getattr(args, "progress_interval", 25),
            )
            combined_rows.extend(crawl_rows)

            # Deduplicate by URL and prefer rows with a date and non-empty title
            url_to_row: Dict[str, Dict[str, str]] = {}
            for r in combined_rows:
                u = r.get("url") or ""
                if not u:
                    continue
                prev = url_to_row.get(u)
                if not prev:
                    url_to_row[u] = r
                else:
                    # Prefer row with date over no-date, and non-empty title
                    def score(x: Dict[str, str]) -> tuple:
                        return (1 if x.get("date") else 0, 1 if x.get("title") else 0)

                    if score(r) > score(prev):
                        url_to_row[u] = r

            final_rows = dedupe_rows_by_title(list(url_to_row.values()))

            out_path = Path(args.out)
            count, saved_path = write_csv(final_rows, out_path)
            print(
                f"Saved {count} posts (ALL-combined) to {saved_path.as_posix()}"
            )


if __name__ == "__main__":
    main()


