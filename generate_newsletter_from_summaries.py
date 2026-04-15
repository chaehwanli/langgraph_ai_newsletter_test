from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def parse_summary_markdown(summary_path: Path) -> tuple[str, str]:
    """Extract title and summary from a markdown file.

    - Remove metadata lines like '원본 파일:', '원본 제목', '번역 제목'
    - Title: first heading line if present, otherwise filename stem
    - Summary: remaining non-empty, non-metadata lines joined as one paragraph
    """
    raw = read_text(summary_path)
    lines = [line.strip() for line in raw.splitlines()]

    # Filter out metadata lines (handles labels with optional parentheses, e.g., '원본 제목(영문):')
    # Also drop source link lines like '원본 URL:' or '원본 링크:' so summaries start directly.
    meta_pattern = re.compile(
        r"^(원본\s*파일|원본\s*제목(?:\([^)]*\))?|번역\s*제목(?:\([^)]*\))?|원본\s*URL|원본\s*링크|원본\s*주소)\s*:\s*",
        re.IGNORECASE,
    )
    filtered = [ln for ln in lines if ln and not meta_pattern.match(ln)]

    if not filtered:
        return (summary_path.stem, "")

    # Determine title from first heading if it exists
    if filtered and filtered[0].startswith("#"):
        title = re.sub(r"^#+\s*", "", filtered[0]).strip()
        content_lines = filtered[1:]
    else:
        title = summary_path.stem
        content_lines = filtered

    summary = " ".join(content_lines).strip()
    return (title, summary)


def extract_titles_from_md(summary_path: Path) -> tuple[str | None, str | None]:
    """Return (korean_title, english_title) if present in metadata lines.

    Matches patterns like:
    - 원본 제목(영문): <english>
    - 번역 제목(한글): <korean>
    Parenthetical hints are optional and ignored.
    """
    raw = read_text(summary_path)
    kr = None
    en = None
    re_en = re.compile(r"^원본\s*제목(?:\([^)]*\))?\s*:\s*(.+)$", re.IGNORECASE)
    re_kr = re.compile(r"^번역\s*제목(?:\([^)]*\))?\s*:\s*(.+)$", re.IGNORECASE)
    for line in raw.splitlines():
        line = line.strip()
        m_en = re_en.match(line)
        if m_en and not en:
            en = m_en.group(1).strip()
            continue
        m_kr = re_kr.match(line)
        if m_kr and not kr:
            kr = m_kr.group(1).strip()
            continue
    return (kr, en)


def normalize_title(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def find_summary_md_for_title(title: str, summary_dir: Path) -> Path | None:
    # Try exact filename first
    candidate = summary_dir / f"{title}_summary_ko.md"
    if candidate.exists():
        return candidate
    # Fallback: normalized match
    wanted = normalize_title(title)
    for path in summary_dir.glob("*_summary_ko.md"):
        base = path.name.replace("_summary_ko.md", "")
        if normalize_title(base) == wanted:
            return path
    return None


def build_placeholders(root: Path) -> dict[str, str]:
    summary_dir = root / "data" / "html_summary"
    # Pick the cookbook CSV with the smallest days suffix: openai_cookbook_last_{N}_days.csv
    def find_smallest_days_csv(base: Path) -> Path | None:
        data_dir = base / "data"
        smallest: tuple[int, Path] | None = None
        for p in data_dir.glob("openai_cookbook_last_*_days.csv"):
            m = re.search(r"openai_cookbook_last_(\d+)_days\.csv$", p.name)
            if not m:
                continue
            days = int(m.group(1))
            if smallest is None or days < smallest[0]:
                smallest = (days, p)
        return smallest[1] if smallest else None

    csv_path = find_smallest_days_csv(root)

    rows: list[dict[str, str]] = []
    if csv_path and csv_path.exists():
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("title") or not row.get("url"):
                    continue
                rows.append({
                    "date": row.get("date", ""),
                    "title": row["title"].strip(),
                    "url": row["url"].strip(),
                })

    # Sort by date desc when possible
    def parse_date(d: str) -> tuple[int, int, int]:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            return (dt.year, dt.month, dt.day)
        except Exception:
            return (0, 0, 0)

    rows.sort(key=lambda r: parse_date(r.get("date", "")), reverse=True)

    items: list[tuple[str, str, str]] = []  # (title, summary, link)
    for r in rows:
        title_csv = r["title"]
        url_csv = r["url"]
        date_csv = r.get("date", "").strip()
        md_path = find_summary_md_for_title(title_csv, summary_dir)
        if not md_path:
            # Skip if there is no corresponding summary md
            continue
        # Extract summary and titles
        _, summary = parse_summary_markdown(md_path)
        kr_title, en_title_from_md = extract_titles_from_md(md_path)
        english_title = en_title_from_md or title_csv
        if kr_title and english_title:
            title_bilingual = f"{kr_title}({english_title})"
        else:
            title_bilingual = english_title or (kr_title or "")
        # Format headline with source tag and date (kept as requested earlier)
        headline = f"[OpenAI Cookbook] {title_bilingual} - {date_csv}" if title_bilingual else ""
        items.append((headline, summary, url_csv))
        if len(items) == 3:
            break

    today = datetime.now()

    placeholders: dict[str, str] = {
        "NEWSLETTER_DATE": today.strftime("%Y-%m-%d"),
        "YEAR": today.strftime("%Y"),
    }

    # Dynamic HTML for OpenAI Cookbook cards (no empty padding)
    cookbook_cards_html_parts: list[str] = []
    for title, summary, link in items:
        card_html = (
            '<div class="card">\n'
            f'  <h3>{title}</h3>\n'
            f'  <p>{summary}</p>\n'
            f'  <div style="padding:0 16px 16px 16px"><a class="btn" href="{link}" target="_blank" rel="noopener">자세히 보기</a></div>\n'
            '</div>\n'
        )
        cookbook_cards_html_parts.append(card_html)

    placeholders["COOKBOOK_HTML"] = "".join(cookbook_cards_html_parts)

    # ------------------------------
    # Add Hacker News TOP 5 (AI)
    # ------------------------------
    def load_latest_hn_rows(base: Path) -> list[dict[str, str]]:
        data_dir = base / "data"
        # pick latest by modified time to be robust against date suffix
        candidates = sorted(
            data_dir.glob("hacker_news_topstories_last_7_days_ai_top5_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return []

        latest = candidates[0]
        rows_hn: list[dict[str, str]] = []
        with latest.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = (row.get("title") or "").strip()
                url = (row.get("url") or "").strip()
                time_str = (row.get("time") or "").strip()
                if not title or not url:
                    continue
                rows_hn.append({"title": title, "url": url, "time": time_str})
        return rows_hn[:5]

    def find_hn_summary_md_for_title(title: str, summary_dir_hn: Path) -> Path | None:
        wanted = normalize_title(title)
        for path in summary_dir_hn.glob("*_summary_ko.md"):
            # Try metadata english title match first
            kr, en = extract_titles_from_md(path)
            if en and normalize_title(en) == wanted:
                return path
        # Fallback: try filename stem normalization (handles cases where slug mirrors title)
        for path in summary_dir_hn.glob("*_summary_ko.md"):
            base = path.name.replace("_summary_ko.md", "")
            if normalize_title(base) == wanted:
                return path
        return None

    hn_summary_dir = root / "data" / "hn_url_summary"
    hn_rows = load_latest_hn_rows(root)
    hn_items: list[tuple[str, str, str]] = []  # (title, summary, link)
    for r in hn_rows:
        title_csv = r["title"]
        url_csv = r["url"]
        time_csv = r.get("time", "")

        md_path = find_hn_summary_md_for_title(title_csv, hn_summary_dir)
        if not md_path:
            # If no matching summary markdown, skip this item
            continue

        # Extract summary and bilingual titles
        _, summary_hn = parse_summary_markdown(md_path)
        kr_title, en_title_from_md = extract_titles_from_md(md_path)
        english_title = en_title_from_md or title_csv
        if kr_title and english_title:
            title_bilingual = f"{kr_title}({english_title})"
        else:
            title_bilingual = english_title or (kr_title or "")

        # Use date part if available: 'YYYY-MM-DD' from 'YYYY-MM-DD HH:MM:SS'
        date_part = ""
        if time_csv:
            date_part = time_csv.split(" ")[0]

        headline_hn = (
            f"{title_bilingual} - {date_part}" if title_bilingual else ""
        )
        hn_items.append((headline_hn, summary_hn, url_csv))
        if len(hn_items) == 5:
            break

    # Dynamic rows HTML for HN (no empty padding)
    hn_rows_html_parts: list[str] = []
    for title, summary, link in hn_items:
        row_html = (
            "<tr>\n"
            "  <td>\n"
            f"    <strong>{title}</strong>\n"
            f"    <div style=\"margin:6px 0 0 0; color:#374151; font-size:14px; line-height:1.6\">{summary}</div>\n"
            f"    <div style=\"margin-top:8px\"><a class=\"btn\" href=\"{link}\" target=\"_blank\" rel=\"noopener\">자세히 보기</a></div>\n"
            "  </td>\n"
            "</tr>\n"
        )
        hn_rows_html_parts.append(row_html)

    placeholders["HN_ROWS_HTML"] = "".join(hn_rows_html_parts)

    return placeholders


def fill_template(template_html: str, placeholders: dict[str, str]) -> str:
    filled = template_html
    for key, value in placeholders.items():
        filled = filled.replace(f"{{{{{key}}}}}", value)
    return filled


def main() -> None:
    root = Path(__file__).parent.resolve()
    template_path = root / "newsletter" / "index.html"
    output_path = root / "newsletter" / "index_filled.html"

    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    placeholders = build_placeholders(root)
    template_html = read_text(template_path)
    filled_html = fill_template(template_html, placeholders)

    write_text(output_path, filled_html)
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()


