import os
from uuid import uuid4
from scripts.scrape_cookbook_week import collect_recent_posts
from pathlib import Path
from langchain_core.tools import tool

# LangSmith 설정
#unique_id = uuid4().hex[0:8]
unique_id = "2"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"OpenAI Cookbook New Posting Crawling - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)


def save_recent_cookbook_posts(days: int, out: str):
    """
    OpenAI Cookbook 홈에서 최근 N일 게시글을 크롤링해 CSV로 저장합니다.

    Args:
        days: 최근 N일
        out: 출력 CSV 경로 문자열

    Returns:
        (count, saved_path, posts): 저장된 행 개수, 실제 저장된 Path 객체, 수집된 게시글 리스트(dict)
    """
    count, saved_path, posts = collect_recent_posts(
        days=days,
        out=out,
        recent_source="auto",
        all_csv=str(Path("data") / "openai_cookbook_all.csv"),
    )
    print(f"Saved {count} posts from the last {days} days to {saved_path.as_posix()}")
    return count, saved_path, posts

@tool
def save_recent_cookbook_posts_tool(days: int, out: str):
    """
    OpenAI Cookbook 홈에서 최근 N일 게시글을 크롤링해 CSV로 저장합니다.

    Args:
        days: 최근 N일
        out: 출력 CSV 경로 문자열

    Returns:
        (count, saved_path, posts): 저장된 행 개수, 실제 저장된 Path 객체, 수집된 게시글 리스트(dict)
    """
    count, saved_path, posts = collect_recent_posts(
        days=days,
        out=out,
        recent_source="auto",
        all_csv=str(Path("data") / "openai_cookbook_all.csv"),
    )
    print(f"Saved {count} posts from the last {days} days to {saved_path.as_posix()}")
    return count, saved_path, posts


def save_cookbook_html_from_csv(csv_path: str = "data/openai_cookbook_last_14_days.csv", out_dir: str = "data/html"):
    """
    CSV의 URL을 순회하여 각 페이지의 실제 HTML을 `title.html` 파일로 저장합니다.

    Args:
        csv_path: `date,title,url` 헤더를 가진 CSV 경로. 기본값은 최근 14일 파일
        out_dir: 결과 HTML 저장 폴더. 기본값은 `data/html`

    Returns:
        저장 결과 리스트. 각 항목은 {"title", "url", "saved_path"}를 포함합니다.
    """
    import csv as _csv
    import re as _re
    import requests as _requests
    from pathlib import Path as _Path

    def _sanitize(filename: str) -> str:
        if not filename:
            return uuid4().hex
        name = _re.sub(r'[<>:\"/\\|?*]+', " ", filename)
        name = _re.sub(r"\s+", " ", name).strip()
        if not name:
            name = uuid4().hex
        max_len = 150
        if len(name) > max_len:
            name = name[:max_len].rstrip()
        return name

    csv_file = _Path(csv_path)
    output_dir = _Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    with csv_file.open("r", encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            title = (row.get("title") or "").strip()
            url = (row.get("url") or "").strip()
            if not url:
                continue

            safe_title = _sanitize(title)
            candidate_path = output_dir / f"{safe_title}.html"
            if candidate_path.exists():
                print(f"이미 HTML 파일이 존재합니다. (Skip): {candidate_path.as_posix()}")
                continue
            save_path = candidate_path

            try:
                resp = _requests.get(
                    url,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/127.0.0.0 Safari/537.36"
                        )
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                html_text = resp.text
            except Exception as e:
                print(f"Failed to fetch HTML for '{title}': {e}")
                continue

            try:
                with save_path.open("w", encoding="utf-8", newline="") as wf:
                    wf.write(html_text)
                results.append({
                    "title": title,
                    "url": url,
                    "saved_path": save_path.as_posix(),
                })
                print(f"Saved HTML: {save_path.as_posix()}")
            except Exception as e:
                print(f"Failed to save HTML for '{title}' to {save_path}: {e}")

    return results

def main():
    # 그냥 함수로 호출
    # count, saved_path, posts = save_recent_cookbook_posts(14, "data/openai_cookbook_last_14_days.csv")
    # print(posts)

    # LangGraph Tool로 호출
    print(save_recent_cookbook_posts_tool.name)
    print(save_recent_cookbook_posts_tool.description)
    print(save_recent_cookbook_posts_tool.args)

    posts = save_recent_cookbook_posts_tool.invoke({"days":14, "out":"data/openai_cookbook_last_14_days.csv"})
    print(posts)

    # CSV 파일에서 HTML 파일로 저장
    save_cookbook_html_from_csv(csv_path="data/openai_cookbook_last_14_days.csv", out_dir="data/html")

if __name__ == "__main__":
    main()


