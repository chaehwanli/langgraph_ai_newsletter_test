import os
import csv
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, ConnectionError, ReadTimeout, ChunkedEncodingError, RequestException
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

BASE_URL = "https://hacker-news.firebaseio.com/v0/"

_SESSION: Optional[requests.Session] = None


def _get_retry_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    session = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.8,  # 0.8, 1.6, 3.2, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "hn-ai-newsletter/1.0 (+https://github.com/)",
        "Accept": "application/json",
    })
    _SESSION = session
    return session

def fetch_data(endpoint: str):
    """
    Hacker News API 엔드포인트에서 JSON 데이터를 가져옵니다.

    Args:
        endpoint: BASE_URL 뒤에 붙는 경로 (예: "topstories.json")

    Returns:
        파싱된 JSON 객체

    Raises:
        Exception: 상태 코드가 200이 아닌 경우
    """
    url = f"{BASE_URL}{endpoint}"
    session = _get_retry_session()

    # 수동 재시도(어댑터 재시도와 별개로 SSLEOFError 등 케이스 보강)
    last_exc: Optional[Exception] = None
    for attempt in range(6):
        try:
            # (connect timeout, read timeout)
            resp = session.get(url, timeout=(5, 25))
            if resp.status_code == 200:
                return resp.json()
            # 4xx/5xx 비정상 응답은 어댑터 재시도가 처리하나, 여기서는 최종 실패 시 None
            last_exc = Exception(f"HTTP {resp.status_code} for {url}")
        except (SSLError, ConnectionError, ReadTimeout, ChunkedEncodingError, RequestException) as e:
            last_exc = e

        # 지수 백오프 + 약간의 지터
        sleep_s = (0.8 * (2 ** attempt)) + (0.1 * (attempt + 1))
        time.sleep(min(sleep_s, 8.0))

    # 최종 실패 시 None 반환하여 호출부에서 건너뛰기 가능
    return None


def fetch_story_details(story_id: int):
    """
    스토리 ID로 개별 스토리 상세 정보를 조회합니다.

    Args:
        story_id: Hacker News 스토리 ID

    Returns:
        스토리 상세 JSON
    """
    endpoint = f"item/{story_id}.json"
    return fetch_data(endpoint)


def filter_recent_stories(story_ids, days: int = 7):
    """
    최근 N일 이내의 스토리만 필터링하고 필요한 필드를 정리합니다.

    Args:
        story_ids: 스토리 ID 리스트
        days: 최근 일수(기본 7일)

    Returns:
        {id, title, url, time, score, by} 딕셔너리 리스트
    """
    recent_stories = []
    now_local = datetime.now()
    time_limit = now_local - timedelta(days=days)

    for story_id in story_ids:
        try:
            story = fetch_story_details(story_id)
        except Exception:
            # 예외 발생 시 해당 항목만 스킵
            continue

        if not story or story.get("type") != "story":
            continue

        story_time = datetime.utcfromtimestamp(story.get("time", 0))
        if story_time >= time_limit:
            recent_stories.append(
                {
                    "id": story_id,
                    "title": story.get("title"),
                    "url": story.get("url"),
                    "time": story_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "score": story.get("score"),
                    "by": story.get("by"),
                }
            )

    return recent_stories


def save_to_csv(rows, output_path: str):
    """
    스토리 리스트를 CSV 파일로 저장합니다.

    Args:
        rows: 저장할 행 리스트(딕셔너리)
        output_path: 출력 CSV 경로
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = ["id", "title", "url", "time", "score", "by"]
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    """
    HN Top Stories를 불러와 최근 7일치로 필터링하고
    점수 내림차순 정렬 후 날짜가 포함된 CSV로 저장합니다.
    """
    top_ids = fetch_data("topstories.json")
    if not top_ids:
        print("No top stories found.")
        return

    recent = filter_recent_stories(top_ids, days=7)
    # score 내림차순 정렬
    recent_sorted = sorted(recent, key=lambda r: (r.get("score") or 0), reverse=True)
    date_str = datetime.now().strftime("%Y%m%d")
    output_csv = os.path.join("data", f"hacker_news_topstories_last_7_days_{date_str}.csv")
    save_to_csv(recent_sorted, output_csv)

    print(f"Recent Top Stories (Last 7 Days): {len(recent_sorted)} saved")
    print(f"CSV: {output_csv}")


if __name__ == "__main__":
    main()


