import os
import csv
import json
import re
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

from langchain_google_genai import ChatGoogleGenerativeAI

from uuid import uuid4

#unique_id = uuid4().hex[0:8]
unique_id = "1"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"Hacker News AI News Top5 Selector - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)

def read_csv_rows(input_path: Path) -> List[Dict[str, Any]]:
    """지정한 CSV 파일을 읽어 각 행을 딕셔너리로 리스트 반환."""
    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
    return rows


def save_csv_rows(rows: List[Dict[str, Any]], output_path: Path) -> None:
    """행 리스트를 지정한 경로의 CSV로 저장. 파일/디렉터리 생성 포함."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        fieldnames = ["id", "title", "url", "time", "score", "by", "rank", "reason", "domain", "source_priority"]
        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def find_latest_ai_only_csv(data_dir: Path) -> Tuple[Path | None, str | None]:
    """data 디렉터리에서 가장 최신의 AI 전용 CSV와 날짜 문자열을 찾는다."""
    pattern = re.compile(r"^hacker_news_topstories_last_7_days_ai_only_(\d{8})\.csv$")
    candidates: List[Tuple[Path, str, datetime]] = []
    for p in data_dir.glob("hacker_news_topstories_last_7_days_ai_only_*.csv"):
        m = pattern.match(p.name)
        if not m:
            continue
        date_str = m.group(1)
        try:
            d = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue
        candidates.append((p, date_str, d))

    if not candidates:
        return None, None

    # 가장 최근 날짜 기준으로 선택
    candidates.sort(key=lambda x: x[2], reverse=True)
    best_path, best_date_str, _ = candidates[0]
    return best_path, best_date_str


def extract_domain(url: str) -> str:
    """URL에서 도메인(netloc)을 소문자로 추출하여 반환."""
    try:
        netloc = urlparse(url).netloc.lower()
        # strip leading 'www.'
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""


def compute_source_priority(domain: str) -> int:
    """
    3: 공식/퍼스트파티(모델사·연구조직·공식 블로그/뉴스룸)
    2: 메이저 언론/학술(Reuters, WSJ, arXiv 등)
    1: 그 외(개인 블로그/커뮤니티 등)
    """
    official_keywords = [
        "openai.com",
        "anthropic.com",
        "deepmind.com",
        "ai.google",
        "google.ai",
        "meta.com",
        "ai.meta.com",
        "research.facebook.com",
        "microsoft.com",
        "azure.microsoft.com",
        "mistral.ai",
        "x.ai",
        "qwen.ai",
        "huggingface.co",
        "stability.ai",
        "nvidia.com",
        "apple.com",
        "cohere.com",
        "databricks.com",
    ]
    major_media_keywords = [
        "reuters.com",
        "wsj.com",
        "bloomberg.com",
        "ft.com",
        "theverge.com",
        "techcrunch.com",
        "ieee.org",
        "spectrum.ieee.org",
        "arxiv.org",
        "nature.com",
        "science.org",
        "wired.com",
        "nytimes.com",
        "guardian.com",
        "cbsnews.com",
        "politico.eu",
    ]

    d = domain or ""
    if any(k in d for k in official_keywords):
        return 3
    if any(k in d for k in major_media_keywords):
        return 2
    return 1


def parse_json_list_safely(text: str) -> List[Dict[str, Any]]:
    """LLM 응답 등에서 리스트 형태의 JSON을 안전하게 파싱하여 반환."""
    text = text.strip()
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    candidates = [
        text,
        text.replace("True", "true").replace("False", "false").replace("None", "null"),
    ]
    for c in candidates:
        try:
            data = json.loads(c)
            if isinstance(data, list):
                return data
        except Exception:
            continue
    return []


def build_ranking_messages(items: List[Dict[str, Any]]) -> list:
    """후보 아이템들을 기반으로 랭킹용 시스템/사용자 메시지 구성."""
    system_msg = """
    당신은 AI 뉴스 에디터입니다. 
    주어진 후보 중에서 가장 중요한 5개만 최종 선정하세요. 
    선정 원칙: 
    (1) OpenAI, Anthropic, Google DeepMind, Microsoft, Meta, Mistral, Qwen 등 공식 사이트/뉴스룸/연구 블로그의 1차 출처를 개인 블로그나 커뮤니티보다 우선합니다.
    (2) AI 연구·제품 릴리스·정책/규제·대형 파트너십·시장에 미치는 영향이 큰 소식에 가중치를 둡니다.
    (3) 중복/재탕/가십성/낚시성은 낮게 평가합니다.
    (4) 동률일 경우 점수(score)와 최신성(time)을 보조 기준으로 사용하세요.
    출력은 JSON 배열만 반환하세요.
    각 요소 스키마: {id, rank(1~5), reason(한국어 1문장), keep: {title, url}}.
    """

    # 모델이 판단에 활용할 수 있도록 최소 특성 제공
    payload = []
    for it in items:
        payload.append({
            "id": str(it.get("id")),
            "title": it.get("title", ""),
            "url": it.get("url", ""),
            "domain": it.get("domain", ""),
            "score": it.get("score", 0),
            "time": it.get("time", ""),
            "source_priority": it.get("source_priority", 1),
        })

    user_msg = (
        "후보 목록이 아래에 있습니다. 공식 출처에 우선순위를 두고 가장 중요한 5개를 선정하세요.\n"
        "입력: " + json.dumps(payload, ensure_ascii=False)
    )

    return [("system", system_msg), ("human", user_msg)]


def coerce_int(value: Any, default: int = 0) -> int:
    """값을 정수로 변환. 실패 시 기본값 반환."""
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def enrich_rows(rows: List[Dict[str, Any]]) -> None:
    """각 행에 도메인, 출처 우선순위, 정수형 점수 필드를 보강."""
    for r in rows:
        d = extract_domain(r.get("url", ""))
        r["domain"] = d
        r["source_priority"] = compute_source_priority(d)
        r["score"] = coerce_int(r.get("score"), 0)


def rank_top5_with_gpt(rows: List[Dict[str, Any]], model_name: str = "gemini-3.1-pro-preview") -> List[Dict[str, Any]]:
    """후보 뉴스에서 Gemini를 이용해 Top5를 선정하고 보강 로직으로 채운다."""
    # 너무 많은 후보일 경우 토큰 보호를 위해 점수 상위 80개로 컷
    candidates = sorted(rows, key=lambda x: x.get("score") or 0, reverse=True)[:80]
    enrich_rows(candidates)

    llm = ChatGoogleGenerativeAI(model=model_name, temperature=0.2)
    messages = build_ranking_messages(candidates)
    resp = llm.invoke(messages)
    content = getattr(resp, "content", str(resp))
    parsed = parse_json_list_safely(content)

    # id -> row 매핑
    by_id: Dict[str, Dict[str, Any]] = {str(r.get("id")): r for r in rows}

    results: List[Dict[str, Any]] = []
    for item in parsed:
        _id = str(item.get("id")) if item.get("id") is not None else ""
        if not _id or _id not in by_id:
            continue
        base = dict(by_id[_id])
        base["rank"] = int(item.get("rank", 0)) if str(item.get("rank", "")).isdigit() else len(results) + 1
        base["reason"] = str(item.get("reason", "")).strip()
        # 보조 정보 유지
        base["domain"] = extract_domain(base.get("url", ""))
        base["source_priority"] = compute_source_priority(base["domain"])
        results.append(base)

    # 모델 출력이 부족/이상할 경우 보강: 공식성→점수→시간 기준 정렬로 채우기
    if len(results) < 5:
        remaining = [r for r in rows if str(r.get("id")) not in {str(x.get("id")) for x in results}]
        enrich_rows(remaining)
        remaining.sort(key=lambda x: (-(x.get("source_priority") or 1), -(x.get("score") or 0), x.get("time", "")), reverse=False)
        for r in remaining:
            if len(results) >= 5:
                break
            r_copy = dict(r)
            r_copy["rank"] = len(results) + 1
            r_copy["reason"] = "모델 보강: 공식성 및 점수 기준으로 자동 충원"
            results.append(r_copy)

    # 최종 상위 5개만, rank로 정렬 후 1..5 재부여
    results.sort(key=lambda x: x.get("rank", 999999))
    results = results[:5]
    for i, r in enumerate(results, start=1):
        r["rank"] = i
    return results


def main() -> None:
    """최신 AI 전용 스토리 CS4V를 읽어 Top5를 선정하고 결과 CSV로 저장."""
    project_root = Path(__file__).parent
    data_dir = project_root / "data"

    input_csv, date_str = find_latest_ai_only_csv(data_dir)
    if not input_csv or not input_csv.exists():
        print(f"AI 전용 입력 CSV를 찾을 수 없습니다: {data_dir}")
        return

    rows = read_csv_rows(input_csv)
    if not rows:
        print("입력 CSV에 데이터가 없습니다.")
        # 그래도 출력 파일은 생성
        out_csv = data_dir / f"hacker_news_topstories_last_7_days_ai_top5_{date_str or datetime.now().strftime('%Y%m%d')}.csv"
        save_csv_rows([], out_csv)
        print(f"빈 결과를 저장했습니다: {out_csv}")
        return

    # Gemini로 중요도 상위 5개 선정 (공식 출처 우대)
    top5 = rank_top5_with_gpt(rows, model_name="gemini-3.1-pro-preview")

    # 저장 및 출력
    out_csv = data_dir / f"hacker_news_topstories_last_7_days_ai_top5_{date_str}.csv"
    # 열 순서 통일
    ordered_rows: List[Dict[str, Any]] = []
    for r in top5:
        ordered_rows.append({
            "id": r.get("id"),
            "title": r.get("title"),
            "url": r.get("url"),
            "time": r.get("time"),
            "score": r.get("score"),
            "by": r.get("by"),
            "rank": r.get("rank"),
            "reason": r.get("reason", ""),
            "domain": r.get("domain", extract_domain(r.get("url", ""))),
            "source_priority": r.get("source_priority", compute_source_priority(extract_domain(r.get("url", "")))),
        })

    save_csv_rows(ordered_rows, out_csv)

    print(f"입력: {input_csv.name} → 최종 Top5 저장: {out_csv.name}")
    for r in ordered_rows:
        print(f"#{r['rank']} [{r['domain']}] {r['title']} — {r['reason']}")


if __name__ == "__main__":
    main()


