import os
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from langchain_google_genai import ChatGoogleGenerativeAI

from uuid import uuid4

#unique_id = uuid4().hex[0:8]
unique_id = "2"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"Hacker News AI News Filter - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)

def read_csv_rows(input_path: Path) -> List[Dict[str, Any]]:
    """
    CSV 파일을 읽어 각 행을 딕셔너리로 담은 리스트로 반환합니다.
    """
    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
    return rows


def save_csv_rows(rows: List[Dict[str, Any]], output_path: Path) -> None:
    """
    행 리스트를 주어진 경로의 CSV 파일로 저장합니다.
    rows가 비어있으면 기본 헤더만 기록합니다.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # 기본 헤더 유지
        fieldnames = ["id", "title", "url", "time", "score", "by"]
        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_json_list_safely(text: str) -> List[Dict[str, Any]]:
    """
    LLM 출력 등에서 온 문자열에서 JSON 배열을 안전하게 파싱합니다.
    실패 시 빈 리스트를 반환합니다.
    """
    text = text.strip()
    # 모델이 코드펜스/설명 등을 붙였을 가능성 방지
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    # 일부 모델이 대문자 Boolean을 반환할 경우 보정
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


def build_classifier_prompt(items: List[Dict[str, Any]]) -> list:
    """
    AI 관련 여부 분류를 위해 시스템/사용자 메시지 목록을 생성합니다.
    """
    system_msg = """
    당신은 뉴스 제목 분류기입니다.
    각 제목이 AI(인공지능) 관련 뉴스인지 여부를 판단하세요.
    AI 관련의 예: 인공지능/머신러닝/딥러닝/생성형 AI/LLM/에이전트/모델 릴리스/AI 정책·규제·윤리/AI 제품·서비스 출시/AI 연구·기술.
    AI와 무관한 일반 IT/정치/사회 이슈는 제외하세요.
    출력은 JSON 배열만, 다른 텍스트 없이 반환하세요.
    """
    # 간단한 입력 포맷: id, title만 전달
    user_payload = [{"id": it.get("id"), "title": it.get("title", "")} for it in items]
    user_msg = f"""
    다음 목록의 각 항목에 대해 {{id, is_ai}}를 반환하세요.
    - is_ai는 true/false만 사용
    - 형식: [{{"id": "<ID>", "is_ai": true}}, ...]

    목록: {json.dumps(user_payload, ensure_ascii=False)}"""
    return [("system", system_msg), ("human", user_msg)]


def classify_ai_titles(rows: List[Dict[str, Any]], model_name: str = "gemini-3.1-pro-preview", chunk_size: int = 60) -> Dict[str, bool]:
    """
    제목 목록을 청크로 나눠 LLM에 보내 AI 여부를 분류합니다.
    결과로 id -> is_ai 불리언 매핑을 반환합니다.
    """
    llm = ChatGoogleGenerativeAI(model=model_name, temperature=0)
    id_to_is_ai: Dict[str, bool] = {}

    # 청크 분할
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        messages = build_classifier_prompt(chunk)
        resp = llm.invoke(messages)
        content = getattr(resp, "content", str(resp))
        parsed = parse_json_list_safely(content)
        for item in parsed:
            _id = str(item.get("id"))
            is_ai = bool(item.get("is_ai"))
            if _id:
                id_to_is_ai[_id] = is_ai

    return id_to_is_ai


def filter_and_sort(rows: List[Dict[str, Any]], id_to_is_ai: Dict[str, bool]) -> List[Dict[str, Any]]:
    """
    분류 결과를 반영하여 AI로 판단된 항목만 남기고,
    score 기준으로 내림차순 정렬해 반환합니다.
    """
    filtered: List[Dict[str, Any]] = []
    for r in rows:
        rid = str(r.get("id"))
        if id_to_is_ai.get(rid, False):
            # score를 정수/실수로 안전 변환
            score_raw = r.get("score")
            try:
                score_num = int(score_raw) if score_raw not in (None, "") else 0
            except Exception:
                try:
                    score_num = float(score_raw)
                except Exception:
                    score_num = 0
            r["score"] = score_num
            filtered.append(r)

    # score 내림차순 유지
    filtered.sort(key=lambda x: (x.get("score") is None, -(x.get("score") or 0)))

    # CSV 저장 시 문자열 일관성 위해 score를 그대로 저장(숫자 가능)
    return filtered


def find_nearest_input_csv(data_dir: Path):
    """
    data 디렉터리에서 날짜가 붙은 CSV들 중 오늘과 가장 가까운 날짜의 파일을 찾아
    (파일 경로, 'YYYYMMDD')를 반환합니다. 해당 파일이 없으면 (None, None)을 반환합니다.
    """
    pattern = re.compile(r"^hacker_news_topstories_last_7_days_(\d{8})\.csv$")
    candidates = []
    for p in data_dir.glob("hacker_news_topstories_last_7_days_*.csv"):
        m = pattern.match(p.name)
        if not m:
            continue
        date_str = m.group(1)
        try:
            d = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            continue
        candidates.append((p, date_str, d))

    if candidates:
        today = datetime.now().date()
        # 가장 가까운 날짜, 동률이면 더 최근 날짜 우선
        candidates.sort(key=lambda x: (abs((x[2] - today).days), -x[2].toordinal()))
        best_path, best_date_str, _ = candidates[0]
        return best_path, best_date_str

    return None, None


def main():
    """
    입력 CSV를 찾고 로드한 뒤 분류를 수행하고,
    AI-only 결과를 날짜 포함 파일명으로 저장합니다.
    """
    project_root = Path(__file__).parent
    data_dir = project_root / "data"
    input_csv, date_str = find_nearest_input_csv(data_dir)

    if not input_csv or not input_csv.exists():
        print(f"입력 파일을 찾을 수 없습니다: {data_dir}")
        return

    rows = read_csv_rows(input_csv)
    if not rows:
        print("입력 CSV에 데이터가 없습니다.")
        # 비어있어도 출력 파일명 규칙은 유지
        out_csv = data_dir / f"hacker_news_topstories_last_7_days_ai_only_{date_str}.csv"
        save_csv_rows([], out_csv)
        print(f"빈 결과를 저장했습니다: {out_csv}")
        return

    id_to_is_ai = classify_ai_titles(rows, model_name="gemini-3.1-pro-preview", chunk_size=60)
    filtered_sorted = filter_and_sort(rows, id_to_is_ai)
    out_csv = data_dir / f"hacker_news_topstories_last_7_days_ai_only_{date_str}.csv"
    save_csv_rows(filtered_sorted, out_csv)

    print(
        f"입력: {input_csv.name} → AI 관련 뉴스: {len(filtered_sorted)}/{len(rows)}건 저장 → {out_csv.name}"
    )


if __name__ == "__main__":
    main()