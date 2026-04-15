import os
import csv
from uuid import uuid4
from pathlib import Path
from typing import TypedDict, Optional, List, Dict
from datetime import datetime

from langgraph.graph import StateGraph, END

# 외부 단계별 구현에서 재사용할 함수들 임포트
from hacker_news_topstories_last_7_days_to_csv import (
    fetch_data,
    filter_recent_stories,
    save_to_csv,
)

from hacker_news_only_ai_news_filtering import (
    read_csv_rows as read_rows_ai_filter,
    save_csv_rows as save_rows_ai_filter,
    classify_ai_titles,
    filter_and_sort,
)

from hacker_news_select_top5_ai_news import (
    read_csv_rows as read_rows_top5,
    save_csv_rows as save_rows_top5,
    rank_top5_with_gpt,
)

from hacker_news_ai_url_summary import (
    process_csv as process_csv_for_summaries,
)


# LangSmith 설정 (선택)
unique_id = uuid4().hex[0:8]
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"HN AI Newsletter Pipeline (LangGraph) - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"


class State(TypedDict):
    days: Optional[int]
    date_str: Optional[str]

    # 단계별 산출물 경로/개수
    topstories_csv_path: Optional[str]
    topstories_count: Optional[int]

    ai_only_csv_path: Optional[str]
    ai_only_count: Optional[int]

    top5_csv_path: Optional[str]
    top5_count: Optional[int]
    top5_title_date_pairs: Optional[List[Dict[str, str]]]

    summary_outputs: Optional[List[str]]
    summary_count: Optional[int]


def node_fetch_topstories(state: State) -> State:
    days = state.get("days") or 7

    top_ids = fetch_data("topstories.json")
    if not top_ids:
        return {
            "topstories_csv_path": None,
            "topstories_count": 0,
        }

    recent = filter_recent_stories(top_ids, days=days)
    recent_sorted = sorted(recent, key=lambda r: (r.get("score") or 0), reverse=True)

    date_str = datetime.now().strftime("%Y%m%d")
    output_csv = os.path.join("data", f"hacker_news_topstories_last_7_days_{date_str}.csv")
    # 이미 존재하면 스킵
    if os.path.exists(output_csv):
        return {
            "days": days,
            "date_str": date_str,
            "topstories_csv_path": output_csv,
            "topstories_count": sum(1 for _ in open(output_csv, "r", encoding="utf-8-sig")) - 1 if os.path.getsize(output_csv) > 0 else 0,
        }
    save_to_csv(recent_sorted, output_csv)

    return {
        "days": days,
        "date_str": date_str,
        "topstories_csv_path": output_csv,
        "topstories_count": len(recent_sorted),
    }


def node_filter_ai_only(state: State) -> State:
    input_csv = state.get("topstories_csv_path")
    date_str = state.get("date_str") or datetime.now().strftime("%Y%m%d")
    if not input_csv:
        return {
            "ai_only_csv_path": None,
            "ai_only_count": 0,
        }

    input_path = Path(input_csv)
    rows = read_rows_ai_filter(input_path)
    if not rows:
        out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_only_{date_str}.csv"
        # 이미 존재하면 스킵
        if out_csv.exists():
            return {
                "ai_only_csv_path": str(out_csv),
                "ai_only_count": sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0,
            }
        save_rows_ai_filter([], out_csv)
        return {
            "ai_only_csv_path": str(out_csv),
            "ai_only_count": 0,
        }

    id_to_is_ai = classify_ai_titles(rows, model_name="gpt-5-mini", chunk_size=60)
    filtered_sorted = filter_and_sort(rows, id_to_is_ai)

    out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_only_{date_str}.csv"
    # 이미 존재하면 스킵
    if out_csv.exists():
        return {
            "ai_only_csv_path": str(out_csv),
            "ai_only_count": sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0,
        }
    save_rows_ai_filter(filtered_sorted, out_csv)
    return {
        "ai_only_csv_path": str(out_csv),
        "ai_only_count": len(filtered_sorted),
    }


def node_select_top5(state: State) -> State:
    input_csv = state.get("ai_only_csv_path")
    date_str = state.get("date_str") or datetime.now().strftime("%Y%m%d")
    if not input_csv:
        return {
            "top5_csv_path": None,
            "top5_count": 0,
            "top5_title_date_pairs": [],
        }

    input_path = Path(input_csv)
    rows = read_rows_top5(input_path)
    if not rows:
        out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_top5_{date_str}.csv"
        # 이미 존재하면 스킵
        if out_csv.exists():
            return {
                "top5_csv_path": str(out_csv),
                "top5_count": sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0,
                "top5_title_date_pairs": [
                    {"title": (r.get("title") or ""), "date": (str(r.get("time"))[:10] if r.get("time") else "")}
                    for r in read_rows_top5(out_csv)
                ],
            }
        save_rows_top5([], out_csv)
        return {
            "top5_csv_path": str(out_csv),
            "top5_count": 0,
            "top5_title_date_pairs": [],
        }

    # GPT-5로 상위 5개 선정 (모듈 내 정책 재사용)
    top5 = rank_top5_with_gpt(rows, model_name="gpt-5")

    # 열 순서 통일하여 저장
    ordered_rows: List[Dict] = []
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
            "domain": r.get("domain"),
            "source_priority": r.get("source_priority"),
        })

    out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_top5_{date_str}.csv"
    # 이미 존재하면 스킵하면서 게시날짜 읽기
    if out_csv.exists():
        existing_rows = read_rows_top5(out_csv)
        return {
            "top5_csv_path": str(out_csv),
            "top5_count": sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0,
            "top5_title_date_pairs": [
                {"title": (r.get("title") or ""), "date": (str(r.get("time"))[:10] if r.get("time") else "")}
                for r in existing_rows
            ],
        }
    save_rows_top5(ordered_rows, out_csv)
    return {
        "top5_csv_path": str(out_csv),
        "top5_count": len(ordered_rows),
        "top5_title_date_pairs": [
            {"title": (r.get("title") or ""), "date": (str(r.get("time"))[:10] if r.get("time") else "")}
            for r in ordered_rows
        ],
    }


def node_summarize_urls(state: State) -> State:
    input_csv = state.get("top5_csv_path")
    if not input_csv:
        return {
            "summary_outputs": [],
            "summary_count": 0,
        }

    project_root = Path(__file__).parent
    output_dir = project_root / "data" / "hn_url_summary"
    results = process_csv_for_summaries(Path(input_csv), output_dir)
    outputs = [str(p) for p in results]
    return {
        "summary_outputs": outputs,
        "summary_count": len(outputs),
    }


def build_and_run_graph(days: int = 7) -> State:
    workflow = StateGraph(State)

    workflow.add_node("fetch_topstories", node_fetch_topstories)
    workflow.add_node("filter_ai_only", node_filter_ai_only)
    workflow.add_node("select_top5", node_select_top5)
    workflow.add_node("summarize_urls", node_summarize_urls)

    workflow.set_entry_point("fetch_topstories")
    workflow.add_edge("fetch_topstories", "filter_ai_only")
    workflow.add_edge("filter_ai_only", "select_top5")
    workflow.add_edge("select_top5", "summarize_urls")
    workflow.add_edge("summarize_urls", END)

    graph = workflow.compile()
    initial_state: State = {  # type: ignore[typeddict-item]
        "days": days,
    }
    final_state = graph.invoke(initial_state)
    return final_state


def main():
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="최근 N일 범위 필터 (기본 7일)")
    parser.add_argument("--force-run", action="store_true", help="최근 7일 내 실행내역이 있어도 강행 실행")
    args = parser.parse_args()

    # 최근 7일 내 Top5 CSV가 있으면 기본적으로 실행 중단
    project_root = Path(__file__).parent
    data_dir = project_root / "data"
    latest_csv = None
    latest_date = None
    for p in sorted(data_dir.glob("hacker_news_topstories_last_7_days_ai_top5_*.csv"), reverse=True):
        name = p.name
        import re
        m = re.search(r"(\d{8})(?=\.csv$)", name)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d").date()
        except Exception:
            continue
        latest_csv = p
        latest_date = d
        break

    if latest_csv and latest_date is not None:
        today = datetime.now().date()
        if (today - latest_date).days <= 7 and not args.force_run:
            print(
                f"최근 7일 내 실행내역이 존재합니다: {latest_csv.name} (날짜: {latest_date}).\n"
                f"재실행하지 않습니다. 강행 실행하려면 --force-run 플래그를 사용하세요."
            )

            # 최신 CSV를 기반으로 final_state 구성
            latest_date_str = latest_date.strftime("%Y%m%d")

            # Top5
            top5_csv_path = str(latest_csv)
            top5_rows = []
            try:
                with open(latest_csv, "r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    top5_rows = list(reader)
            except Exception:
                top5_rows = []
            top5_count = len(top5_rows)
            title_date_pairs = [
                {"title": (r.get("title") or ""), "date": (str(r.get("time"))[:10] if r.get("time") else "")}
                for r in top5_rows
            ]

            # AI-only
            ai_only_path = data_dir / f"hacker_news_topstories_last_7_days_ai_only_{latest_date_str}.csv"
            ai_only_csv_path = str(ai_only_path) if ai_only_path.exists() else None
            ai_only_count = None
            if ai_only_path.exists():
                try:
                    with open(ai_only_path, "r", encoding="utf-8-sig", newline="") as f:
                        reader = csv.reader(f)
                        ai_only_count = sum(1 for _ in reader) - 1
                except Exception:
                    ai_only_count = None

            # Topstories (원본)
            topstories_path = data_dir / f"hacker_news_topstories_last_7_days_{latest_date_str}.csv"
            topstories_csv_path = str(topstories_path) if topstories_path.exists() else None
            topstories_count = None
            if topstories_path.exists():
                try:
                    with open(topstories_path, "r", encoding="utf-8-sig", newline="") as f:
                        reader = csv.reader(f)
                        topstories_count = sum(1 for _ in reader) - 1
                except Exception:
                    topstories_count = None

            final_state = {
                "days": args.days,
                "date_str": latest_date_str,
                "topstories_csv_path": topstories_csv_path,
                "topstories_count": topstories_count,
                "ai_only_csv_path": ai_only_csv_path,
                "ai_only_count": ai_only_count,
                "top5_csv_path": top5_csv_path,
                "top5_count": top5_count,
                "top5_title_date_pairs": title_date_pairs,
                "summary_outputs": None,
                "summary_count": None,
            }
        else:
            final_state = build_and_run_graph(days=args.days)
    else:
        final_state = build_and_run_graph(days=args.days)

    # 간단한 출력 요약
    if final_state.get("topstories_csv_path"):
        print(f"TopStories: {final_state.get('topstories_count')}건 → {final_state.get('topstories_csv_path')}")
    if final_state.get("ai_only_csv_path"):
        print(f"AI Only: {final_state.get('ai_only_count')}건 → {final_state.get('ai_only_csv_path')}")
    if final_state.get("top5_csv_path"):
        print(f"Top5: {final_state.get('top5_count')}건 → {final_state.get('top5_csv_path')}")
    if final_state.get("summary_count") is not None:
        print(f"URL 요약 완료: {final_state.get('summary_count')}건")

    # Top5 제목-날짜 쌍 출력
    pairs = final_state.get("top5_title_date_pairs") or []
    if pairs:
        print("Top5 제목-날짜:")
        for i, pair in enumerate(pairs, start=1):
            print(f"#{i} [{pair.get('date','')}] {pair.get('title','')}")


if __name__ == "__main__":
    main()


