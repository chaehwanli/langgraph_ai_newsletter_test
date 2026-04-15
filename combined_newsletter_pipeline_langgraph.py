import os
import csv
from pathlib import Path
from typing import TypedDict, Optional, List, Dict
from datetime import datetime

from langgraph.graph import StateGraph, END

# OpenAI Cookbook 수집/요약 관련 유틸
from openai_cookbook_new_posting_crawling_tool_example import (
    save_cookbook_html_from_csv,
    save_recent_cookbook_posts_tool,
)
from openai_cookbook_new_posting_summary import process_html_directory

# Hacker News 파이프라인 유틸
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

# 뉴스레터 템플릿 채우기 유틸
from generate_newsletter_from_summaries import (
    build_placeholders,
    read_text,
    write_text,
    fill_template,
)


class State(TypedDict):
    # 공통
    days_cookbook: Optional[int]
    days_hn: Optional[int]
    force_run_hn: Optional[bool]

    # Cookbook 산출물
    cookbook_csv_path: Optional[str]
    cookbook_html_dir: Optional[str]
    cookbook_summary_dir: Optional[str]
    cookbook_html_saved_count: Optional[int]
    cookbook_summary_count: Optional[int]

    # HN 산출물
    hn_topstories_csv_path: Optional[str]
    hn_topstories_count: Optional[int]
    hn_ai_only_csv_path: Optional[str]
    hn_ai_only_count: Optional[int]
    hn_top5_csv_path: Optional[str]
    hn_top5_count: Optional[int]
    hn_summary_outputs: Optional[List[str]]
    hn_summary_count: Optional[int]

    # 최종 HTML
    newsletter_output_path: Optional[str]


# ------------------------------
# OpenAI Cookbook 브랜치
# ------------------------------
def node_cookbook_save_recent_posts(state: State) -> State:
    days = state.get("days_cookbook") or 7
    out_path = f"data/openai_cookbook_last_{days}_days.csv"
    
    # 기존 openai_cookbook_last_{*}_days.csv 전부 삭제 후 재생성
    data_dir = Path("data")
    if data_dir.exists():
        for p in data_dir.glob("openai_cookbook_last_*_days.csv"):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    count, saved_path, _raw_posts = save_recent_cookbook_posts_tool.invoke({
        "days": days,
        "out": out_path,
    })
    # count/saved_path는 참고용. saved_path를 신뢰
    return {
        "days_cookbook": days,
        "cookbook_csv_path": saved_path,
    }


def node_cookbook_save_html_from_csv(state: State) -> State:
    csv_path = state.get("cookbook_csv_path") or f"data/openai_cookbook_last_{state.get('days_cookbook') or 7}_days.csv"
    out_dir = "data/html"

    results = save_cookbook_html_from_csv(csv_path=csv_path, out_dir=out_dir)
    return {
        "cookbook_html_dir": out_dir,
        "cookbook_html_saved_count": len(results),
    }


def node_cookbook_summarize_html(state: State) -> State:
    project_root = Path(__file__).parent
    input_dir = project_root / "data" / "html"
    output_dir = project_root / "data" / "html_summary"

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = process_html_directory(input_dir, output_dir)
    return {
        "cookbook_summary_dir": str(output_dir),
        "cookbook_summary_count": len(results),
    }


# ------------------------------
# Hacker News 브랜치
# ------------------------------
def node_hn_fetch_topstories(state: State) -> State:
    days = state.get("days_hn") or 7
    top_ids = fetch_data("topstories.json")
    if not top_ids:
        return {
            "hn_topstories_csv_path": None,
            "hn_topstories_count": 0,
        }

    recent = filter_recent_stories(top_ids, days=days)
    recent_sorted = sorted(recent, key=lambda r: (r.get("score") or 0), reverse=True)

    date_str = datetime.now().strftime("%Y%m%d")
    output_csv = os.path.join("data", f"hacker_news_topstories_last_7_days_{date_str}.csv")

    force = bool(state.get("force_run_hn"))
    if os.path.exists(output_csv) and not force:
        count = sum(1 for _ in open(output_csv, "r", encoding="utf-8-sig")) - 1 if os.path.getsize(output_csv) > 0 else 0
        return {
            "days_hn": days,
            "hn_topstories_csv_path": output_csv,
            "hn_topstories_count": count,
        }

    save_to_csv(recent_sorted, output_csv)
    return {
        "days_hn": days,
        "hn_topstories_csv_path": output_csv,
        "hn_topstories_count": len(recent_sorted),
    }


def node_hn_filter_ai_only(state: State) -> State:
    input_csv = state.get("hn_topstories_csv_path")
    date_str = datetime.now().strftime("%Y%m%d")
    if not input_csv:
        return {
            "hn_ai_only_csv_path": None,
            "hn_ai_only_count": 0,
        }

    input_path = Path(input_csv)
    rows = read_rows_ai_filter(input_path)
    out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_only_{date_str}.csv"

    if not rows:
        if out_csv.exists():
            count = sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0
            return {
                "hn_ai_only_csv_path": str(out_csv),
                "hn_ai_only_count": count,
            }
        save_rows_ai_filter([], out_csv)
        return {
            "hn_ai_only_csv_path": str(out_csv),
            "hn_ai_only_count": 0,
        }

    id_to_is_ai = classify_ai_titles(rows, model_name="gemini-3.1-pro-preview", chunk_size=60)
    filtered_sorted = filter_and_sort(rows, id_to_is_ai)

    if out_csv.exists():
        count = sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0
        return {
            "hn_ai_only_csv_path": str(out_csv),
            "hn_ai_only_count": count,
        }

    save_rows_ai_filter(filtered_sorted, out_csv)
    return {
        "hn_ai_only_csv_path": str(out_csv),
        "hn_ai_only_count": len(filtered_sorted),
    }


def node_hn_select_top5(state: State) -> State:
    input_csv = state.get("hn_ai_only_csv_path")
    date_str = datetime.now().strftime("%Y%m%d")
    if not input_csv:
        return {
            "hn_top5_csv_path": None,
            "hn_top5_count": 0,
        }

    input_path = Path(input_csv)
    rows = read_rows_top5(input_path)
    out_csv = input_path.parent / f"hacker_news_topstories_last_7_days_ai_top5_{date_str}.csv"

    if not rows:
        if out_csv.exists():
            count = sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0
            return {
                "hn_top5_csv_path": str(out_csv),
                "hn_top5_count": count,
            }
        save_rows_top5([], out_csv)
        return {
            "hn_top5_csv_path": str(out_csv),
            "hn_top5_count": 0,
        }

    top5 = rank_top5_with_gpt(rows, model_name="gemini-3.1-pro-preview")

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

    if out_csv.exists():
        count = sum(1 for _ in open(out_csv, "r", encoding="utf-8-sig")) - 1 if out_csv.stat().st_size > 0 else 0
        return {
            "hn_top5_csv_path": str(out_csv),
            "hn_top5_count": count,
        }

    save_rows_top5(ordered_rows, out_csv)
    return {
        "hn_top5_csv_path": str(out_csv),
        "hn_top5_count": len(ordered_rows),
    }


def node_hn_summarize_urls(state: State) -> State:
    input_csv = state.get("hn_top5_csv_path")
    if not input_csv:
        return {"hn_summary_outputs": [], "hn_summary_count": 0}

    project_root = Path(__file__).parent
    output_dir = project_root / "data" / "hn_url_summary"
    results = process_csv_for_summaries(Path(input_csv), output_dir)
    outputs = [str(p) for p in results]
    return {
        "hn_summary_outputs": outputs,
        "hn_summary_count": len(outputs),
    }


# ------------------------------
# 최종 뉴스레터 HTML 생성 노드
# ------------------------------
def node_generate_newsletter(state: State) -> State:
    root = Path(__file__).parent.resolve()
    template_path = root / "newsletter" / "index.html"
    output_path = root / "newsletter" / "index_filled.html"

    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")

    placeholders = build_placeholders(root)
    template_html = read_text(template_path)
    filled_html = fill_template(template_html, placeholders)

    write_text(output_path, filled_html)
    return {"newsletter_output_path": str(output_path)}


def build_and_run_graph(days_cookbook: int = 7, days_hn: int = 7, force_run_hn: bool = False) -> State:
    workflow = StateGraph(State)

    # 노드 등록
    workflow.add_node("cookbook_save_recent", node_cookbook_save_recent_posts)
    workflow.add_node("cookbook_save_html", node_cookbook_save_html_from_csv)
    workflow.add_node("cookbook_summarize", node_cookbook_summarize_html)

    workflow.add_node("hn_fetch", node_hn_fetch_topstories)
    workflow.add_node("hn_filter_ai", node_hn_filter_ai_only)
    workflow.add_node("hn_select_top5", node_hn_select_top5)
    workflow.add_node("hn_summarize", node_hn_summarize_urls)

    workflow.add_node("generate_newsletter", node_generate_newsletter)

    # 순차 실행 (Cookbook → HN → Generate)
    workflow.set_entry_point("cookbook_save_recent")
    workflow.add_edge("cookbook_save_recent", "cookbook_save_html")
    workflow.add_edge("cookbook_save_html", "cookbook_summarize")
    workflow.add_edge("cookbook_summarize", "hn_fetch")
    workflow.add_edge("hn_fetch", "hn_filter_ai")
    workflow.add_edge("hn_filter_ai", "hn_select_top5")
    workflow.add_edge("hn_select_top5", "hn_summarize")
    workflow.add_edge("hn_summarize", "generate_newsletter")
    workflow.add_edge("generate_newsletter", END)

    graph = workflow.compile()
    initial_state: State = {  # type: ignore[typeddict-item]
        "days_cookbook": days_cookbook,
        "days_hn": days_hn,
        "force_run_hn": force_run_hn,
    }
    final_state = graph.invoke(initial_state)
    return final_state


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--days-cookbook", type=int, default=7, help="OpenAI Cookbook 최근 N일")
    parser.add_argument("--days-hn", type=int, default=7, help="Hacker News 최근 N일")
    parser.add_argument("--force-run-hn", action="store_true", help="HN TopStories CSV가 있어도 강행 실행")
    args = parser.parse_args()

    final_state = build_and_run_graph(
        days_cookbook=args.days_cookbook,
        days_hn=args.days_hn,
        force_run_hn=args.force_run_hn,
    )

    # 요약 출력
    if final_state.get("cookbook_csv_path"):
        print(f"Cookbook CSV: {final_state.get('cookbook_csv_path')}")
    if final_state.get("cookbook_html_saved_count") is not None:
        print(f"Cookbook HTML 저장: {final_state.get('cookbook_html_saved_count')}건")
    if final_state.get("cookbook_summary_count") is not None:
        print(f"Cookbook 요약: {final_state.get('cookbook_summary_count')}건")

    if final_state.get("hn_topstories_csv_path"):
        print(f"TopStories: {final_state.get('hn_topstories_count')}건 → {final_state.get('hn_topstories_csv_path')}")
    if final_state.get("hn_ai_only_csv_path"):
        print(f"AI Only: {final_state.get('hn_ai_only_count')}건 → {final_state.get('hn_ai_only_csv_path')}")
    if final_state.get("hn_top5_csv_path"):
        print(f"Top5: {final_state.get('hn_top5_count')}건 → {final_state.get('hn_top5_csv_path')}")
    if final_state.get("hn_summary_count") is not None:
        print(f"HN URL 요약 완료: {final_state.get('hn_summary_count')}건")

    if final_state.get("newsletter_output_path"):
        print(f"Generated: {final_state.get('newsletter_output_path')}")


if __name__ == "__main__":
    main()