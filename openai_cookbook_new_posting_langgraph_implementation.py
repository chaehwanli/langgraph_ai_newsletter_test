import os
from uuid import uuid4
from pathlib import Path
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated, Any, Dict, List, Literal
from pydantic import BaseModel
from datetime import date
from langchain_google_genai import ChatGoogleGenerativeAI
import json
from typing import Optional
import argparse
from openai_cookbook_new_posting_summary import process_html_directory
from openai_cookbook_new_posting_crawling_tool_example import save_cookbook_html_from_csv, save_recent_cookbook_posts_tool

# LangSmith 설정
#unique_id = uuid4().hex[0:8]
unique_id = "3"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"OpenAI Cookbook New Posting Crawling and Summary (LangGraph Implementation) - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)

class CookbookPost(BaseModel):
    date: date
    title: str
    url: str

class CookbookPostList(BaseModel):
    items: List[CookbookPost]

# 상태 정의
class State(TypedDict):
    cookbook_posts: Optional[CookbookPostList]
    days: Optional[int]
    csv_path: Optional[str]
    html_results: Optional[List[Dict[str, str]]]
    summary_outputs: Optional[List[str]]
    summary_count: Optional[int]

def save_recent_cookbook_posts(state: State) -> State:
    # 1) LLM 준비
    llm = ChatGoogleGenerativeAI(model="gemini-3.1-pro-preview", temperature=0)
    structured_llm = llm.with_structured_output(CookbookPostList)

    days = state.get("days") or 14
    out_path = f"data/openai_cookbook_last_{days}_days.csv"

    count, saved_path, raw_posts = save_recent_cookbook_posts_tool.invoke(
        {"days": days, "out": out_path}
    )

    # 3) LLM에게 스키마 강제하여 구조화 변환
    resp = structured_llm.invoke([
        ("system", "당신은 데이터 포매터입니다. 주어진 리스트에서 스키마에 맞게 출력하세요."),
        ("human", f"아래 리스트를 CookbookPostList 스키마에 맞게 반환해줘.\n{json.dumps(raw_posts, ensure_ascii=False)}")
    ])
    return {"cookbook_posts": resp}

def save_html_from_csv_node(state: State) -> State:
    """
    상태에 저장된 CSV 경로를 사용하여 외부 모듈의 save_cookbook_html_from_csv를 호출합니다.
    결과 리스트를 state["html_results"]에 저장합니다.
    """
    csv_path = state.get("csv_path") or f"data/openai_cookbook_last_{state.get('days') or 14}_days.csv"
    out_dir = "data/html"
    results = save_cookbook_html_from_csv(csv_path=csv_path, out_dir=out_dir)
    return {"html_results": results}

def summarize_html_files(state: State) -> State:
    project_root = Path(__file__).parent
    default_input_dir = project_root / "data" / "html"
    default_output_dir = project_root / "data" / "html_summary"

    input_dir = Path(default_input_dir)
    output_dir = Path(default_output_dir)

    # 디렉터리 보장 생성
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = process_html_directory(input_dir, output_dir)
    outputs = [str(p) for p in results]
    print(f"총 {len(outputs)}개 파일 요약 완료")

    return {"summary_outputs": outputs, "summary_count": len(outputs)}

def main():
    # 실행 인자 파싱 및 초기 상태 구성
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    # 그래프 생성
    workflow = StateGraph(State)

    # 노드 추가
    workflow.add_node("save_recent_cookbook_posts", save_recent_cookbook_posts)
    workflow.add_node("save_html_from_csv", save_html_from_csv_node)
    workflow.add_node("summarize_html_files", summarize_html_files)

    # 노드 연결
    workflow.set_entry_point("save_recent_cookbook_posts")
    workflow.add_edge("save_recent_cookbook_posts", "save_html_from_csv")
    workflow.add_edge("save_html_from_csv", "summarize_html_files")
    workflow.add_edge("summarize_html_files", END)

    # 그래프 빌드
    graph = workflow.compile()

    initial_state: State = {  # type: ignore[typeddict-item]
        "days": args.days,
        "csv_path": f"data/openai_cookbook_last_{args.days}_days.csv",
    }

    # 그래프 실행 및 결과 출력
    final_state = graph.invoke(initial_state)
    
    posts = final_state.get("cookbook_posts")
    if posts is not None:
        try:
            print(json.dumps(posts.model_dump(), ensure_ascii=False, indent=2))
        except Exception:
            print(posts)
    else:
        print("cookbook_posts not found in state")

    html_results = final_state.get("html_results")
    if html_results is not None:
        print(f"Saved HTML files: {len(html_results)}")
    else:
        print("html_results not found in state")

    summary_count = final_state.get("summary_count")
    if summary_count is not None:
        print(f"총 {summary_count}개 파일 요약 완료")
    else:
        print("summary_count not found in state")

if __name__ == "__main__":
    main()