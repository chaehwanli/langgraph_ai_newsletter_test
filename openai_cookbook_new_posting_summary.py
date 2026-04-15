import os
from uuid import uuid4
from scripts.scrape_cookbook_week import collect_recent_posts
from datetime import datetime, timezone
from pathlib import Path
import sys

from langchain_core.tools import tool
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_google_genai import ChatGoogleGenerativeAI
from textwrap import dedent
from bs4 import BeautifulSoup
import re

# LangSmith 설정
#unique_id = uuid4().hex[0:8]
unique_id = "3"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"OpenAI Cookbook New Posting Summary - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)

prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            dedent(
                """아래 영어 원문을 한국어로 번역·요약하세요.
                요약 내용만 보고도 전체 원문의 핵심 주제가 무엇인지 파악할 수 있도록 핵심 주제를 명확히 파악한뒤 요약을 진행하세요.
                요약을 진행할때 다음의 지침을 따르세요:
                - 전체 원문의 핵심 주제를 명확히 파악한뒤 요약을 진행하세요.
                - 지나치게 지엽적인 내용은 제외하세요.
                - 최종 요약은 존댓말을 사용하세요.
                - [부분1/2]과 같이 부분 내용을 입력으로 받을 경우에도 요약을 꼭 진행하세요.
                출력 형식 규칙:
                - 정확히 1개의 문단으로만 출력하세요.
                - 총 3~6문장으로 간결하게 작성하세요.
                - 제목/머리말/꼬리말/설명/코드블록/번호/불릿을 추가하지 마세요.
                """
            ),
        ),
        MessagesPlaceholder(variable_name="messages"),
    ]
)
llm = ChatGoogleGenerativeAI(model="gemini-3.1-pro-preview", temperature=0, timeout=60, max_retries=2)
generate = prompt | llm


def extract_text_from_html(html_path: str) -> str:
    """HTML 파일에서 본문 텍스트를 추출합니다.

    - 여러 개의 article 블록이 있는 페이지를 모두 합쳐서 추출
    - article이 없으면 main/role=main, 최종적으로 body에서 추출
    - 불필요한 태그(script/style/noscript/header/footer/nav/aside)는 제거
    """
    html = Path(html_path).read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    # 제거 대상 태그 정리
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    # <br>를 줄바꿈으로 보존
    for br in soup.find_all("br"):
        br.replace_with("\n")

    texts: list[str] = []
    articles = soup.find_all("article")
    if articles:
        for a in articles:
            texts.append(a.get_text("\n"))
    else:
        main = soup.find("main") or soup.find(attrs={"role": "main"})
        if main:
            texts.append(main.get_text("\n"))
        else:
            body = soup.body or soup
            texts.append(body.get_text("\n"))

    text = "\n".join(t for t in texts if t)

    # 공백 정제
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    compact = "\n".join(lines)
    return compact


def split_text(text: str, max_chars: int = 15000, overlap: int = 500) -> list:
    """긴 텍스트를 겹침(overlap)을 두고 chunk로 분할합니다."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        chunk = text[start:end]
        chunks.append(chunk)
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks


def summarize_with_prompt(content: str) -> str:
    """정의된 프롬프트에 본문을 Human 메시지로 넣어 요약을 생성합니다."""
    response = generate.invoke({"messages": [HumanMessage(content=content)]})
    if isinstance(response, (AIMessage,)):
        return response.content
    try:
        return response.content  # type: ignore[attr-defined]
    except Exception:
        return str(response)


def translate_title_to_korean(title: str) -> str:
    """영문 제목을 자연스러운 한국어 제목으로 번역합니다."""
    prompt_text = dedent(
        f"""
        다음 영어 제목을 자연스러운 한국어 제목으로 번역하세요.
        - 제목만 출력하세요. 따옴표나 추가 설명은 금지합니다.

        영어 제목:
        {title}
        """
    )
    try:
        response = llm.invoke([HumanMessage(content=prompt_text)])
        if isinstance(response, (AIMessage,)):
            result = response.content
        else:
            try:
                result = response.content  # type: ignore[attr-defined]
            except Exception:
                result = str(response)
        return result.strip().strip('"').strip("'")
    except Exception:
        return title


def summarize_large_text(text: str) -> str:
    """큰 문서를 청크 요약 → 종합 요약(map-reduce)으로 처리합니다."""
    # 요약 파라미터 (기본값 고정)
    max_total_chars = 180_000  # 전체 텍스트 허용 최대 길이. 초과분은 미리 절단해 비용/시간 절감
    max_chars = 15_000         # 청크 하나의 최대 길이. 이 값을 기준으로 원문을 분할
    overlap = 500              # 청크 간 겹침 길이. 문맥 끊김을 줄이기 위해 이전 청크의 일부를 포함
    max_chunks = 12            # 처리할 최대 청크 개수 상한. 과도한 분량 처리 방지
    max_concurrency = 3        # 모델 호출 동시 처리 개수. 병렬 요청 수준 조절

    # 너무 큰 문서는 선제적으로 잘라내기
    if len(text) > max_total_chars:
        print(f"텍스트가 매우 큽니다({len(text)}자). {max_total_chars}자로 절단합니다.")
        text = text[:max_total_chars]

    chunks = split_text(text, max_chars=max_chars, overlap=overlap)
    if len(chunks) > max_chunks:
        print(f"청크가 너무 많습니다({len(chunks)}개). 최초 {max_chunks}개만 사용합니다.")
        chunks = chunks[:max_chunks]

    print(f"청크 개수: {len(chunks)}, 동시성: {max_concurrency}, 청크 길이: ~{max_chars}자")
    if len(chunks) == 1:
        return summarize_with_prompt(chunks[0])

    # 병렬 요약(batch)
    inputs = []
    for idx, chunk in enumerate(chunks, start=1):
        header = f"[부분 {idx}/{len(chunks)}]\n"
        inputs.append({"messages": [HumanMessage(content=header + chunk)]})

    responses = generate.with_config({"max_concurrency": max_concurrency}).batch(inputs)
    partial_summaries = []
    for r in responses:
        if isinstance(r, (AIMessage,)):
            partial_summaries.append(r.content)
        else:
            try:
                partial_summaries.append(r.content)  # type: ignore[attr-defined]
            except Exception:
                partial_summaries.append(str(r))

    combined = "\n\n".join(partial_summaries)
    combine_instruction = dedent(f"""\
    다음은 문서를 분할한 부분 요약들입니다. 중복을 제거하고 핵심만 남겨 최종 1문단 요약을 만드세요.
    요약 내용만 보고도 전체 원문의 핵심 주제가 무엇인지 파악할 수 있도록 핵심 주제를 명확히 파악한뒤 요약을 진행하세요.
    요약을 진행할때 다음의 지침을 따르세요:
    - 전체 원문의 핵심 주제를 명확히 파악한뒤 요약을 진행하세요.
    - 지나치게 지엽적인 내용은 제외하세요.
    - 최종 요약은 존댓말을 사용하세요.
    출력 형식 규칙:
    - 정확히 1개의 문단으로만 출력하세요.
    - 총 3~6문장.
    - 제목/머리말/꼬리말/설명/코드블록/번호/불릿을 금지합니다.

    부분 요약 모음:
    {combined}
    """)
    final_summary = summarize_with_prompt(combine_instruction)
    return final_summary


def summarize_html_file(input_path: Path, output_dir: Path) -> Path:
    """단일 HTML 파일을 요약하고 결과를 .md로 저장합니다."""
    text = extract_text_from_html(str(input_path))
    summary = summarize_large_text(text)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_filename = input_path.stem + "_summary_ko.md"
    output_path = output_dir / output_filename

    original_title = input_path.stem.replace("_", " ")
    translated_title = translate_title_to_korean(original_title)
    header = (
        f"원본 파일: {input_path.name}\n"
        f"원본 제목(영문): {original_title}\n"
        f"번역 제목(한글): {translated_title}\n"
    )
    output_path.write_text(header + summary + "\n", encoding="utf-8")
    return output_path


def process_html_directory(input_dir: Path, output_dir: Path) -> list:
    """디렉터리 내 모든 .html 파일을 처리하고 결과 경로 리스트를 반환합니다."""
    outputs = []
    output_dir.mkdir(parents=True, exist_ok=True)

    for html_file in sorted(input_dir.glob("*.html")):
        expected_output = output_dir / f"{html_file.stem}_summary_ko.md"
        if expected_output.exists():
            print(f"이미 요약결과가 있습니다. (Skip): {expected_output.name}")
            outputs.append(expected_output)
            continue

        print(f"Processing: {html_file.name}")
        out_path = summarize_html_file(html_file, output_dir)
        print(f"Saved: {out_path}")
        outputs.append(out_path)
    return outputs


def main():
    project_root = Path(__file__).parent
    input_dir = project_root / "data" / "html"
    output_dir = project_root / "data" / "html_summary"

    if not input_dir.exists():
        print(f"입력 디렉터리가 없습니다: {input_dir}")
        sys.exit(1)

    results = process_html_directory(input_dir, output_dir)
    print(f"총 {len(results)}개 파일 요약 완료")

if __name__ == "__main__":
    main()