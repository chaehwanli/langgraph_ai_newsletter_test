import os
from uuid import uuid4
from datetime import datetime
from pathlib import Path
import sys
import csv
import re
from textwrap import dedent

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_google_genai import ChatGoogleGenerativeAI


# LangSmith 설정
unique_id = uuid4().hex[0:8]
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = f"HN AI Top5 URL Summary - {unique_id}"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
print(unique_id)


prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            dedent(
                """
                아래 영어 원문을 한국어로 번역·요약하세요.
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


def fetch_url(url: str, timeout: int = 20) -> str:
    """URL에서 HTML을 가져옵니다.

    Args:
        url: 다운로드할 대상 URL.
        timeout: 요청 타임아웃(초).

    Returns:
        응답 HTML 문자열. 실패 시 빈 문자열.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        # 인코딩 추정 개선
        if not resp.encoding or resp.encoding.lower() == "ISO-8859-1".lower():
            resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except Exception as e:
        print(f"[WARN] URL fetch 실패: {url} ({e})")
        return ""


def build_jina_reader_url(original_url: str) -> str:
    """Jina Reader(r.jina.ai)용 프록시 URL을 생성합니다."""
    parsed = urlparse(original_url)
    # 스킴은 r.jina.ai/http:// 형태를 사용 (https도 지원)
    netloc_path = f"{parsed.netloc}{parsed.path}"
    if parsed.query:
        netloc_path += f"?{parsed.query}"
    return f"https://r.jina.ai/http://{netloc_path}"


def fetch_via_jina_reader(url: str, timeout: int = 20) -> str:
    """r.jina.ai 리더 모드로 문서를 텍스트 형태로 가져옵니다."""
    proxy_url = build_jina_reader_url(url)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/plain,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
        "Cache-Control": "no-cache",
    }
    try:
        resp = requests.get(proxy_url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        if not resp.encoding or resp.encoding.lower() == "ISO-8859-1".lower():
            resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except Exception as e:
        print(f"[WARN] Jina Reader 폴백도 실패: {proxy_url} ({e})")
        return ""


def get_best_text_from_url(url: str) -> str:
    """원문 fetch → HTML 본문 추출 → 필요 시 Jina Reader 폴백을 적용해 최선의 텍스트를 반환합니다.

    추가 예외처리:
    - 인증/에러/봇차단 페이지로 추정되면 Jina Reader 폴백을 우선 시도.
    - 폴백 후에도 여전히 비정상 본문으로 판단되면 빈 문자열 반환.
    """
    html = fetch_url(url)
    text_primary = extract_text_from_html_content(html) if html else ""

    # 비정상 본문(인증/에러/봇차단 등) 감지 함수
    def detect_irrelevant_reason(text: str, source_url: str | None = None) -> str | None:
        if not text:
            return "본문 없음"
        t = text.lower()
        strong_signals = [
            "404 not found",
            "403 forbidden",
            "500 internal server error",
            "502 bad gateway",
            "503 service unavailable",
            "504 gateway timeout",
            "access denied",
            "unauthorized",
            "your access to this site has been limited",
            "just a moment",
            "checking your browser",
            "please enable cookies",
            "enable javascript",
            "verify you are a human",
            "captcha",
            "help.openai.com",
            "return to homepage",
        ]
        for s in strong_signals:
            if s in t:
                return f"비정상 페이지 감지: '{s}'"
        # 로그인 화면(약한 신호들) 다수 동시 등장 시
        weak_login_signals = [
            "sign in",
            "log in",
            "create account",
            "forgot password",
            "로그인",
            "회원가입",
        ]
        weak_hits = sum(1 for s in weak_login_signals if s in t)
        if weak_hits >= 3:
            return "로그인 화면으로 추정"
        # 과도하게 짧은 본문
        if len(text) < 300:
            return "본문이 매우 짧음"
        return None

    reason_primary = detect_irrelevant_reason(text_primary, url)
    # 본문이 너무 짧거나 비정상으로 의심되면 폴백 시도
    if reason_primary is not None:
        print(f"[INFO] 기본 본문이 부적합({reason_primary}). Jina Reader 폴백을 시도합니다.")
        fallback_text = fetch_via_jina_reader(url)
        text_fallback = extract_text_from_html_content(fallback_text) if "<" in fallback_text else fallback_text
        reason_fallback = detect_irrelevant_reason(text_fallback, url)
        if reason_fallback is None and len(text_fallback) >= max(len(text_primary), 300):
            return text_fallback
        print(f"[WARN] 폴백 후에도 본문 비정상으로 판단되어 스킵합니다: {reason_fallback or '불명확'}")
        return ""

    return text_primary


def extract_text_from_html_content(html: str) -> str:
    """HTML 문자열에서 본문 텍스트를 추출합니다.

    처리 규칙:
    - 여러 개의 article 블록이 있는 경우 모두 합쳐서 추출
    - article이 없으면 main/role=main, 없으면 body에서 추출
    - 불필요한 태그(script/style/noscript/header/footer/nav/aside)는 제거
    - <br>는 줄바꿈으로 보존

    Args:
        html: HTML 원문 문자열.

    Returns:
        라인 단위 공백 정제가 적용된 본문 텍스트.
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

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
    """긴 텍스트를 겹침(overlap)을 두고 chunk로 분할합니다.

    Args:
        text: 원문 텍스트.
        max_chars: 청크 최대 길이.
        overlap: 청크 간 겹침 길이.

    Returns:
        분할된 텍스트 조각 리스트(순서 보장).
    """
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
    """정의된 프롬프트에 본문을 Human 메시지로 넣어 요약을 생성합니다.

    Args:
        content: 요약할 입력 텍스트.

    Returns:
        프롬프트 지침을 따른 한국어 요약 텍스트.
    """
    response = generate.invoke({"messages": [HumanMessage(content=content)]})
    if isinstance(response, (AIMessage,)):
        return response.content
    try:
        return response.content  # type: ignore[attr-defined]
    except Exception:
        return str(response)


def translate_title_to_korean(title: str) -> str:
    """영문 제목을 자연스러운 한국어 제목으로 번역합니다.

    Args:
        title: 영문 제목 문자열.

    Returns:
        따옴표 제거된 자연스러운 한국어 제목. 실패 시 원문 반환.
    """
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
    """큰 문서를 청크 요약 → 종합 요약(map-reduce)으로 처리합니다.

    Args:
        text: 원문 텍스트.

    Returns:
        프롬프트 지침을 따른 최종 1문단 한국어 요약.
    """
    # 요약 파라미터 (기본값 고정)
    max_total_chars = 180_000  # 전체 입력 텍스트의 상한. 비용/시간 절감을 위해 초과분은 사전 절단
    max_chars = 15_000         # 청크 하나의 최대 길이. 너무 크면 모델 컨텍스트에 부담, 너무 작으면 문맥 단절
    overlap = 500              # 청크 간 겹침 길이. 이전 청크의 말미를 포함해 문맥 끊김 완화
    max_chunks = 12            # 처리할 최대 청크 개수. 과도한 분량을 제한해 호출 수/비용 제어
    max_concurrency = 3        # 부분 요약 동시 처리 개수. API 레이트/안정성 고려해 보수적 설정 권장

    if not text or len(text.strip()) < 300:
        return "원문에서 추출된 본문이 부족하거나 접근이 제한되어 요약이 어렵습니다. 원문 링크를 참고해 주세요."

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


def make_safe_slug(text: str, fallback: str = "item") -> str:
    """주어진 텍스트로 파일명에 안전한 슬러그를 생성합니다.

    Args:
        text: 원본 텍스트(제목 등). 비어 있으면 fallback 사용.
        fallback: 유효한 슬러그를 만들 수 없을 때 대체 문자열.

    Returns:
        영문 소문자와 숫자, 하이픈만 포함하는 최대 80자 슬러그.
    """
    text = text.strip() if text else fallback
    text = re.sub(r"[\s\-\_]+", " ", text)
    text = text[:80]
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or fallback


def summarize_url_to_markdown(url: str, title: str, out_dir: Path, prefix: str = "") -> Path | None:
    """URL을 요약하여 마크다운 파일로 저장합니다.

    Args:
        url: 원문 문서의 URL.
        title: 문서 제목(가능하면 영문). 파일명 슬러그 및 한글 번역 제목에 사용.
        out_dir: 결과 마크다운을 저장할 디렉터리.
        prefix: 파일명 앞에 붙일 접두사(예: "01_123456_").

    Returns:
        생성된 마크다운 파일 경로. 실패 시 None.
    """
    content = get_best_text_from_url(url)
    # 비정상/부적합 본문은 스킵
    if not content or len(content.strip()) < 300:
        print(f"[SKIP] 본문이 부적합하여 저장을 건너뜁니다: {url}")
        return None

    summary = summarize_large_text(content)

    out_dir.mkdir(parents=True, exist_ok=True)

    translated_title = translate_title_to_korean(title or "")
    slug = make_safe_slug(title or url, fallback="article")
    fname = f"{prefix}{slug}_summary_ko.md" if prefix else f"{slug}_summary_ko.md"
    output_path = out_dir / fname

    header = (
        f"원본 URL: {url}\n"
        f"원본 제목(영문): {title}\n"
        f"번역 제목(한글): {translated_title}\n"
    )
    output_path.write_text(header + summary + "\n", encoding="utf-8")
    return output_path


def process_csv(input_csv: Path, output_dir: Path) -> list[Path]:
    """CSV를 읽어 각 URL을 요약하고 결과 파일 목록을 반환합니다.

    기대하는 CSV 헤더: id, title, url, rank 등.

    Args:
        input_csv: 입력 CSV 경로.
        output_dir: 결과 마크다운 저장 디렉터리.

    Returns:
        생성되었거나 기존에 존재하여 스킵된 마크다운 파일 경로 리스트.
    """
    outputs: list[Path] = []
    if not input_csv.exists():
        print(f"입력 CSV가 없습니다: {input_csv}")
        return outputs

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        url = (row.get("url") or "").strip()
        title = (row.get("title") or "").strip()
        rank = (row.get("rank") or "").strip()
        _id = (row.get("id") or "").strip()

        if not url:
            print("URL이 비어있어 스킵합니다.")
            continue

        safe_prefix = ""
        if rank and _id:
            safe_prefix = f"{int(rank):02d}_{_id}_"
        elif _id:
            safe_prefix = f"{_id}_"
        elif rank:
            safe_prefix = f"{int(rank):02d}_"

        slug = make_safe_slug(title or url, fallback="article")
        expected = output_dir / (f"{safe_prefix}{slug}_summary_ko.md" if safe_prefix else f"{slug}_summary_ko.md")
        if expected.exists():
            print(f"이미 요약결과가 있습니다. (Skip): {expected.name}")
            outputs.append(expected)
            continue

        print(f"Processing: rank={rank or '-'} id={_id or '-'} title={title or url}")
        try:
            out_path = summarize_url_to_markdown(url=url, title=title, out_dir=output_dir, prefix=safe_prefix)
            if out_path:
                print(f"Saved: {out_path}")
                outputs.append(out_path)
        except Exception as e:
            print(f"[ERROR] 처리 실패: {_id or '-'} ({e})")
            continue

    return outputs


def find_latest_ai_top5_csv(data_dir: Path) -> Path | None:
    """가장 최근 날짜의 AI Top5 CSV를 찾습니다.

    파일명 패턴: hacker_news_topstories_last_7_days_ai_top5_YYYYMMDD.csv

    Args:
        data_dir: CSV 파일들이 위치한 상위 디렉터리.

    Returns:
        가장 최근 날짜의 CSV 경로. 없으면 None.
    """
    latest_path: Path | None = None
    latest_date: datetime | None = None
    for csv_path in data_dir.glob("hacker_news_topstories_last_7_days_ai_top5_*.csv"):
        m = re.search(r"(\d{8})(?=\.csv$)", csv_path.name)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d")
        except Exception:
            continue
        if latest_date is None or d > latest_date:
            latest_date = d
            latest_path = csv_path
    return latest_path


def main():
    """가장 최근 날짜의 AI Top5 CSV를 자동 선택해 URL 요약을 수행합니다."""
    project_root = Path(__file__).parent
    data_dir = project_root / "data"
    output_dir = project_root / "data" / "hn_url_summary"

    # 최신 CSV 자동 선택
    input_csv = find_latest_ai_top5_csv(data_dir)
    if not input_csv:
        print(f"자동 선택할 CSV를 찾지 못했습니다: {data_dir}/hacker_news_topstories_last_7_days_ai_top5_*.csv")
        sys.exit(1)

    results = process_csv(input_csv, output_dir)
    print(f"총 {len(results)}개 URL 요약 완료")


if __name__ == "__main__":
    main()
