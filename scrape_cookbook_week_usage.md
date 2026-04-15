## OpenAI Cookbook 수집 스크립트 사용법 (`scripts/scrape_cookbook_week.py`)

### 개요
`scrape_cookbook_week.py`는 OpenAI Cookbook(`https://cookbook.openai.com/`)의 글을 CSV로 저장합니다.
- `recent` 모드: 최근 N일 포스트만 수집
- `all` 모드: 사이트 전역에서 가능한 모든 포스트 수집(사이트 구조 변화에 대비한 폴백 내장)

CSV 컬럼: `date,title,url`

### 설치
```powershell
python -m pip install -r requirements.txt
```

### 기본 실행 예시
- 최근 7일 수집:
```powershell
python scripts\scrape_cookbook_week.py --mode recent --days 7 --out data\openai_cookbook_last_7_days.csv
```

- 전체 수집(전역):
```powershell
python scripts\scrape_cookbook_week.py --mode all --since 2022-09-12 --max-pages 5000 --max-depth 6 --progress --progress-interval 25 --out data\openai_cookbook_all.csv
```

### 옵션 설명
- `--mode {recent,all}`: 수집 모드 선택. 기본값 `recent`
- `--days N`: `recent` 모드에서 최근 N일 수집(기본 7)
- `--since YYYY-MM-DD`: `all` 모드에서 이 날짜 이후만 포함(예: `2022-09-12`)
- `--out PATH`: 결과 CSV 경로. 
  - `recent` 기본: `data/openai_cookbook_last_7_days.csv`
  - `all` 기본: `data/openai_cookbook_all.csv`
- `--base-url URL`: 기본 사이트 루트(기본: `https://cookbook.openai.com/`)
- `--max-pages N`: `all` 모드 크롤 시 최대 방문 페이지 수(기본 2000)
- `--max-depth N`: `all` 모드 크롤 시 링크 탐색 최대 깊이(기본 4)
- `--progress`: 진행상황 로그 출력 활성화
- `--progress-interval N`: 진행 로그 출력 주기(방문 페이지 기준, 기본 25)

### 동작 개요
1. `recent` 모드
   - 홈에서 날짜 패턴(예: `Aug 7, 2025`)을 찾아 최근 N일 포스트만 저장
2. `all` 모드
   - 1) `sitemap.xml` 시도 → 실패 시 2) 홈의 `__NEXT_DATA__` JSON 파싱 → 부족 시 3) 내부 링크 BFS 크롤
   - 저장 전 URL 정규화 및 중복 제거 후 `date` 내림차순 저장

### 진행 로그 및 종료 조건
- `--progress` 사용 시 `[PROGRESS] visited=... queued=... collected=... rate=... ETA=...` 형태로 STDERR 출력
- 종료 조건:
  - 큐(queued)가 0이면 링크 소진으로 종료
  - `visited`가 `--max-pages`에 도달하면 상한 도달로 종료

### 후처리 도구
- CSV 정렬 전용:
```powershell
python scripts\sort_csv_by_date_desc.py data\openai_cookbook_all.csv --output data\openai_cookbook_all_sorted.csv
```

- CSV 중복 제거 전용:
```powershell
python scripts\dedupe_csv_by_url.py data\openai_cookbook_all.csv --output data\openai_cookbook_all_dedup.csv
```

### 참고
- 수집 대상: [OpenAI Cookbook](https://cookbook.openai.com/)
- 사이트 구조가 바뀔 수 있으므로, 수집 결과가 적다면 `--max-depth`, `--max-pages`를 조정하고 `--since`를 활용하세요.


