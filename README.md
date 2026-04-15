## OpenAI Cookbook 수집/정렬/중복제거

- 수집 스크립트: `scripts/scrape_cookbook_week.py`
- 정렬 스크립트: `scripts/sort_csv_by_date_desc.py`
- 중복제거(제목 기반): `scripts/dedupe_csv_by_title.py`

### 1) 설치
```powershell
python -m pip install -r requirements.txt
```

### 2) 수집
- 최근 N일 수집:
```powershell
python scripts\scrape_cookbook_week.py --mode recent --days 7 --out data\openai_cookbook_last_7_days.csv
```
- 전체 수집(전역, 진행 로그 표시):
```powershell
python scripts\scrape_cookbook_week.py --mode all --since 2022-09-12 --max-pages 5000 --max-depth 6 --progress --progress-interval 25 --out data\openai_cookbook_all.csv
```

### 3) 정렬(내림차순)
```powershell
python scripts\sort_csv_by_date_desc.py data\openai_cookbook_all.csv --output data\openai_cookbook_all_sorted.csv
```

### 4) 중복 제거(제목 기반)
```powershell
python scripts\dedupe_csv_by_title.py data\openai_cookbook_all.csv --output data\openai_cookbook_all_dedup.csv
```

> 비고: 수집 스크립트의 ALL 모드는 저장 전에 내부적으로도 제목 기반 중복제거 후 `date` 내림차순으로 저장합니다.

