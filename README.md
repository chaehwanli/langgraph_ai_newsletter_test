## OpenAI Cookbook 수집/정렬/중복제거

- 수집 스크립트: `scripts/scrape_cookbook_week.py`
- 정렬 스크립트: `scripts/sort_csv_by_date_desc.py`
- 중복제거(제목 기반): `scripts/dedupe_csv_by_title.py`

### 0) Python 가상 환경 설정

프로젝트 의존성을 시스템 Python과 분리하기 위해 가상 환경 사용을 권장합니다.

#### 가상 환경 생성
프로젝트 루트에서 `.venv` 라는 이름의 가상 환경을 생성합니다.
```powershell
python -m venv .venv
```

#### 가상 환경 활성화

- **Windows (PowerShell)**
```powershell
.\.venv\Scripts\Activate.ps1
```
> ⚠️ PowerShell 실행 정책 오류가 발생하면 아래 명령을 먼저 실행하세요:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

- **Windows (CMD)**
```cmd
.venv\Scripts\activate.bat
```

- **macOS / Linux**
```bash
source .venv/bin/activate
```

#### 가상 환경 비활성화
활성화된 가상 환경에서 나오려면 아래 명령을 실행합니다.
```powershell
deactivate
```

#### 가상 환경 확인
현재 사용 중인 Python이 가상 환경의 것인지 확인합니다.
```powershell
python --version
pip --version
```
> 경로에 `.venv`가 포함되어 있으면 정상적으로 가상 환경이 활성화된 상태입니다.

---

### 1) 설치
가상 환경을 활성화한 뒤, 의존성을 설치합니다.
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

