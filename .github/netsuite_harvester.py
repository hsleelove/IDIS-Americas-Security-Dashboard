"""
netsuite_harvester.py
=====================
NetSuite 저장된 리포트 URL → 파일 자동 다운로드

사용법:
  python netsuite_harvester.py
  python netsuite_harvester.py --output-dir ./data

설정:
  아래 NETSUITE_CONFIG 에 계정 정보와 리포트 URL을 입력하세요.
  비밀번호는 환경변수로 관리하는 것을 권장합니다:
    export NS_EMAIL=your@email.com
    export NS_PASSWORD=yourpassword

필요 패키지:
  pip install requests
"""

import os
import time
import argparse
from datetime import date, datetime
from pathlib import Path

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("필요 패키지 설치: pip install requests")
    raise


# ════════════════════════════════════════════════════════════════
#  설정 — 여기를 수정하세요
# ════════════════════════════════════════════════════════════════
NETSUITE_CONFIG = {
    # NetSuite 계정 정보
    "account_id": "YOUR_ACCOUNT_ID",       # 예: 1234567
    "email":      os.environ.get("NS_EMAIL",    "your@email.com"),
    "password":   os.environ.get("NS_PASSWORD", ""),
    "role":       "3",                     # 기본 Administrator role

    # 저장된 리포트들 (NetSuite → Reports → Saved Searches 에서 URL 복사)
    "reports": {
        "opps":       {
            "url":      "https://YOUR_ACCOUNT.app.netsuite.com/app/reporting/reportrunner.nl?reporttype=OPPORTUNITYPIPELINE&SAVEDREPORTID=XXXXX",
            "filename": "Opps_and_Quotes_{date}.xlsx",
            "format":   "xlsx",
        },
        "booking":    {
            "url":      "https://YOUR_ACCOUNT.app.netsuite.com/app/reporting/reportrunner.nl?reporttype=SALESORDER&SAVEDREPORTID=XXXXX",
            "filename": "MTD_booking_{date}_{week}.xls",
            "format":   "xls",
        },
        "ytd":        {
            "url":      "https://YOUR_ACCOUNT.app.netsuite.com/app/reporting/reportrunner.nl?reporttype=ITEMFULFILLMENT&SAVEDREPORTID=XXXXX",
            "filename": "Sales_YTD_{date}.xls",
            "format":   "xls",
        },
        "pf":         {
            "url":      "https://YOUR_ACCOUNT.app.netsuite.com/app/reporting/reportrunner.nl?reporttype=SALESORDER&SAVEDREPORTID=XXXXX",
            "filename": "Pending_Fulfillment_{date}_{week}.xls",
            "format":   "xls",
        },
        "activities": {
            "url":      "https://YOUR_ACCOUNT.app.netsuite.com/app/reporting/reportrunner.nl?reporttype=ACTIVITY&SAVEDREPORTID=XXXXX",
            "filename": "Sales_Activities_{date}_{week}.xls",
            "format":   "xls",
        },
    },
}

# ── 다운로드 설정 ────────────────────────────────────────────────
DOWNLOAD_TIMEOUT   = 120   # 초 (리포트 생성 시간 여유)
RETRY_COUNT        = 3
RETRY_DELAY        = 5     # 초


# ════════════════════════════════════════════════════════════════
#  NetSuite 세션 로그인
# ════════════════════════════════════════════════════════════════
def make_session():
    """재시도 설정이 있는 requests 세션 생성"""
    s = requests.Session()
    retry = Retry(total=RETRY_COUNT, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "Mozilla/5.0 IDIS-Dashboard-Updater/1.0"})
    return s


def login(session, config):
    """
    NetSuite에 로그인하고 세션 쿠키 획득.

    NetSuite는 form-based login을 사용합니다.
    회사 SSO(Okta 등)를 쓰는 경우 이 방식이 동작하지 않을 수 있습니다.
    그 경우 Token-Based Authentication(TBA) 또는 OAuth 2.0을 사용하세요.
    """
    login_url = f"https://system.netsuite.com/pages/loginform.jsp"
    payload = {
        "username":   config["email"],
        "password":   config["password"],
        "account":    config["account_id"],
        "role":       config["role"],
        "redirect2":  "/app/center/card.nl?sc=-29",
    }

    print(f"  NetSuite 로그인 중... ({config['email']})")
    resp = session.post(login_url, data=payload, timeout=30, allow_redirects=True)

    if resp.status_code != 200:
        raise RuntimeError(f"로그인 실패: HTTP {resp.status_code}")
    if "Invalid login" in resp.text or "loginForm" in resp.url:
        raise RuntimeError("로그인 실패: 이메일/비밀번호를 확인하세요.")

    print("  ✅ 로그인 성공")
    return session


# ════════════════════════════════════════════════════════════════
#  리포트 다운로드
# ════════════════════════════════════════════════════════════════
def download_report(session, name, report_config, output_dir, date_str, week_tag):
    """단일 리포트 다운로드"""
    url      = report_config["url"]
    fmt      = report_config.get("format", "xlsx")
    fname    = report_config["filename"].format(date=date_str, week=week_tag)
    out_path = Path(output_dir) / fname

    # 다운로드 포맷 파라미터 추가
    if "?" in url:
        dl_url = url + f"&csv=T&OfficeXML=T" if fmt == "xlsx" else url + "&xls=T"
    else:
        dl_url = url + f"?csv=T&OfficeXML=T" if fmt == "xlsx" else url + "?xls=T"

    print(f"  [{name}] 다운로드 중...", end="", flush=True)

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = session.get(dl_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            if resp.status_code == 200:
                with open(out_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                size_kb = out_path.stat().st_size // 1024
                print(f" ✅ {fname} ({size_kb} KB)")
                return str(out_path)
            else:
                print(f" ⚠️  HTTP {resp.status_code} (시도 {attempt}/{RETRY_COUNT})")
        except requests.RequestException as e:
            print(f" ⚠️  연결 오류: {e} (시도 {attempt}/{RETRY_COUNT})")

        if attempt < RETRY_COUNT:
            time.sleep(RETRY_DELAY)

    raise RuntimeError(f"[{name}] 다운로드 실패 ({RETRY_COUNT}회 재시도)")


# ════════════════════════════════════════════════════════════════
#  메인
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="NetSuite 리포트 자동 다운로드")
    parser.add_argument("--output-dir", default="./data",
                        help="다운로드 폴더 (기본: ./data)")
    parser.add_argument("--reports",    default="all",
                        help="다운로드할 리포트 (쉼표 구분, 기본: all)\n"
                             "예: opps,booking,ytd")
    args = parser.parse_args()

    today    = date.today()
    date_str = today.strftime("%Y%m%d")
    week_tag = f"W{today.isocalendar()[1]:02d}"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 다운로드 대상 리포트 선택
    config   = NETSUITE_CONFIG
    all_keys = list(config["reports"].keys())
    if args.reports == "all":
        targets = all_keys
    else:
        targets = [k.strip() for k in args.reports.split(",") if k.strip() in all_keys]

    print("=" * 55)
    print("  NetSuite Report Harvester")
    print(f"  날짜: {today}  |  주차: {week_tag}")
    print(f"  출력: {output_dir.resolve()}")
    print("=" * 55)

    session = make_session()

    # 로그인
    try:
        login(session, config)
    except RuntimeError as e:
        print(f"\n❌ {e}")
        return

    # 리포트 다운로드
    downloaded = {}
    print(f"\n📥 {len(targets)}개 리포트 다운로드:")
    for key in targets:
        if key not in config["reports"]:
            print(f"  [{key}] ❌ 설정 없음")
            continue
        try:
            path = download_report(session, key, config["reports"][key],
                                   output_dir, date_str, week_tag)
            downloaded[key] = path
        except RuntimeError as e:
            print(f"  ❌ {e}")

    print(f"\n✅ 완료: {len(downloaded)}/{len(targets)} 리포트 다운로드")
    return downloaded


if __name__ == "__main__":
    main()
