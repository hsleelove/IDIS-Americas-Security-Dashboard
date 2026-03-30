"""
netsuite_harvester.py
=====================
NetSuite 자동 로그인 → 리포트 XLS 다운로드

사용법:
  python netsuite_harvester.py
  python netsuite_harvester.py --output-dir ./data

환경변수:
  NS_EMAIL    : NetSuite 로그인 이메일
  NS_PASSWORD : NetSuite 비밀번호

필요 패키지:
  pip install requests
"""

import os
import time
import argparse
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("pip install requests")
    raise

# ════════════════════════════════════════════════════
#  계정 설정
# ════════════════════════════════════════════════════
ACCOUNT_ID = "4631664"
BASE_URL   = f"https://{ACCOUNT_ID}.app.netsuite.com"
EMAIL      = os.environ.get("NS_EMAIL",    "")
PASSWORD   = os.environ.get("NS_PASSWORD", "")

DELAY_BETWEEN = 6   # 리포트 사이 대기 시간(초)

# ════════════════════════════════════════════════════
#  리포트 목록
# ════════════════════════════════════════════════════
REPORTS = [
    {
        "key":      "opps",
        "name":     "Opps & Quotation",
        "searchid": "3408",
        "searchtype": "Transaction",
        "extra_params": {
            "Transaction_DATECREATEDrange": "CUSTOM",
            "Transaction_DATECREATEDfrom":  "1/1/2024",
            "Transaction_DATECREATEDmodi":  "WITHIN",
            "Transaction_DATECREATED":      "CUSTOM",
            "Transaction_STATUS":           "@ALL@",
            "Transaction_FORECASTTYPE":     "@ALL@",
            "detail":     "IT_CUSTITEMCUSTITEM_CVS_PRODUCT_FAMILY",
            "detailname": "Total",
        },
        "filename": "Opps_and_Quotes_{date}_W{week}.xlsx",
        "format":   "xlsx",
    },
    {
        "key":      "booking",
        "name":     "MTD Booking",
        "searchid": "7165",
        "searchtype": "Transaction",
        "extra_params": {
            "Transaction_DATECREATEDmodi": "WITHIN",
            "Transaction_DATECREATED":     "TY",
            "Transaction_CLASStype":       "ANYOF",
            "Transaction_CLASS":           "@ALL@",
            "detail":     "CUSTBODY_SALESTEAM_ORDER",
            "detailname": "Total",
        },
        "filename": "MTD_booking_{date}_W{week}.xls",
        "format":   "xls",
    },
    {
        "key":      "ytd",
        "name":     "Sales YTD",
        "searchid": "7255",
        "searchtype": "Transaction",
        "extra_params": {
            "Transaction_TRANDATEmodi": "WITHIN",
            "Transaction_TRANDATE":     "TY",
            "Transaction_CLASStype":    "ANYOF",
            "Transaction_CLASS":        "@ALL@",
            "detail":     "AL_CUSTBODY_SALESTEAM_ORDER",
            "detailname": "Total",
        },
        "filename": "Sales_YTD_{date}.xls",
        "format":   "xls",
    },
    {
        "key":      "pf",
        "name":     "Pending Fulfillment",
        "searchid": "7227",
        "searchtype": "Transaction",
        "extra_params": {
            "Transaction_SHIPDATEmodi": "WITHIN",
            "Transaction_SHIPDATE":     "TY",
            "detail":     "CUSTBODY_SALESTEAM_ORDER",
            "detailname": "Total",
        },
        "filename": "Pending_Fulfillment_{date}_W{week}.xls",
        "format":   "xls",
    },
    {
        "key":      "activities",
        "name":     "Sales Activities",
        "searchid": "7349",
        "searchtype": "Calendar",
        "extra_params": {
            "Calendar_DATErange": "CUSTOM",
            "Calendar_DATEfrom":  "1/1/2024",
            "Calendar_DATEmodi":  "WITHIN",
            "Calendar_DATE":      "CUSTOM",
            "detail":     "Calendar_ATTENDEE",
            "detailname": "Total",
        },
        "filename": "Sales_Activities_{date}_W{week}.xls",
        "format":   "xls",
    },
]


# ════════════════════════════════════════════════════
#  로그인
# ════════════════════════════════════════════════════
def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    })
    return s


def login(session):
    if not EMAIL or not PASSWORD:
        raise RuntimeError(
            "환경변수를 설정하세요:\n"
            "  export NS_EMAIL=your@email.com\n"
            "  export NS_PASSWORD=yourpassword"
        )

    print(f"  로그인 중... ({EMAIL})")

    # 로그인 페이지 먼저 열기 (쿠키 초기화)
    session.get(f"{BASE_URL}/pages/loginform.jsp", timeout=30)

    # 로그인 form 제출
    resp = session.post(
        f"{BASE_URL}/pages/loginform.jsp",
        data={
            "username":  EMAIL,
            "password":  PASSWORD,
            "account":   ACCOUNT_ID,
            "role":      "3",
            "redirect2": "/app/center/card.nl",
        },
        timeout=30,
        allow_redirects=True,
    )

    if "loginForm" in resp.url or "Invalid login" in resp.text:
        raise RuntimeError("로그인 실패 — 이메일/비밀번호 확인하세요")

    print("  ✅ 로그인 성공")


# ════════════════════════════════════════════════════
#  다운로드
# ════════════════════════════════════════════════════
def download_report(session, report, output_dir, date_str, week_str):
    today = date.today()

    # 오늘 날짜 (NetSuite 형식: M/D/YYYY)
    today_ns = f"{today.month}/{today.day}/{today.year}"

    # 파라미터 조립
    params = {
        "searchtype": report["searchtype"],
        "searchid":   report["searchid"],
        "style":      "NORMAL",
        "dle":        "F",
    }
    params.update(report["extra_params"])

    # 날짜 끝 값 오늘로 설정
    for k in ["Transaction_DATECREATEDto", "Calendar_DATEto"]:
        if k in params and not params[k]:
            params[k] = today_ns

    # XLS / XLSX 다운로드 파라미터
    if report["format"] == "xlsx":
        params.update({"csv": "T", "OfficeXML": "T"})
    else:
        params.update({"csv": "T", "OfficeXML": "F", "xls": "T"})

    url = f"{BASE_URL}/app/common/search/searchresults.nl?" + urlencode(params)

    filename = report["filename"].format(date=date_str, week=week_str)
    out_path = Path(output_dir) / filename

    print(f"  [{report['name']}] 다운로드 중...", end="", flush=True)

    resp = session.get(url, timeout=120, stream=True)

    if resp.status_code != 200:
        print(f" ❌ HTTP {resp.status_code}")
        return None

    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)

    size_kb = out_path.stat().st_size // 1024
    if size_kb < 1:
        print(f" ⚠️  파일 너무 작음 ({size_kb}KB)")
        return None

    print(f" ✅ {filename} ({size_kb}KB)")
    return str(out_path)


# ════════════════════════════════════════════════════
#  메인
# ════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    today    = date.today()
    date_str = today.strftime("%Y%m%d")
    week_str = f"W{today.isocalendar()[1]:02d}"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print(f"  NetSuite Harvester  {today}  {week_str}")
    print(f"  출력: {output_dir.resolve()}")
    print("=" * 50)

    session = make_session()

    try:
        login(session)
    except RuntimeError as e:
        print(f"\n❌ {e}")
        return {}

    print(f"\n📥 {len(REPORTS)}개 리포트 (각 {DELAY_BETWEEN}초 간격):\n")

    downloaded = {}
    for i, report in enumerate(REPORTS):
        path = download_report(session, report, output_dir, date_str, week_str)
        if path:
            downloaded[report["key"]] = path
        if i < len(REPORTS) - 1:
            time.sleep(DELAY_BETWEEN)

    print(f"\n완료: {len(downloaded)}/{len(REPORTS)}")
    return downloaded


if __name__ == "__main__":
    main()
