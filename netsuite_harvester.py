"""
netsuite_harvester.py
=====================
Selenium으로 NetSuite 브라우저 자동조작 → Excel 다운로드

필요 패키지:
  pip install selenium
"""

import os
import time
import argparse
from datetime import date
from pathlib import Path

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("pip install selenium")
    raise

# ════════════════════════════════════════════════════
#  설정
# ════════════════════════════════════════════════════
ACCOUNT_ID = "4631664"
BASE_URL   = f"https://{ACCOUNT_ID}.app.netsuite.com"
EMAIL      = os.environ.get("NS_EMAIL",    "")
PASSWORD   = os.environ.get("NS_PASSWORD", "")

DELAY_BETWEEN  = 8
PAGE_LOAD_WAIT = 25

# Excel Export 버튼 XPath (모든 리포트 동일)
EXCEL_BTN_XPATH = (
    "/html/body/div[1]/div[4]/form[1]/div[2]"
    "/table[2]/tbody/tr/td[1]/table/tbody/tr/td[2]/div"
)

# ════════════════════════════════════════════════════
#  리포트 목록
# ════════════════════════════════════════════════════
REPORTS = [
    {
        "key":  "opps",
        "name": "Opps & Quotation",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchid=3408"
            "&Transaction_DATECREATEDmodi=WITHIN"
            "&Transaction_DATECREATED=CUSTOM"
            "&Transaction_DATECREATEDrange=CUSTOM"
            "&Transaction_DATECREATEDfrom=1%2F1%2F2024"
            "&Transaction_STATUS=%40ALL%40"
            "&Transaction_FORECASTTYPE=%40ALL%40"
            "&detail=IT_CUSTITEMCUSTITEM_CVS_PRODUCT_FAMILY"
            "&detailname=Total"
        ),
        "filename": "Opps_and_Quotes_{date}_W{week}.xlsx",
    },
    {
        "key":  "booking",
        "name": "MTD Booking",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchid=7165"
            "&Transaction_DATECREATEDmodi=WITHIN"
            "&Transaction_DATECREATED=TY"
            "&Transaction_CLASStype=ANYOF"
            "&Transaction_CLASS=%40ALL%40"
            "&detail=CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
        ),
        "filename": "MTD_booking_{date}_W{week}.xls",
    },
    {
        "key":  "ytd",
        "name": "Sales YTD",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchid=7255"
            "&Transaction_TRANDATEmodi=WITHIN"
            "&Transaction_TRANDATE=TY"
            "&Transaction_CLASStype=ANYOF"
            "&Transaction_CLASS=%40ALL%40"
            "&detail=AL_CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
        ),
        "filename": "Sales_YTD_{date}.xls",
    },
    {
        "key":  "pf",
        "name": "Pending Fulfillment",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchid=7227"
            "&Transaction_SHIPDATEmodi=WITHIN"
            "&Transaction_SHIPDATE=TY"
            "&detail=CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
        ),
        "filename": "Pending_Fulfillment_{date}_W{week}.xls",
    },
    {
        "key":  "activities",
        "name": "Sales Activities",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchid=7349"
            "&Calendar_DATEmodi=WITHIN"
            "&Calendar_DATE=CUSTOM"
            "&Calendar_DATErange=CUSTOM"
            "&Calendar_DATEfrom=1%2F1%2F2024"
            "&detail=Calendar_ATTENDEE"
            "&detailname=Total"
        ),
        "filename": "Sales_Activities_{date}_W{week}.xls",
    },
]


# ════════════════════════════════════════════════════
#  브라우저 설정 — GitHub Actions 서버 전용
# ════════════════════════════════════════════════════
def make_driver(download_dir):
    options = Options()
    options.add_argument("--headless=new")          # 새 headless 모드
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )

    # 다운로드 폴더 설정
    dl_path = str(Path(download_dir).resolve())
    options.add_experimental_option("prefs", {
        "download.default_directory":   dl_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade":   True,
        "safebrowsing.enabled":         False,
        "safebrowsing.disable_download_protection": True,
    })

    # GitHub Actions에 설치된 Chrome 직접 사용
    # (webdriver-manager 없이)
    driver = webdriver.Chrome(options=options)
    return driver


# ════════════════════════════════════════════════════
#  로그인
# ════════════════════════════════════════════════════
def login(driver):
    if not EMAIL or not PASSWORD:
        raise RuntimeError(
            "환경변수를 설정하세요:\n"
            "  NS_EMAIL, NS_PASSWORD"
        )

    print(f"  로그인 중... ({EMAIL})")
    driver.get(f"{BASE_URL}/pages/loginform.jsp")

    wait = WebDriverWait(driver, PAGE_LOAD_WAIT)
    wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(EMAIL)
    driver.find_element(By.ID, "password").send_keys(PASSWORD)
    driver.find_element(By.ID, "submitButton").click()

    time.sleep(6)

    if "loginForm" in driver.current_url or "login" in driver.current_url.lower():
        raise RuntimeError("로그인 실패 — 이메일/비밀번호 확인하세요")

    print("  ✅ 로그인 성공")


# ════════════════════════════════════════════════════
#  Excel 다운로드
# ════════════════════════════════════════════════════
def download_excel(driver, report, output_dir, date_str, week_str):
    name     = report["name"]
    filename = report["filename"].format(date=date_str, week=week_str)
    out_path = Path(output_dir) / filename

    print(f"  [{name}] 로딩...", end="", flush=True)

    driver.get(report["url"])

    wait = WebDriverWait(driver, PAGE_LOAD_WAIT)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, EXCEL_BTN_XPATH)))
    except Exception as e:
        print(f" ❌ 버튼 없음 (URL: {driver.current_url[:80]})")
        return None

    # 다운로드 전 스냅샷
    before = set(Path(output_dir).glob("*"))

    # 버튼 클릭
    try:
        btn = driver.find_element(By.XPATH, EXCEL_BTN_XPATH)
        driver.execute_script("arguments[0].click();", btn)
        print(f" 클릭...", end="", flush=True)
    except Exception as e:
        print(f" ❌ 클릭 실패: {e}")
        return None

    # 다운로드 완료 대기 (최대 60초)
    for _ in range(60):
        time.sleep(1)
        after     = set(Path(output_dir).glob("*"))
        new_files = [
            f for f in (after - before)
            if not str(f).endswith((".crdownload", ".tmp"))
        ]
        if new_files:
            downloaded = new_files[0]
            downloaded.rename(out_path)
            size_kb = out_path.stat().st_size // 1024
            print(f" ✅ {filename} ({size_kb}KB)")
            return str(out_path)

    print(f" ❌ 타임아웃")
    return None


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

    driver = make_driver(output_dir)

    try:
        login(driver)

        print(f"\n📥 {len(REPORTS)}개 리포트:\n")
        downloaded = {}

        for i, report in enumerate(REPORTS):
            path = download_excel(driver, report, output_dir, date_str, week_str)
            if path:
                downloaded[report["key"]] = path
            if i < len(REPORTS) - 1:
                print(f"     {DELAY_BETWEEN}초 대기...")
                time.sleep(DELAY_BETWEEN)

        print(f"\n완료: {len(downloaded)}/{len(REPORTS)}")
        return downloaded

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
