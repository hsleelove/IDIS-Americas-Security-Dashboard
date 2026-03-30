"""
netsuite_harvester.py
=====================
Selenium으로 NetSuite 자동 로그인 → Excel 다운로드

필요 패키지:
  pip install selenium webdriver-manager
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
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("pip install selenium webdriver-manager")
    raise

# ════════════════════════════════════════════════════
#  계정 설정
# ════════════════════════════════════════════════════
ACCOUNT_ID = "4631664"
LOGIN_URL  = "https://system.netsuite.com/pages/customerlogin.jsp?country=US"
EMAIL      = os.environ.get("NS_EMAIL",    "")
PASSWORD   = os.environ.get("NS_PASSWORD", "")

DELAY_BETWEEN  = 8    # 리포트 사이 대기 (초)
PAGE_LOAD_WAIT = 25   # 페이지 로딩 대기 (초)

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
            "?searchtype=Transaction"
            "&Transaction_DATECREATEDrange=CUSTOM"
            "&Transaction_DATECREATEDfrom=1%2F1%2F2024"
            "&Transaction_DATECREATEDfromrel_formattedValue="
            "&Transaction_DATECREATEDfromrel="
            "&Transaction_DATECREATEDfromreltype=DAGO"
            "&Transaction_DATECREATEDto=12%2F31%2F2026"
            "&Transaction_DATECREATEDtorel_formattedValue="
            "&Transaction_DATECREATEDtorel="
            "&Transaction_DATECREATEDtoreltype=DAGO"
            "&Transaction_STATUS=%40ALL%40"
            "&Transaction_FORECASTTYPE=%40ALL%40"
            "&IT_CUSTITEM_BRAND=%40ALL%40"
            "&IT_CUSTITEM_PRODUCT_SERIES=%40ALL%40"
            "&style=NORMAL"
            "&Transaction_DATECREATEDmodi=WITHIN"
            "&Transaction_DATECREATED=CUSTOM"
            "&searchid=3408"
            "&dle=F"
            "&sortcol=Transaction_NAME_raw"
            "&sortdir=ASC"
            "&detail=IT_CUSTITEMCUSTITEM_CVS_PRODUCT_FAMILY"
            "&detailname=Total"
            "&IT_CUSTITEMCUSTITEM_CVS_PRODUCT_FAMILY=%40PRESERVE%40"
            "&IT_CUSTITEMCUSTITEM_CVS_PRODUCT_FAMILYtype=ANYOF"
            "&twbx=F"
        ),
        "filename": "Opps_and_Quotes_{date}_W{week}.xlsx",
    },
    {
        "key":  "booking",
        "name": "MTD Booking",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchtype=Transaction"
            "&CUSTBODY_SALESTEAM_ORDERtype=ANYOF"
            "&CUSTBODY_SALESTEAM_ORDER=%40PRESERVE%40"
            "&detail=CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
            "&searchid=7165"
            "&Transaction_DATECREATEDmodi=WITHIN"
            "&Transaction_DATECREATED=TY"
            "&Transaction_CLASStype=ANYOF"
            "&Transaction_CLASS=%40ALL%40"
        ),
        "filename": "MTD_booking_{date}_W{week}.xls",
    },
    {
        "key":  "ytd",
        "name": "Sales YTD",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchtype=Transaction"
            "&AL_CUSTBODY_SALESTEAM_ORDERtype=ANYOF"
            "&AL_CUSTBODY_SALESTEAM_ORDER=%40PRESERVE%40"
            "&detail=AL_CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
            "&searchid=7255"
            "&Transaction_TRANDATEmodi=WITHIN"
            "&Transaction_TRANDATE=TY"
            "&Transaction_CLASStype=ANYOF"
            "&Transaction_CLASS=%40ALL%40"
            "&AL_Transaction_SALESREPtype=ANYOF"
            "&AL_Transaction_SALESREP=%40ALL%40"
        ),
        "filename": "Sales_YTD_{date}.xls",
    },
    {
        "key":  "pf",
        "name": "Pending Fulfillment",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchtype=Transaction"
            "&CUSTBODY_SALESTEAM_ORDERtype=ANYOF"
            "&CUSTBODY_SALESTEAM_ORDER=%40PRESERVE%40"
            "&detail=CUSTBODY_SALESTEAM_ORDER"
            "&detailname=Total"
            "&searchid=7227"
            "&Transaction_SHIPDATEmodi=WITHIN"
            "&Transaction_SHIPDATE=TY"
        ),
        "filename": "Pending_Fulfillment_{date}_W{week}.xls",
    },
    {
        "key":  "activities",
        "name": "Sales Activities",
        "url":  (
            "https://4631664.app.netsuite.com/app/common/search/searchresults.nl"
            "?searchtype=Calendar"
            "&Calendar_DATErange=CUSTOM"
            "&Calendar_DATEfrom=1%2F1%2F2024"
            "&Calendar_DATEfromrel_formattedValue="
            "&Calendar_DATEfromrel="
            "&Calendar_DATEfromreltype=DAGO"
            "&Calendar_DATEto=12%2F31%2F2026"
            "&Calendar_DATEtorel_formattedValue="
            "&Calendar_DATEtorel="
            "&Calendar_DATEtoreltype=DAGO"
            "&EN_CUSTENTITY_SALESTEAM_REP=%40ALL%40"
            "&style=NORMAL"
            "&Calendar_DATEmodi=WITHIN"
            "&Calendar_DATE=CUSTOM"
            "&searchid=7349"
            "&dle=F"
            "&sortcol=Calendar_INTERNALID_raw"
            "&sortdir=DESC"
            "&detail=Calendar_ATTENDEE"
            "&detailname=Total"
            "&Calendar_ATTENDEE=%40PRESERVE%40"
            "&Calendar_ATTENDEEtype=ANYOF"
            "&twbx=F"
        ),
        "filename": "Sales_Activities_{date}_W{week}.xls",
    },
]


# ════════════════════════════════════════════════════
#  브라우저 설정
# ════════════════════════════════════════════════════
def make_driver(download_dir):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
    dl_path = str(Path(download_dir).resolve())
    options.add_experimental_option("prefs", {
        "download.default_directory":               dl_path,
        "download.prompt_for_download":             False,
        "download.directory_upgrade":               True,
        "safebrowsing.enabled":                     False,
        "safebrowsing.disable_download_protection": True,
    })
    print("  ChromeDriver 준비 중...")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)
    print("  Chrome 실행 완료")
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
    driver.get(LOGIN_URL)
    time.sleep(3)
    print(f"  로그인 페이지: {driver.current_url}")

    wait = WebDriverWait(driver, PAGE_LOAD_WAIT)

    # 이메일 입력 — 여러 ID 후보 시도
    email_el = None
    for eid in ["email", "Email", "userName", "username"]:
        try:
            email_el = driver.find_element(By.ID, eid)
            print(f"  email 필드 찾음 (ID: {eid})")
            break
        except: pass
    if not email_el:
        try:
            email_el = driver.find_element(By.XPATH, "//input[@type='email' or @name='email' or @name='userName']")
        except:
            src = driver.page_source[:600]
            raise RuntimeError(f"email 필드 못 찾음\n페이지 소스:\n{src}")

    email_el.clear()
    email_el.send_keys(EMAIL)

    # 비밀번호 입력
    pw_el = None
    for pid in ["password", "Password", "pass"]:
        try:
            pw_el = driver.find_element(By.ID, pid)
            break
        except: pass
    if not pw_el:
        try:
            pw_el = driver.find_element(By.XPATH, "//input[@type='password']")
        except:
            raise RuntimeError("password 필드 못 찾음")
    pw_el.clear()
    pw_el.send_keys(PASSWORD)
    print("  비밀번호 입력 완료")

    # 로그인 버튼 클릭
    submit_el = None
    for sid in ["submitButton", "submit", "loginButton", "Login"]:
        try:
            submit_el = driver.find_element(By.ID, sid)
            break
        except: pass
    if not submit_el:
        try:
            submit_el = driver.find_element(By.XPATH, "//input[@type='submit']")
        except:
            try:
                submit_el = driver.find_element(By.XPATH, "//button[@type='submit']")
            except:
                raise RuntimeError("로그인 버튼 못 찾음")
    submit_el.click()
    print("  로그인 버튼 클릭")

    time.sleep(6)
    print(f"  로그인 후 URL: {driver.current_url}")

    if "loginform" in driver.current_url or "customerlogin" in driver.current_url:
        try:
            err = driver.find_element(By.CLASS_NAME, "errMsg").text
            raise RuntimeError(f"로그인 실패: {err}")
        except RuntimeError:
            raise
        except:
            raise RuntimeError(f"로그인 실패 — URL: {driver.current_url}")

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
