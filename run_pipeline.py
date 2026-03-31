"""
run_pipeline.py
===============
전체 파이프라인 실행 스크립트:
  1. NetSuite에서 엑셀 파일 다운로드  (netsuite_harvester.py)
  2. 엑셀 파싱 → 대시보드 데이터 생성  (update_dashboard.py)
  3. dashboard_data.json 으로 저장     → 공개 URL에 업로드

사용법:
  python run_pipeline.py                  # 전체 실행
  python run_pipeline.py --skip-download  # 다운로드 건너뜀 (기존 파일 사용)
  python run_pipeline.py --upload-s3      # S3에 업로드
  python run_pipeline.py --upload-github  # GitHub Pages에 업로드

GitHub Actions 에서:
  이 파일을 .github/workflows/update.yml 의 run: 항목에 추가하세요.

필요 패키지:
  pip install requests pandas openpyxl lxml boto3   # S3 업로드 시
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import date

# ── 같은 폴더의 모듈 import ──────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from update_dashboard import (
    Config, find_input_files, build_dashboard_data
)


# ════════════════════════════════════════════════════════════════
#  설정
# ════════════════════════════════════════════════════════════════
DATA_DIR    = "./data"
OUTPUT_DIR  = "./output"
JSON_NAME   = "dashboard_data.json"

# GitHub Pages 업로드 설정 (선택)
GITHUB_REPO       = os.environ.get("GITHUB_REPOSITORY", "hsleelove/IDIS-Americas-Security-Dashboard")
GITHUB_BRANCH     = "gh-pages"
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
GITHUB_JSON_PATH  = "data/dashboard_data.json"

# AWS S3 업로드 설정 (선택)
S3_BUCKET         = os.environ.get("S3_BUCKET", "idis-dashboard-data")
S3_KEY            = "dashboard_data.json"
S3_REGION         = "us-east-1"


# ════════════════════════════════════════════════════════════════
#  JSON 저장
# ════════════════════════════════════════════════════════════════
def save_json(D, output_dir):
    """dashboard_data.json 저장"""
    path = Path(output_dir) / JSON_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(D, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = path.stat().st_size // 1024
    print(f"  ✅ JSON 저장: {path.resolve()} ({size_kb} KB)")
    return str(path)


# ════════════════════════════════════════════════════════════════
#  업로드: GitHub Pages
# ════════════════════════════════════════════════════════════════
def upload_github(json_path):
    """
    GitHub API를 통해 gh-pages 브랜치에 JSON 파일 업로드.
    GITHUB_TOKEN 환경변수 필요.
    """
    try:
        import base64, requests as req
    except ImportError:
        print("  ❌ requests 없음: pip install requests")
        return False

    if not GITHUB_TOKEN:
        print("  ❌ GITHUB_TOKEN 환경변수가 없습니다.")
        return False

    with open(json_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_JSON_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
    }

    # 기존 파일 SHA 조회 (업데이트용)
    sha = None
    r = req.get(api_url, headers=headers, params={"ref": GITHUB_BRANCH})
    if r.status_code == 200:
        sha = r.json().get("sha")

    payload = {
        "message": f"Update dashboard data {date.today()}",
        "content": content,
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = req.put(api_url, headers=headers, json=payload)
    if r.status_code in (200, 201):
        url = f"https://{GITHUB_REPO.split('/')[0]}.github.io/{GITHUB_REPO.split('/')[1]}/{GITHUB_JSON_PATH}"
        print(f"  ✅ GitHub Pages 업로드 완료")
        print(f"     URL: {url}")
        return True
    else:
        print(f"  ❌ GitHub 업로드 실패: {r.status_code} {r.text[:200]}")
        return False


# ════════════════════════════════════════════════════════════════
#  업로드: AWS S3
# ════════════════════════════════════════════════════════════════
def upload_s3(json_path):
    """
    AWS S3에 JSON 업로드 (퍼블릭 읽기 가능하게).
    AWS 자격증명은 환경변수 또는 ~/.aws/credentials 로 설정.
    """
    try:
        import boto3
    except ImportError:
        print("  ❌ boto3 없음: pip install boto3")
        return False

    s3 = boto3.client("s3", region_name=S3_REGION)
    try:
        s3.upload_file(
            json_path, S3_BUCKET, S3_KEY,
            ExtraArgs={
                "ContentType":  "application/json",
                "CacheControl": "no-cache, max-age=0",
            },
        )
        url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{S3_KEY}"
        print(f"  ✅ S3 업로드 완료")
        print(f"     URL: {url}")
        return True
    except Exception as e:
        print(f"  ❌ S3 업로드 실패: {e}")
        return False


# ════════════════════════════════════════════════════════════════
#  메인 파이프라인
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="IDIS Dashboard Pipeline")
    parser.add_argument("--skip-download", action="store_true",
                        help="NetSuite 다운로드 건너뜀 (기존 파일 사용)")
    parser.add_argument("--data-dir",      default=DATA_DIR)
    parser.add_argument("--output-dir",    default=OUTPUT_DIR)
    parser.add_argument("--upload-github", action="store_true",
                        help="GitHub Pages에 JSON 업로드")
    parser.add_argument("--upload-s3",     action="store_true",
                        help="AWS S3에 JSON 업로드")
    parser.add_argument("--upload-gdrive", action="store_true",
                        help="Google Drive에 JSON 업로드")
    parser.add_argument("--as-of",         default=None)
    parser.add_argument("--week",          default=None)
    args = parser.parse_args()

    today     = date.today()
    as_of_str = args.as_of or today.strftime("%Y-%m-%d")
    week_num  = today.isocalendar()[1]
    week_tag  = args.week or f"{today.year}-W{week_num:02d}"

    print("=" * 55)
    print("  IDIS Dashboard Pipeline")
    print(f"  기준일: {as_of_str}  |  주차: {week_tag}")
    print("=" * 55)

    # ── Step 1: 다운로드 ────────────────────────────────────────
    if not args.skip_download:
        print("\n[Step 1] NetSuite 파일 다운로드")
        try:
            from netsuite_harvester import main as harvest_main
            import sys as _sys
            _sys.argv = ["netsuite_harvester.py", "--output-dir", args.data_dir]
            harvest_main()
        except Exception as e:
            import traceback
            print(f"❌ 다운로드 실패: {e}")
            print("--- 상세 오류 ---")
            traceback.print_exc()
            print("--- 오류 끝 ---")
            print("--skip-download 옵션으로 기존 파일을 사용할 수 있습니다.")
            sys.exit(1)
    else:
        print("\n[Step 1] 다운로드 건너뜀 (기존 파일 사용)")

    # ── Step 2: 파싱 & 데이터 빌드 ─────────────────────────────
    print("\n[Step 2] 데이터 파싱")
    files = find_input_files(args.data_dir)
    missing = [k for k in ["ytd","booking","pf","activities","opps"] if k not in files]
    if missing:
        print(f"❌ 필수 파일 없음: {missing}")
        sys.exit(1)

    D = build_dashboard_data(files, as_of_str, week_tag)

    # 요약
    print(f"\n  Pipeline {len(D['pipeline']):,}건 | "
          f"PF {len(D['pf_raw']):,}건 | "
          f"Booking reps {len(D['booking_raw'])}명")
    for mk in ["2026-01","2026-02","2026-03"]:
        cm = D["company_monthly"].get(mk, {})
        print(f"  {mk}: booking ${cm.get('booking',0):>10,.0f}  "
              f"sales ${cm.get('sales',0):>10,.0f}")

    # ── Step 3: JSON 저장 ────────────────────────────────────────
    print("\n[Step 3] JSON 저장")
    json_path = save_json(D, args.output_dir)

    # ── Step 4: 업로드 ───────────────────────────────────────────
    if args.upload_github:
        print("\n[Step 4] GitHub Pages 업로드")
        upload_github(json_path)
    elif args.upload_s3:
        print("\n[Step 4] S3 업로드")
        upload_s3(json_path)
    elif args.upload_gdrive:
        print("\n[Step 4] Google Drive 업로드")
        try:
            from gdrive_uploader import upload_json
            url, file_id = upload_json(json_path)
            print(f"  ✅ Google Drive 업로드 완료")
            print(f"  URL: {url}")
        except Exception as e:
            print(f"  ❌ Google Drive 업로드 실패: {e}")
    else:
        print(f"\n[Step 4] 업로드 건너뜀")
        print(f"  로컬 파일: {Path(json_path).resolve()}")
        print(f"  업로드하려면: --upload-gdrive 옵션 추가")

    print("\n🎉 파이프라인 완료!")


if __name__ == "__main__":
    main()
