"""
gdrive_uploader.py
==================
dashboard_data.json 을 Google Drive에 업로드하고
공개 링크를 반환합니다.

필요 패키지:
  pip install google-api-python-client google-auth
"""

import os
import json
import sys
from pathlib import Path

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    print("pip install google-api-python-client google-auth")
    raise

# ════════════════════════════════════════════════════
#  설정
# ════════════════════════════════════════════════════
FOLDER_ID   = os.environ.get("GDRIVE_FOLDER_ID", "")
CREDENTIALS = os.environ.get("GDRIVE_CREDENTIALS", "")   # Service Account JSON 전체
FILE_NAME   = "dashboard_data.json"
SCOPES      = ["https://www.googleapis.com/auth/drive"]


# ════════════════════════════════════════════════════
#  Google Drive 서비스 초기화
# ════════════════════════════════════════════════════
def get_drive_service():
    if not CREDENTIALS:
        raise RuntimeError(
            "GDRIVE_CREDENTIALS 환경변수가 없습니다.\n"
            "GitHub Secrets에 Service Account JSON을 등록하세요."
        )
    creds_info = json.loads(CREDENTIALS)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


# ════════════════════════════════════════════════════
#  파일 업로드 (이미 있으면 업데이트)
# ════════════════════════════════════════════════════
def upload_json(local_path):
    if not FOLDER_ID:
        raise RuntimeError(
            "GDRIVE_FOLDER_ID 환경변수가 없습니다.\n"
            "Google Drive 폴더 ID를 GitHub Secrets에 등록하세요."
        )

    service   = get_drive_service()
    file_path = Path(local_path)

    print(f"  Google Drive 업로드 중: {file_path.name}")

    # 기존 파일 검색 (같은 이름이 있으면 업데이트)
    query = (
        f"name='{FILE_NAME}' "
        f"and '{FOLDER_ID}' in parents "
        f"and trashed=false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    existing = results.get("files", [])

    media = MediaFileUpload(
        str(file_path),
        mimetype="application/json",
        resumable=True
    )

    if existing:
        # 기존 파일 업데이트
        file_id = existing[0]["id"]
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"  ✅ 기존 파일 업데이트 (ID: {file_id})")
    else:
        # 새 파일 생성
        metadata = {
            "name":    FILE_NAME,
            "parents": [FOLDER_ID],
        }
        result = service.files().create(
            body=metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        file_id = result["id"]
        print(f"  ✅ 새 파일 생성 (ID: {file_id})")

    # 공개 링크 설정 (anyone with link can view)
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    # 직접 다운로드 URL 생성
    direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    view_url   = f"https://drive.google.com/file/d/{file_id}/view"

    print(f"  📎 공개 URL: {direct_url}")
    return direct_url, file_id


# ════════════════════════════════════════════════════
#  메인
# ════════════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="output/dashboard_data.json")
    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"❌ 파일 없음: {args.file}")
        sys.exit(1)

    url, file_id = upload_json(args.file)
    print(f"\n✅ 업로드 완료!")
    print(f"   File ID : {file_id}")
    print(f"   직접 URL: {url}")
    print(f"\n   HTML에 이 URL을 넣으세요:")
    print(f"   const DATA_URL = '{url}';")
    return url


if __name__ == "__main__":
    main()
