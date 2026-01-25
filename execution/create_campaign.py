#!/usr/bin/env python3
"""
Create Campaign + Upload CSV

Creates a campaign via the API and uploads a mapped CSV of leads.
Supports CSV input or Google Sheets input.
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
except ImportError:
    build = None
    HttpError = None
    ServiceAccountCredentials = None


SUPPORTED_FIELDS = [
    "first_name",
    "last_name",
    "full_name",
    "email",
    "personal_email",
    "company_name",
    "company_website",
    "company_domain",
    "linkedin",
    "title",
    "industry",
    "city",
    "state",
    "country",
]

APOLLO_FIELD_MAP = {
    "job_title": "title",
    "person_linkedin": "linkedin",
    "company_category": "industry",
    "company_city": "city",
    "company_state": "state",
    "company_country": "country",
}


def normalize_header(header: str) -> str:
    normalized = header.strip().lower()
    normalized = normalized.replace(" ", "_").replace("-", "_")
    return normalized


def read_text_file(path: str) -> str:
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def authenticate_google():
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    if ServiceAccountCredentials is None:
        print("❌ Error: Google Sheets libraries not available.")
        print("   Install with: pip install google-api-python-client google-auth")
        sys.exit(1)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    return ServiceAccountCredentials.from_service_account_file(creds_path, scopes=scopes)


def extract_spreadsheet_id(spreadsheet_url: str) -> str:
    if "/d/" in spreadsheet_url:
        return spreadsheet_url.split("/d/")[1].split("/")[0]
    return spreadsheet_url


def resolve_sheet_name(service, spreadsheet_id: str, gid: Optional[str], sheet_name: Optional[str]) -> Optional[str]:
    if sheet_name:
        return sheet_name
    if not gid:
        return None
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if str(props.get("sheetId")) == str(gid):
                return props.get("title")
    except HttpError:
        return None
    return None


def load_from_google_sheet(spreadsheet_url: str, sheet_name: Optional[str], limit: Optional[int]) -> Tuple[List[str], List[List[str]]]:
    if build is None:
        print("❌ Google Sheets libraries not available.")
        sys.exit(1)

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
    gid = None
    if "gid=" in spreadsheet_url:
        gid = spreadsheet_url.split("gid=")[1].split("&")[0]

    resolved_name = resolve_sheet_name(service, spreadsheet_id, gid, sheet_name)
    if resolved_name:
        range_name = f"'{resolved_name}'!A:ZZ"
    else:
        range_name = "A:ZZ"

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
    except HttpError as e:
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)

    rows = result.get("values", [])
    if not rows:
        print("❌ No data found in sheet")
        sys.exit(1)

    header = rows[0]
    body = rows[1:]
    if limit:
        body = body[:limit]
    return header, body


def load_from_csv(source_file: str, limit: Optional[int]) -> Tuple[List[str], List[List[str]]]:
    if not os.path.exists(source_file):
        print(f"❌ File not found: {source_file}")
        sys.exit(1)
    with open(source_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        print("❌ CSV is empty")
        sys.exit(1)
    header = rows[0]
    body = rows[1:]
    if limit:
        body = body[:limit]
    return header, body


def build_mapping(headers: List[str]) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    for idx, header in enumerate(headers):
        normalized = normalize_header(header)
        if normalized in SUPPORTED_FIELDS:
            mapping[idx] = normalized
            continue
        if normalized in APOLLO_FIELD_MAP:
            mapping[idx] = APOLLO_FIELD_MAP[normalized]
            continue
        # Accept common camel case variants
        if normalized == "first_name":
            mapping[idx] = "first_name"
        elif normalized == "last_name":
            mapping[idx] = "last_name"
        elif normalized == "full_name":
            mapping[idx] = "full_name"
        elif normalized == "company_name":
            mapping[idx] = "company_name"
        elif normalized in ("company_website", "company_domain"):
            mapping[idx] = normalized
        elif normalized in ("linkedin", "title", "industry", "city", "state", "country", "email", "personal_email"):
            mapping[idx] = normalized
    return mapping


def map_rows(headers: List[str], rows: List[List[str]]) -> List[Dict[str, str]]:
    mapping = build_mapping(headers)
    mapped_rows: List[Dict[str, str]] = []
    for row in rows:
        padded = row + [""] * (len(headers) - len(row))
        out = {key: "" for key in SUPPORTED_FIELDS}
        for idx, target_key in mapping.items():
            out[target_key] = padded[idx]
        if not out["full_name"] and (out["first_name"] or out["last_name"]):
            out["full_name"] = " ".join([out["first_name"], out["last_name"]]).strip()
        mapped_rows.append(out)
    return mapped_rows


def write_mapped_csv(rows: List[Dict[str, str]], output_path: Optional[str]) -> str:
    if not output_path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = f".tmp/campaign_upload_{timestamp}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUPPORTED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def create_campaign(base_url: str, payload: Dict[str, str]) -> str:
    url = f"{base_url.rstrip('/')}/api/campaigns"
    response = requests.post(url, json=payload, timeout=60)
    if response.status_code >= 400:
        print(f"❌ Create campaign failed: {response.status_code}")
        print(response.text)
        sys.exit(1)
    data = response.json()
    campaign = data.get("campaign") or {}
    campaign_id = campaign.get("id")
    if not campaign_id:
        print("❌ Campaign ID not found in response")
        print(response.text)
        sys.exit(1)
    return campaign_id


def upload_csv(base_url: str, campaign_id: str, csv_path: str) -> int:
    url = f"{base_url.rstrip('/')}/api/campaigns/{campaign_id}/upload"
    with open(csv_path, "rb") as f:
        response = requests.post(url, files={"file": f}, timeout=300)
    if response.status_code >= 400:
        print(f"❌ Upload failed: {response.status_code}")
        print(response.text)
        sys.exit(1)
    data = response.json()
    inserted = data.get("inserted", 0)
    return int(inserted)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create campaign and upload CSV")
    parser.add_argument("--name", required=True, help="Campaign name")
    parser.add_argument("--service-line", required=True, help="Service line description")
    parser.add_argument("--summarize-prompt", help="Summarize prompt string")
    parser.add_argument("--summarize-prompt-file", help="Path to summarize prompt text file")
    parser.add_argument("--icebreaker-prompt", help="Icebreaker prompt string")
    parser.add_argument("--icebreaker-prompt-file", help="Path to icebreaker prompt text file")
    parser.add_argument("--source-file", help="CSV file path")
    parser.add_argument("--source-url", help="Google Sheet URL")
    parser.add_argument("--sheet-name", help="Sheet name (optional)")
    parser.add_argument("--limit", type=int, default=100, help="Max rows to upload (default 100)")
    parser.add_argument("--output", help="Mapped CSV output path (optional)")
    parser.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:3000"))
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.source_file and not args.source_url:
        print("❌ Provide --source-file or --source-url")
        sys.exit(1)

    summarize_prompt = args.summarize_prompt
    if args.summarize_prompt_file:
        summarize_prompt = read_text_file(args.summarize_prompt_file)
    if not summarize_prompt:
        print("❌ Provide --summarize-prompt or --summarize-prompt-file")
        sys.exit(1)

    icebreaker_prompt = args.icebreaker_prompt
    if args.icebreaker_prompt_file:
        icebreaker_prompt = read_text_file(args.icebreaker_prompt_file)
    if not icebreaker_prompt:
        print("❌ Provide --icebreaker-prompt or --icebreaker-prompt-file")
        sys.exit(1)

    if args.source_url:
        headers, body = load_from_google_sheet(args.source_url, args.sheet_name, args.limit)
    else:
        headers, body = load_from_csv(args.source_file, args.limit)

    mapped_rows = map_rows(headers, body)
    output_csv = write_mapped_csv(mapped_rows, args.output)

    payload = {
        "name": args.name,
        "service_line": args.service_line,
        "summarize_prompt": summarize_prompt,
        "icebreaker_prompt": icebreaker_prompt,
    }

    print("📦 Creating campaign...")
    campaign_id = create_campaign(args.base_url, payload)
    print(f"✅ Campaign created: {campaign_id}")
    print(f"📤 Uploading CSV: {output_csv}")
    inserted = upload_csv(args.base_url, campaign_id, output_csv)
    print(f"✅ Uploaded {inserted} leads")


if __name__ == "__main__":
    main()
