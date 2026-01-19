#!/usr/bin/env python3
"""
Filter Google Sheet leads by state (exclude one or more states).

Loads leads from a Google Sheet, detects state column (or derives from city),
filters out excluded states, and writes to a new Google Sheet.
"""

import argparse
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
except ImportError:
    print("❌ Error: Google Sheets libraries not available.")
    print("   Install with: pip install google-api-python-client google-auth")
    sys.exit(1)


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
STATE_ABBR_RE = re.compile(r",\s*([A-Z]{2})\b")


def authenticate_google():
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    return ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)


def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str]) -> List[Dict[str, Any]]:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    if "/d/" in spreadsheet_url:
        spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
    else:
        spreadsheet_id = spreadsheet_url

    try:
        range_name = f"{sheet_name}!A:ZZ" if sheet_name else "A:ZZ"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        ).execute()
        rows = result.get("values", [])
        if not rows:
            print("❌ No data found in sheet")
            return []
        headers = rows[0]
        leads = []
        for row in rows[1:]:
            row = row + [""] * (len(headers) - len(row))
            leads.append({headers[i]: row[i] for i in range(len(headers))})
        return leads
    except HttpError as e:
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)


def save_to_google_sheets(leads: List[Dict[str, Any]], sheet_name: str, folder_id: Optional[str]) -> str:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    if folder_id:
        file_metadata = {
            "name": sheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        file = drive_service.files().create(
            body=file_metadata, fields="id", supportsAllDrives=True
        ).execute()
        spreadsheet_id = file.get("id")
    else:
        spreadsheet = {"properties": {"title": sheet_name}}
        spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
        spreadsheet_id = spreadsheet["spreadsheetId"]

    if not leads:
        print("❌ No leads to save")
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    headers = list(leads[0].keys())
    rows = [headers]
    for lead in leads:
        rows.append([str(lead.get(h, "")) for h in headers])

    chunk_size = 2000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": chunk},
        ).execute()

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"


def detect_column(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    for name in fieldnames:
        if name.lower() in {c.lower() for c in candidates}:
            return name
    return None


def normalize_state(value: str) -> str:
    return (value or "").strip().upper()


def derive_state_from_city(city_value: str) -> str:
    if not city_value:
        return ""
    match = STATE_ABBR_RE.search(city_value)
    return match.group(1) if match else ""


def filter_leads(
    leads: List[Dict[str, Any]],
    exclude_states: List[str],
    state_column: Optional[str],
    city_column: Optional[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    stats = {"total": len(leads), "excluded": 0, "kept": 0, "derived": 0, "missing_state": 0}
    results: List[Dict[str, Any]] = []
    exclude_set = {s.upper() for s in exclude_states}

    for lead in leads:
        state_value = (lead.get(state_column) or "").strip() if state_column else ""
        derived_state = ""
        if not state_value and city_column:
            derived_state = derive_state_from_city(lead.get(city_column, ""))
            if derived_state:
                stats["derived"] += 1
        normalized_state = normalize_state(state_value or derived_state)
        if not normalized_state:
            stats["missing_state"] += 1

        lead["derived_state"] = derived_state
        if normalized_state in exclude_set:
            stats["excluded"] += 1
            continue

        results.append(lead)
        stats["kept"] += 1

    return results, stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter Google Sheet leads by excluded states.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-url", required=True, help="Google Sheet URL")
    parser.add_argument("--output-sheet", required=True, help="Output sheet name")
    parser.add_argument("--sheet-name", default=None, help="Source sheet name (optional)")
    parser.add_argument(
        "--exclude-state",
        action="append",
        default=["NC"],
        help="State abbreviation to exclude (repeatable). Default: NC",
    )
    parser.add_argument("--state-column", default=None, help="State column name (optional)")
    parser.add_argument("--city-column", default=None, help="City column name (optional)")
    parser.add_argument("--folder-id", default=DEFAULT_FOLDER_ID, help="Google Drive folder ID")

    args = parser.parse_args()

    leads = load_from_google_sheets(args.source_url, args.sheet_name)
    if not leads:
        print("❌ No leads loaded.")
        sys.exit(1)

    fieldnames = list(leads[0].keys())
    state_column = args.state_column or detect_column(
        fieldnames, ["state", "state_abbr", "state_abbreviation"]
    )
    city_column = args.city_column or detect_column(fieldnames, ["city", "City"])

    filtered, stats = filter_leads(
        leads,
        exclude_states=args.exclude_state,
        state_column=state_column,
        city_column=city_column,
    )

    print("✅ Filter complete")
    print(f"   Total: {stats['total']}")
    print(f"   Kept: {stats['kept']}")
    print(f"   Excluded: {stats['excluded']}")
    print(f"   Missing state: {stats['missing_state']}")
    print(f"   Derived state: {stats['derived']}")

    sheet_url = save_to_google_sheets(filtered, args.output_sheet, args.folder_id)
    print(f"✅ Output Sheet: {sheet_url}")


if __name__ == "__main__":
    main()
