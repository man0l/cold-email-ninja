#!/usr/bin/env python3
"""
Export to Google Sheets

Exports JSON data (from checkpoint files or other sources) to Google Sheets.
Supports creating new spreadsheets in specified Drive folders.
"""

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
except ImportError:
    print("❌ Error: Google API libraries not available.")
    print("   Install with: pip install google-api-python-client google-auth")
    sys.exit(1)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

DEFAULT_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID', '0ADWgx-M8Z5r-Uk9PVA')


def authenticate_google():
    """Authenticate with Google APIs using service account credentials"""
    creds_path = 'credentials.json'
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)

    creds = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)
    return creds


def load_json_file(file_path: str) -> List[Dict[str, Any]]:
    """Load data from a JSON file. Handles both array and checkpoint formats."""
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Handle checkpoint format (has 'leads' key)
    if isinstance(data, dict) and 'leads' in data:
        return data['leads']

    # Handle direct array format
    if isinstance(data, list):
        return data

    print(f"❌ Error: Unrecognized JSON format in {file_path}")
    sys.exit(1)


def extract_gid(spreadsheet_url: str) -> Optional[int]:
    match = re.search(r"[?&]gid=(\d+)", spreadsheet_url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def parse_spreadsheet_id(spreadsheet_url: str) -> str:
    if "/d/" in spreadsheet_url:
        return spreadsheet_url.split("/d/")[1].split("/")[0]
    return spreadsheet_url


def resolve_sheet_name(service, spreadsheet_id: str, sheet_name: Optional[str], spreadsheet_url: str) -> Optional[str]:
    if sheet_name:
        return sheet_name
    gid = extract_gid(spreadsheet_url)
    if gid is None:
        return None
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("sheetId") == gid:
                return props.get("title")
    except HttpError:
        return None
    return None


def ensure_sheet_tab(service, spreadsheet_id: str, sheet_name: str) -> str:
    try:
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        print(f"✅ Added tab '{sheet_name}'")
        return sheet_name
    except HttpError as e:
        if "already exists" in str(e):
            return sheet_name
        raise


def ensure_sheet_size(service, spreadsheet_id: str, sheet_name: str, rows: int, cols: int) -> None:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    current_rows = 0
    current_cols = 0
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            sheet_id = props.get("sheetId")
            grid = props.get("gridProperties", {})
            current_rows = grid.get("rowCount", 0)
            current_cols = grid.get("columnCount", 0)
            break
    if sheet_id is None:
        return
    if current_rows >= rows and current_cols >= cols:
        return
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": max(current_rows, rows),
                            "columnCount": max(current_cols, cols),
                        },
                    },
                    "fields": "gridProperties(rowCount,columnCount)",
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def apply_header_formatting(service, spreadsheet_id: str, sheet_name: str, header_count: int) -> None:
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name:
                sheet_id = props.get("sheetId")
                break
        if sheet_id is None:
            return
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                            "cell": {
                                "userEnteredFormat": {
                                    "textFormat": {"bold": True},
                                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                                }
                            },
                            "fields": "userEnteredFormat(textFormat,backgroundColor)",
                        }
                    },
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": header_count,
                            }
                        }
                    },
                ]
            },
        ).execute()
    except HttpError:
        pass

def export_to_google_sheets(
    leads: List[Dict[str, Any]],
    sheet_name: str,
    folder_id: Optional[str] = None
) -> str:
    """
    Export leads to a new Google Sheet.

    Args:
        leads: List of dictionaries to export
        sheet_name: Name for the new spreadsheet
        folder_id: Google Drive folder ID (uses shared drive if provided)

    Returns:
        URL of the created spreadsheet
    """
    if not leads:
        print("❌ No data to export")
        sys.exit(1)

    creds = authenticate_google()
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    spreadsheet_id = None

    # Create spreadsheet
    if folder_id:
        print(f"📂 Creating spreadsheet in folder: {folder_id}")
        try:
            file_metadata = {
                'name': sheet_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [folder_id]
            }
            file = drive_service.files().create(
                body=file_metadata,
                fields='id',
                supportsAllDrives=True
            ).execute()
            spreadsheet_id = file.get('id')
            print(f"✅ Created spreadsheet: {spreadsheet_id}")
        except HttpError as e:
            print(f"❌ Error creating spreadsheet in folder: {e}")
            sys.exit(1)
    else:
        print("📂 Creating spreadsheet in root Drive")
        try:
            spreadsheet = {'properties': {'title': sheet_name}}
            spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = spreadsheet['spreadsheetId']
            print(f"✅ Created spreadsheet: {spreadsheet_id}")
        except HttpError as e:
            print(f"❌ Error creating spreadsheet: {e}")
            sys.exit(1)

    # Prepare data
    headers = list(leads[0].keys())
    rows = [headers]
    for lead in leads:
        row = [str(lead.get(h, '')) for h in headers]
        rows.append(row)

    # Upload data in chunks
    chunk_size = 2000
    total_rows = len(rows)
    print(f"📤 Uploading {total_rows} rows...")

    for i in range(0, total_rows, chunk_size):
        chunk = rows[i:i + chunk_size]
        try:
            sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range="Sheet1!A1",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': chunk}
            ).execute()
        except HttpError as e:
            print(f"❌ Error uploading data: {e}")
            sys.exit(1)

    # Format header row
    try:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [
                    {
                        'repeatCell': {
                            'range': {'sheetId': 0, 'startRowIndex': 0, 'endRowIndex': 1},
                            'cell': {
                                'userEnteredFormat': {
                                    'textFormat': {'bold': True},
                                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                                }
                            },
                            'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                        }
                    },
                    {
                        'autoResizeDimensions': {
                            'dimensions': {
                                'sheetId': 0,
                                'dimension': 'COLUMNS',
                                'startIndex': 0,
                                'endIndex': len(headers)
                            }
                        }
                    }
                ]
            }
        ).execute()
    except HttpError:
        pass  # Formatting is optional

    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    return sheet_url


def upload_to_existing_sheet(
    leads: List[Dict[str, Any]],
    spreadsheet_url: str,
    sheet_name: Optional[str],
) -> str:
    if not leads:
        print("❌ No data to export")
        sys.exit(1)

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_url)
    resolved_name = resolve_sheet_name(service, spreadsheet_id, sheet_name, spreadsheet_url)
    if not resolved_name:
        resolved_name = sheet_name or "Sheet1"
    resolved_name = ensure_sheet_tab(service, spreadsheet_id, resolved_name)

    headers = list(leads[0].keys())
    rows = [headers]
    rows.extend([[str(lead.get(h, "")) for h in headers] for lead in leads])

    ensure_sheet_size(service, spreadsheet_id, resolved_name, len(rows), len(headers))
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{resolved_name}'!A:ZZ",
        body={},
    ).execute()

    chunk_size = 2000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        start_row = i + 1
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{resolved_name}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": chunk},
        ).execute()

    apply_header_formatting(service, spreadsheet_id, resolved_name, len(headers))
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"


def main():
    parser = argparse.ArgumentParser(
        description='Export JSON data to Google Sheets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export checkpoint file to Google Sheets
  python export_to_sheets.py --input .tmp/google_maps_checkpoint.json --output-sheet "My Leads"

  # Export to specific Drive folder
  python export_to_sheets.py --input data.json --output-sheet "Leads" --folder-id "ABC123..."

  # Export any JSON array
  python export_to_sheets.py --input leads.json --output-sheet "Exported Leads"
"""
    )

    parser.add_argument('--input', '-i', required=True,
                        help='Path to JSON file (checkpoint or array format)')
    parser.add_argument('--output-sheet', '-o', required=True,
                        help='Name for the Google Sheet')
    parser.add_argument('--folder-id', '-f', default=DEFAULT_FOLDER_ID,
                        help=f'Google Drive folder ID (default: {DEFAULT_FOLDER_ID})')
    parser.add_argument('--no-folder', action='store_true',
                        help='Create in Drive root instead of default folder')
    parser.add_argument('--spreadsheet-url', default=None,
                        help='Existing Google Sheet URL to overwrite')
    parser.add_argument('--spreadsheet-id', default=None,
                        help='Existing Google Sheet ID to overwrite')
    parser.add_argument('--sheet-name', default=None,
                        help='Target tab name when overwriting a sheet')

    args = parser.parse_args()

    # Load data
    print(f"📁 Loading data from: {args.input}")
    leads = load_json_file(args.input)
    print(f"   Found {len(leads)} records")

    if args.spreadsheet_url or args.spreadsheet_id:
        target = args.spreadsheet_url or args.spreadsheet_id
        print(f"📤 Overwriting spreadsheet: {target}")
        sheet_url = upload_to_existing_sheet(leads, target, args.sheet_name or args.output_sheet)
    else:
        # Determine folder
        folder_id = None if args.no_folder else args.folder_id
        sheet_url = export_to_google_sheets(leads, args.output_sheet, folder_id)

    print(f"\n✅ Export complete!")
    print(f"📊 Google Sheet: {sheet_url}")


if __name__ == "__main__":
    main()
