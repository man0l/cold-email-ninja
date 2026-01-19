#!/usr/bin/env python3
"""
Export to Google Sheets

Exports JSON data (from checkpoint files or other sources) to Google Sheets.
Supports creating new spreadsheets in specified Drive folders.
"""

import argparse
import json
import os
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

    args = parser.parse_args()

    # Load data
    print(f"📁 Loading data from: {args.input}")
    leads = load_json_file(args.input)
    print(f"   Found {len(leads)} records")

    # Determine folder
    folder_id = None if args.no_folder else args.folder_id

    # Export
    sheet_url = export_to_google_sheets(leads, args.output_sheet, folder_id)

    print(f"\n✅ Export complete!")
    print(f"📊 Google Sheet: {sheet_url}")


if __name__ == "__main__":
    main()
