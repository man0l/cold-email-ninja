#!/usr/bin/env python3
"""
Fix scattered city/state/zip/country columns in a Google Sheet.

Reads a source sheet, normalizes location fields, and writes to a new tab
in the same spreadsheet by default.
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
    print("❌ Error: Google API libraries not available.")
    print("   Install with: pip install google-api-python-client google-auth")
    sys.exit(1)


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

STATE_ABBR = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

STATE_NAME_TO_ABBR = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}

ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
STATE_ABBR_RE = re.compile(r"\b([A-Z]{2})\b")
COUNTRY_RE = re.compile(r"\b(usa|us|united states|united states of america)\b", re.I)


def authenticate_google():
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    return ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)


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


def resolve_sheet_name(
    service, spreadsheet_id: str, sheet_name: Optional[str], spreadsheet_url: str
) -> Optional[str]:
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


def format_sheet_range(sheet_name: Optional[str]) -> str:
    if not sheet_name:
        return "A:ZZ"
    safe_name = sheet_name.replace("'", "''")
    return f"'{safe_name}'!A:ZZ"


def load_sheet_rows(service, spreadsheet_id: str, sheet_name: Optional[str]) -> List[List[str]]:
    range_name = format_sheet_range(sheet_name)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        ).execute()
        return result.get("values", [])
    except HttpError as e:
        if sheet_name and "Unable to parse range" in str(e):
            fallback_range = f"{sheet_name}!A:ZZ"
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=fallback_range,
                ).execute()
                return result.get("values", [])
            except HttpError:
                pass
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)


def build_leads_from_rows(rows: List[List[str]], headers: List[str]) -> List[Dict[str, Any]]:
    if not rows or not headers:
        return []
    if rows and rows[0] == headers:
        rows = rows[1:]
    leads = []
    for row in rows:
        row = row + [""] * (len(headers) - len(row))
        leads.append({headers[i]: row[i] for i in range(len(headers))})
    return leads


def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str]) -> List[Dict[str, Any]]:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = parse_spreadsheet_id(spreadsheet_url)
    sheet_name = resolve_sheet_name(service, spreadsheet_id, sheet_name, spreadsheet_url)
    rows = load_sheet_rows(service, spreadsheet_id, sheet_name)
    if not rows:
        print("❌ No data found in sheet")
        return []
    headers = rows[0]
    return build_leads_from_rows(rows[1:], headers)


def detect_column(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    for name in fieldnames:
        if name.lower() in {c.lower() for c in candidates}:
            return name
    return None


def ensure_sheet_tab(service, spreadsheet_id: str, sheet_name: str) -> str:
    try:
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        print(f"  ✅ Added tab '{sheet_name}'")
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


def upload_to_google_sheets(
    leads: List[Dict[str, Any]],
    sheet_name: str,
    target_spreadsheet_id: str,
) -> str:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = target_spreadsheet_id
    if not leads:
        print("❌ No leads to upload")
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    sheet_name = ensure_sheet_tab(service, spreadsheet_id, sheet_name)

    headers = list(leads[0].keys())
    rows = [headers]
    rows.extend([[str(lead.get(h, "")) for h in headers] for lead in leads])

    ensure_sheet_size(service, spreadsheet_id, sheet_name, len(rows), len(headers))
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A:ZZ",
        body={},
    ).execute()

    chunk_size = 2000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        start_row = i + 1
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": chunk},
        ).execute()

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"


def extract_zip(text: str) -> Tuple[Optional[str], str]:
    if not text:
        return None, text
    match = ZIP_RE.search(text)
    if not match:
        return None, text
    zip_code = match.group(0)
    cleaned = (text[: match.start()] + text[match.end() :]).strip(" ,")
    return zip_code, cleaned


def extract_state(text: str) -> Tuple[Optional[str], str]:
    if not text:
        return None, text
    for name, abbr in sorted(STATE_NAME_TO_ABBR.items(), key=lambda item: len(item[0]), reverse=True):
        pattern = r"\b" + re.escape(name) + r"\b"
        if re.search(pattern, text, flags=re.I):
            cleaned = re.sub(pattern, "", text, flags=re.I).strip(" ,")
            return abbr, cleaned
    match = STATE_ABBR_RE.search(text.upper())
    if match and match.group(1) in STATE_ABBR:
        abbr = match.group(1)
        cleaned = (text[: match.start()] + text[match.end() :]).strip(" ,")
        return abbr, cleaned
    return None, text


def extract_country(text: str) -> Tuple[Optional[str], str]:
    if not text:
        return None, text
    match = COUNTRY_RE.search(text)
    if not match:
        return None, text
    cleaned = COUNTRY_RE.sub("", text).strip(" ,")
    return "USA", cleaned


def normalize_city_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip(" ,")
    return cleaned


def normalize_state_value(value: str) -> Optional[str]:
    if not value:
        return None
    trimmed = value.strip()
    if trimmed.upper() in STATE_ABBR:
        return trimmed.upper()
    lowered = trimmed.lower()
    if lowered in STATE_NAME_TO_ABBR:
        return STATE_NAME_TO_ABBR[lowered]
    return None


def normalize_zip_value(value: str) -> Optional[str]:
    if not value:
        return None
    match = ZIP_RE.search(value)
    return match.group(0) if match else None


def normalize_country_value(value: str) -> Optional[str]:
    if not value:
        return None
    return "USA" if COUNTRY_RE.search(value) else None


def parse_address_city_state_zip(address: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not address:
        return None, None, None
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        return None, None, None
    city_part = parts[-2]
    state_zip_part = parts[-1]
    zip_value, remaining = extract_zip(state_zip_part)
    state_value, _ = extract_state(remaining)
    return normalize_city_text(city_part) or None, state_value, zip_value


def fix_location_fields(
    lead: Dict[str, Any],
    city_column: str,
    state_column: str,
    zip_column: str,
    country_column: str,
    address_column: Optional[str],
) -> Dict[str, Any]:
    values = {
        "city": (lead.get(city_column) or "").strip(),
        "state": (lead.get(state_column) or "").strip(),
        "zip": (lead.get(zip_column) or "").strip(),
        "country": (lead.get(country_column) or "").strip(),
        "address": (lead.get(address_column) or "").strip() if address_column else "",
    }

    city_parts: List[str] = []
    state_value = normalize_state_value(values["state"])
    zip_value = normalize_zip_value(values["zip"])
    country_value = normalize_country_value(values["country"])

    for raw in [values["city"], values["state"], values["zip"], values["country"]]:
        if not raw:
            continue
        working = raw

        found_zip, working = extract_zip(working)
        if found_zip and not zip_value:
            zip_value = found_zip

        found_state, working = extract_state(working)
        if found_state and not state_value:
            state_value = found_state

        found_country, working = extract_country(working)
        if found_country and not country_value:
            country_value = found_country

        cleaned = normalize_city_text(working)
        if cleaned:
            city_parts.append(cleaned)

    if values["address"]:
        parsed_city, parsed_state, parsed_zip = parse_address_city_state_zip(values["address"])
        if parsed_state and not state_value:
            state_value = parsed_state
        if parsed_zip and not zip_value:
            zip_value = parsed_zip
        if parsed_city:
            city_value_candidate = normalize_city_text(" ".join(city_parts))
            has_digits = bool(re.search(r"\d", city_value_candidate))
            parsed_has_digits = bool(re.search(r"\d", parsed_city))
            if not city_parts or (has_digits and not parsed_has_digits):
                city_parts = [parsed_city]

    city_value = normalize_city_text(" ".join(city_parts))

    if city_value and re.search(r"\d", city_value) and state_value and not zip_value:
        parts = [p for p in city_value.split() if p]
        if parts:
            candidate = parts[-1]
            if candidate.isalpha() and len(candidate) > 2:
                city_value = candidate

    if not country_value and (state_value or zip_value):
        country_value = "USA"

    lead[city_column] = city_value or values["city"]
    lead[state_column] = state_value or values["state"]
    lead[zip_column] = zip_value or values["zip"]
    lead[country_column] = country_value or values["country"]

    return lead


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix scattered city/state/zip/country columns in Google Sheets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-url", required=True, help="Google Sheet URL")
    parser.add_argument("--sheet-name", default=None, help="Source sheet name (optional)")
    parser.add_argument(
        "--output-sheet",
        default="Fixed Locations",
        help="Output sheet tab name in the same spreadsheet",
    )

    args = parser.parse_args()

    leads = load_from_google_sheets(args.source_url, args.sheet_name)
    if not leads:
        print("❌ No leads loaded.")
        sys.exit(1)

    fieldnames = list(leads[0].keys())
    city_column = detect_column(fieldnames, ["city"])
    state_column = detect_column(fieldnames, ["state", "state_abbr", "state_abbreviation"])
    zip_column = detect_column(fieldnames, ["zip", "zipcode", "zip_code", "postal", "postal_code"])
    country_column = detect_column(fieldnames, ["country", "company_country"])
    address_column = detect_column(
        fieldnames, ["address", "street", "street_address", "full_address", "company_address"]
    )

    if not all([city_column, state_column, zip_column, country_column]):
        missing = [
            name
            for name, col in [
                ("city", city_column),
                ("state", state_column),
                ("zip", zip_column),
                ("country", country_column),
            ]
            if not col
        ]
        print(f"❌ Missing required columns: {', '.join(missing)}")
        sys.exit(1)

    fixed = [
        fix_location_fields(
            lead,
            city_column,
            state_column,
            zip_column,
            country_column,
            address_column,
        )
        for lead in leads
    ]

    spreadsheet_id = parse_spreadsheet_id(args.source_url)
    sheet_url = upload_to_google_sheets(fixed, args.output_sheet, spreadsheet_id)
    print(f"✅ Output Sheet: {sheet_url}")


if __name__ == "__main__":
    main()
