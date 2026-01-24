#!/usr/bin/env python3
"""
Convert enriched leads to Apollo-compatible format.

Supports input from JSON files or Google Sheets.
Optionally uploads the converted data to Google Sheets.
"""

import argparse
import ast
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

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

DEFAULT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
DEFAULT_HEADERS_URL = (
    "https://docs.google.com/spreadsheets/d/1B0dlnl-76PhdpYn5vgwI_m1KNL01zgyUzF2FsjMzDIA/edit"
)
DEFAULT_HEADERS_SHEET = "Emails Sample 20 (Jan 24 2026)"


def authenticate_google():
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    return ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)


def extract_gid(spreadsheet_url: str) -> Optional[int]:
    match = re.search(r"[?&]gid=(\d+)", spreadsheet_url)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
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


def load_sheet_rows(service, spreadsheet_id: str, sheet_name: Optional[str]) -> List[List[str]]:
    try:
        range_name = f"{sheet_name}!A:ZZ" if sheet_name else "A:ZZ"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        ).execute()
        return result.get("values", [])
    except HttpError as e:
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


def normalize_header_key(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "", (value or "").strip().lower().replace(" ", "_"))


def load_headers_from_sheet(
    service,
    spreadsheet_url: str,
    sheet_name: Optional[str],
) -> List[str]:
    spreadsheet_id = parse_spreadsheet_id(spreadsheet_url)
    resolved_sheet = resolve_sheet_name(service, spreadsheet_id, sheet_name, spreadsheet_url)
    rows = load_sheet_rows(service, spreadsheet_id, resolved_sheet)
    return rows[0] if rows else []


def build_row_from_lead(lead: Dict[str, Any], headers: List[str]) -> List[str]:
    alias_map = {
        "emails": "email",
        "email": "email",
        "first_name": "first_name",
        "last_name": "last_name",
        "full_name": "full_name",
        "company": "company_name",
        "company_name": "company_name",
        "company_domain": "company_domain",
        "company_website": "company_website",
        "company_phone": "company_phone",
        "company_linkedin": "company_linkedin",
        "person_linkedin": "person_linkedin",
        "company_address": "company_address",
        "company_city": "company_city",
        "company_state": "company_state",
        "company_zip": "company_zip",
        "company_country": "company_country",
        "company_category": "company_category",
        "job_title": "job_title",
        "source": "source",
    }
    lead_key_map = {normalize_header_key(k): k for k in lead.keys()}
    row = []
    for header in headers:
        normalized = normalize_header_key(header)
        mapped = alias_map.get(normalized, normalized)
        lead_key = lead_key_map.get(mapped, lead_key_map.get(normalized, ""))
        row.append(str(lead.get(lead_key, "")) if lead_key else "")
    return row


def parse_maybe_literal(value: Any) -> Any:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return value
        if raw.startswith("{") or raw.startswith("["):
            try:
                return ast.literal_eval(raw)
            except Exception:
                return value
    return value


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def build_key_map(lead: Dict[str, Any]) -> Dict[str, str]:
    return {k.lower(): k for k in lead.keys()}


def pick_value(lead: Dict[str, Any], key_map: Dict[str, str], candidates: Iterable[str]) -> str:
    for candidate in candidates:
        actual = key_map.get(candidate.lower())
        if actual is None:
            continue
        raw = lead.get(actual, "")
        if isinstance(raw, str) and not raw.strip():
            continue
        if raw is None:
            continue
        return raw
    return ""


def split_full_name(full_name: str) -> Tuple[str, str]:
    cleaned = normalize_space(full_name)
    if not cleaned:
        return "", ""
    parts = cleaned.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def extract_domain(website: str) -> str:
    if not website:
        return ""
    value = website.strip()
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    try:
        parsed = urlparse(value)
        return parsed.netloc.lower().replace("www.", "")
    except Exception:
        return ""


def extract_email_from_value(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return ""
        splitters = [",", ";", " "]
        for splitter in splitters:
            if splitter in normalized:
                normalized = normalized.split(splitter)[0].strip()
                break
        return normalized
    if isinstance(value, dict):
        for key in ["email", "value", "address"]:
            if key in value and value[key]:
                return str(value[key]).strip()
        for key in ["primary", "work", "personal"]:
            if key in value and value[key]:
                return extract_email_from_value(value[key])
    if isinstance(value, list):
        for item in value:
            email = extract_email_from_value(item)
            if email:
                return email
    return ""


def extract_emails_from_socials(value: Any) -> str:
    parsed = parse_maybe_literal(value)
    if isinstance(parsed, dict):
        return extract_email_from_value(parsed.get("email") or parsed.get("emails"))
    if isinstance(parsed, list):
        return extract_email_from_value(parsed)
    return ""


def extract_email(lead: Dict[str, Any], key_map: Dict[str, str]) -> Tuple[str, str]:
    primary = pick_value(lead, key_map, ["primary_email", "primaryEmail"])
    if primary:
        return extract_email_from_value(primary), "primary"

    direct = pick_value(
        lead,
        key_map,
        [
            "email",
            "person_email",
            "contact_email",
            "email_address",
            "emailAddress",
            "personEmail",
            "contactEmail",
        ],
    )
    if direct:
        return extract_email_from_value(direct), "direct"

    raw_emails = pick_value(
        lead,
        key_map,
        ["emails_raw", "emails", "emails_found", "email_list", "emailList"],
    )
    if raw_emails:
        parsed = parse_maybe_literal(raw_emails)
        email = extract_email_from_value(parsed)
        if email:
            return email, "list"

    socials = pick_value(lead, key_map, ["socials", "social_links", "socialLinks"])
    if socials:
        email = extract_emails_from_socials(socials)
        if email:
            return email, "socials"

    return "", "none"


def extract_linkedin(value: Any) -> str:
    parsed = parse_maybe_literal(value)
    if isinstance(parsed, str):
        return parsed.strip()
    if isinstance(parsed, dict):
        for key in ["linkedin", "linkedin_url", "linkedinUrl", "url"]:
            if key in parsed and parsed[key]:
                return str(parsed[key]).strip()
    if isinstance(parsed, list):
        for item in parsed:
            link = extract_linkedin(item)
            if link:
                return link
    return ""


def convert_lead(lead: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    key_map = build_key_map(lead)

    full_name = pick_value(lead, key_map, ["full_name", "fullName", "person_name", "contact_name", "name"])
    first_name, last_name = split_full_name(full_name)

    job_title = pick_value(
        lead,
        key_map,
        ["job_title", "title", "person_title", "decision_maker_title", "person_job_title"],
    )

    email, email_source = extract_email(lead, key_map)

    company_name = pick_value(
        lead,
        key_map,
        ["company_name", "company", "business_name", "companyName", "name"],
    )

    company_website = pick_value(
        lead,
        key_map,
        ["company_website", "website", "companyWebsite", "domain", "company_domain"],
    )
    company_domain = extract_domain(company_website) or pick_value(
        lead,
        key_map,
        ["company_domain", "domain", "companyDomain"],
    )

    company_phone = pick_value(
        lead,
        key_map,
        ["company_phone", "phone", "phone_number", "contact_phone", "phoneNumber"],
    )

    person_linkedin = pick_value(
        lead,
        key_map,
        ["person_linkedin", "linkedin", "linkedin_url", "contact_linkedin", "profile_linkedin"],
    )
    company_linkedin = pick_value(
        lead,
        key_map,
        ["company_linkedin", "company_linkedin_url", "social_linkedin", "companyLinkedin"],
    )

    socials = pick_value(lead, key_map, ["socials", "social_links", "socialLinks"])
    if socials and not company_linkedin:
        company_linkedin = extract_linkedin(socials)
    if socials and not person_linkedin:
        person_linkedin = extract_linkedin(socials)

    company_address = pick_value(
        lead,
        key_map,
        ["company_address", "address", "full_address", "street_address", "location"],
    )
    company_city = pick_value(lead, key_map, ["company_city", "city", "town"])
    company_state = pick_value(lead, key_map, ["company_state", "state", "province"])
    company_zip = pick_value(lead, key_map, ["company_zip", "zip", "postal_code", "postcode"])

    company_country = pick_value(lead, key_map, ["company_country", "country"])
    if not company_country:
        company_country = "United States"
    else:
        company_country = "United States" if company_country.lower() in {"us", "usa", "united states"} else company_country

    company_category = pick_value(lead, key_map, ["category", "industry", "company_category"])
    source_url = pick_value(lead, key_map, ["place_url", "google_maps_url", "source_url", "source"])

    output = {
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "job_title": job_title,
        "email": email,
        "company_name": company_name,
        "company_domain": company_domain,
        "company_website": company_website,
        "company_phone": company_phone,
        "company_linkedin": company_linkedin,
        "person_linkedin": person_linkedin,
        "company_address": company_address,
        "company_city": company_city,
        "company_state": company_state,
        "company_zip": company_zip,
        "company_country": company_country,
        "company_category": company_category,
        "source": source_url,
    }

    score = 0
    if email:
        score += 2
    if email_source == "primary":
        score += 2
    if first_name or last_name:
        score += 1
    return output, score


def normalize_company_key(company_name: str, company_domain: str) -> str:
    base = company_domain or company_name
    if not base:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", "", base.lower())
    return cleaned


def convert_leads(leads: List[Dict[str, Any]], limit: Optional[int]) -> List[Dict[str, Any]]:
    if limit:
        leads = leads[:limit]

    deduped: Dict[str, Tuple[Dict[str, Any], int]] = {}
    results: List[Dict[str, Any]] = []

    for lead in leads:
        converted, score = convert_lead(lead)
        if not converted.get("email"):
            continue
        key = normalize_company_key(converted.get("company_name", ""), converted.get("company_domain", ""))
        if not key:
            results.append(converted)
            continue
        if key not in deduped or score > deduped[key][1]:
            deduped[key] = (converted, score)

    results.extend(item[0] for item in deduped.values())
    return results


def save_json(data: List[Dict[str, Any]], output_file: str) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


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


def upload_to_google_sheets(
    leads: List[Dict[str, Any]],
    sheet_name: str,
    target_spreadsheet_id: Optional[str],
    folder_id: Optional[str],
    headers: Optional[List[str]] = None,
) -> str:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    if target_spreadsheet_id:
        spreadsheet_id = target_spreadsheet_id
    else:
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
        print("❌ No leads to upload")
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    sheet_name = ensure_sheet_tab(service, spreadsheet_id, sheet_name)

    output_headers = headers or list(leads[0].keys())
    rows = [output_headers]
    if headers:
        rows.extend(build_row_from_lead(lead, output_headers) for lead in leads)
    else:
        rows.extend([[str(lead.get(h, "")) for h in output_headers] for lead in leads])

    chunk_size = 2000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": chunk},
        ).execute()

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"


def load_json_file(file_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found: {file_path}")
        sys.exit(1)
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "leads" in data:
        return data["leads"]
    if isinstance(data, list):
        return data
    print(f"❌ Error: Unrecognized JSON format in {file_path}")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Convert leads to Apollo format")
    parser.add_argument("--input", "-i", help="Path to input JSON file")
    parser.add_argument("--output", "-o", default=".tmp/apollo_leads.json", help="Output JSON path")
    parser.add_argument("--spreadsheet-url", help="Google Sheets URL with leads")
    parser.add_argument("--sheet-name", help="Sheet name in spreadsheet")
    parser.add_argument(
        "--headers-from-sheet",
        help="Sheet name to use as headers (for headerless tabs)",
    )
    parser.add_argument(
        "--headers-from-url",
        default=DEFAULT_HEADERS_URL,
        help="Spreadsheet URL to pull headers from",
    )
    parser.add_argument(
        "--headers-sheet-name",
        default=DEFAULT_HEADERS_SHEET,
        help="Sheet name to pull headers from",
    )
    parser.add_argument("--limit", type=int, help="Max number of leads to convert")
    parser.add_argument("--output-sheet", default="Apollo Export", help="Sheet name for upload")
    parser.add_argument("--target-spreadsheet-id", help="Spreadsheet ID to upload to")
    parser.add_argument("--folder-id", default=DEFAULT_FOLDER_ID, help="Drive folder ID for new sheet")
    parser.add_argument("--no-upload", action="store_true", help="Skip Google Sheets upload")

    args = parser.parse_args()

    if not args.input and not args.spreadsheet_url:
        print("❌ Provide --input or --spreadsheet-url")
        sys.exit(1)

    if args.input:
        leads = load_json_file(args.input)
        source_spreadsheet_id = None
    else:
        source_spreadsheet_id = parse_spreadsheet_id(args.spreadsheet_url)
        if args.headers_from_sheet:
            creds = authenticate_google()
            service = build("sheets", "v4", credentials=creds)
            data_sheet = resolve_sheet_name(
                service, source_spreadsheet_id, args.sheet_name, args.spreadsheet_url
            )
            header_rows = load_sheet_rows(service, source_spreadsheet_id, args.headers_from_sheet)
            headers = header_rows[0] if header_rows else []
            data_rows = load_sheet_rows(service, source_spreadsheet_id, data_sheet)
            leads = build_leads_from_rows(data_rows, headers)
        else:
            leads = load_from_google_sheets(args.spreadsheet_url, args.sheet_name)

    if not leads:
        print("❌ No leads found")
        sys.exit(1)

    converted = convert_leads(leads, args.limit)
    save_json(converted, args.output)
    print(f"✅ Saved {len(converted)} leads to {args.output}")

    if not args.no_upload:
        creds = authenticate_google()
        service = build("sheets", "v4", credentials=creds)
        template_headers = load_headers_from_sheet(
            service,
            args.headers_from_url,
            args.headers_sheet_name,
        )
        if not template_headers:
            print("❌ Failed to load headers from template sheet")
            sys.exit(1)
        normalized_headers = [normalize_header_key(h) for h in template_headers]
        if "emails" not in normalized_headers:
            print("❌ Template headers must include 'emails'")
            sys.exit(1)
        sheet_url = upload_to_google_sheets(
            converted,
            args.output_sheet,
            args.target_spreadsheet_id or source_spreadsheet_id,
            None if (args.target_spreadsheet_id or source_spreadsheet_id) else args.folder_id,
            headers=template_headers,
        )
        print(f"📊 Google Sheet: {sheet_url}")


if __name__ == "__main__":
    main()
