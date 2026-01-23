#!/usr/bin/env python3
"""
Find decision maker emails using Anymail Finder.

Usage:
  python execution/anymail_find_emails.py \
    --source-url "SHEET_URL" \
    --output-sheet "Decision Maker Emails" \
    --decision-maker-category "ceo" \
    --max-leads 100
"""

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

ANYMAIL_ENDPOINT = "https://api.anymailfinder.com/v5.1/find-email/decision-maker"
CHECKPOINT_FILE = ".tmp/anymail_find_emails_checkpoint.json"

OUTPUT_FIELDS = [
    "decision_maker_email",
    "decision_maker_email_status",
    "decision_maker_name",
    "decision_maker_title",
    "decision_maker_linkedin",
]


def authenticate_google():
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    return ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)


def load_from_csv(file_path: str) -> List[Dict[str, Any]]:
    leads: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            leads.append(dict(row))
    return leads


def load_from_json(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "leads" in data:
            return data["leads"]
    print("❌ Error: JSON file must contain a list or have a 'leads' key")
    sys.exit(1)


def extract_spreadsheet_id_and_gid(spreadsheet_url: str) -> Tuple[str, Optional[int]]:
    spreadsheet_id = spreadsheet_url
    gid = None
    if "/d/" in spreadsheet_url:
        spreadsheet_id = spreadsheet_url.split("/d/")[1].split("/")[0]
    if "gid=" in spreadsheet_url:
        try:
            gid = int(spreadsheet_url.split("gid=")[1].split("&")[0].split("#")[0])
        except ValueError:
            gid = None
    return spreadsheet_id, gid


def resolve_sheet_name(service, spreadsheet_id: str, gid: int) -> Optional[str]:
    try:
        meta = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        ).execute()
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("sheetId") == gid:
                return props.get("title")
    except HttpError as exc:
        print(f"❌ Error resolving sheet name from gid: {exc}")
        sys.exit(1)
    return None


def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available. Install with: pip install google-api-python-client google-auth")
        sys.exit(1)

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id, gid = extract_spreadsheet_id_and_gid(spreadsheet_url)
    if sheet_name is None and gid is not None:
        sheet_name = resolve_sheet_name(service, spreadsheet_id, gid)

    try:
        range_name = f"{sheet_name}!A:ZZ" if sheet_name else "A:ZZ"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        ).execute()
        rows = result.get("values", [])
        if not rows:
            print("❌ No data found in sheet")
            return [], []
        headers = rows[0]
        leads: List[Dict[str, Any]] = []
        for row in rows[1:]:
            row = row + [""] * (len(headers) - len(row))
            leads.append({headers[i]: row[i] for i in range(len(headers))})
        return leads, headers
    except HttpError as exc:
        print(f"❌ Error accessing Google Sheets: {exc}")
        sys.exit(1)


def save_to_json(leads: List[Dict[str, Any]], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)


def save_checkpoint(leads: List[Dict[str, Any]], processed_indexes: List[int]) -> None:
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    payload = {
        "leads": leads,
        "processed_indexes": processed_indexes,
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_checkpoint() -> Optional[Dict[str, Any]]:
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"⚠️ Error loading checkpoint: {exc}")
        return None


def clear_checkpoint() -> None:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("🧹 Checkpoint file cleaned up")
        except Exception:
            pass


def create_spreadsheet(title: str, folder_id: Optional[str]) -> str:
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    if folder_id:
        file_metadata = {
            "name": title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        file = drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return file.get("id")

    spreadsheet = service.spreadsheets().create(
        body={"properties": {"title": title}}
    ).execute()
    return spreadsheet["spreadsheetId"]


def save_to_google_sheets(leads: List[Dict[str, Any]], output_sheet: str, folder_id: Optional[str]) -> None:
    if not leads:
        print("❌ No leads to save")
        return

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = create_spreadsheet(output_sheet, folder_id)

    headers = list(leads[0].keys())
    rows = [headers]
    for lead in leads:
        rows.append([str(lead.get(h, "")) for h in headers])

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    print(f"\n✅ Saved to Google Sheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def get_value_case_insensitive(row: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    lower_map = {k.lower(): k for k in row.keys()}
    for key in keys:
        if key in row:
            return str(row[key]).strip()
        lower = key.lower()
        if lower in lower_map:
            return str(row[lower_map[lower]]).strip()
    return None


def extract_domain_from_value(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if "://" not in value:
        parsed = urlparse(f"https://{value}")
    else:
        parsed = urlparse(value)
    domain = parsed.netloc or parsed.path
    if domain:
        return domain.replace("www.", "").strip()
    return None


def get_domain_or_company(lead: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    domain_fields = [
        "website",
        "companyWebsite",
        "company_website",
        "domain",
        "companyDomain",
        "company_domain",
    ]
    for field in domain_fields:
        value = get_value_case_insensitive(lead, [field])
        domain = extract_domain_from_value(value or "")
        if domain:
            return domain, None

    company_fields = ["company_name", "company", "name"]
    for field in company_fields:
        value = get_value_case_insensitive(lead, [field])
        if value:
            return None, value

    return None, None


def has_decision_maker_email(lead: Dict[str, Any]) -> bool:
    email_fields = [
        "decision_maker_email",
        "decision maker email",
        "decisionmakeremail",
    ]
    value = get_value_case_insensitive(lead, email_fields)
    return bool(value)


def call_anymail_finder(
    api_key: str,
    domain: Optional[str],
    company_name: Optional[str],
    categories: List[str],
    verbose: bool,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "decision_maker_category": categories,
    }
    if domain:
        payload["domain"] = domain
    if company_name and not domain:
        payload["company_name"] = company_name

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    retries = 2
    for attempt in range(retries + 1):
        try:
            response = requests.post(
                ANYMAIL_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=180,
            )
            if response.status_code == 200:
                return response.json()
            if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                wait_time = 2 ** attempt
                if verbose:
                    print(f"  ⚠️  Retrying after {wait_time}s (status {response.status_code})")
                time.sleep(wait_time)
                continue
            return {"error": f"{response.status_code} {response.text}"}
        except requests.RequestException as exc:
            if attempt < retries:
                wait_time = 2 ** attempt
                if verbose:
                    print(f"  ⚠️  Request error, retrying after {wait_time}s: {exc}")
                time.sleep(wait_time)
                continue
            return {"error": str(exc)}
    return {"error": "unknown_error"}


def normalize_leads(leads: List[Dict[str, Any]]) -> None:
    for lead in leads:
        for field in OUTPUT_FIELDS:
            lead.setdefault(field, "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Find decision maker emails via Anymail Finder.")
    parser.add_argument("--source-url", help="Google Sheet URL")
    parser.add_argument("--source-file", help="Path to CSV or JSON file")
    parser.add_argument("--sheet-name", default=None, help="Source sheet name (optional)")
    parser.add_argument("--output", help="Output JSON path")
    parser.add_argument("--output-sheet", help="Output Google Sheet name")
    parser.add_argument("--output-in-place", action="store_true", help="Update the source sheet in place")
    parser.add_argument("--folder-id", default=os.getenv("GOOGLE_DRIVE_FOLDER_ID"), help="Drive folder ID")
    parser.add_argument("--decision-maker-category", action="append", dest="categories", help="Decision maker category (repeatable)")
    parser.add_argument("--max-leads", type=int, default=100, help="Max leads to process")
    parser.add_argument("--include-existing", action="store_true", help="Process leads that already have decision maker email")
    parser.add_argument("--skip-first", type=int, default=0, help="Skip the first N leads")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--checkpoint-every", type=int, default=25, help="Save checkpoint every N processed leads")

    args = parser.parse_args()

    if not args.source_url and not args.source_file:
        print("❌ Error: Provide --source-url or --source-file")
        sys.exit(1)

    if not args.output and not args.output_sheet and not args.output_in_place:
        print("❌ Error: Provide --output, --output-sheet, or --output-in-place")
        sys.exit(1)

    if not args.categories:
        print("❌ Error: Provide at least one --decision-maker-category")
        sys.exit(1)

    api_key = os.getenv("ANYMAIL_FINDER_API_KEY")
    if not api_key:
        print("❌ Error: ANYMAIL_FINDER_API_KEY not found in environment")
        sys.exit(1)

    headers: List[str] = []
    checkpoint_data = load_checkpoint()
    processed_indexes: List[int] = []
    if checkpoint_data:
        leads = checkpoint_data.get("leads", [])
        processed_indexes = checkpoint_data.get("processed_indexes", [])
        print(f"🔁 Resuming from checkpoint: {len(processed_indexes)} processed")
    else:
        if args.source_url:
            leads, headers = load_from_google_sheets(args.source_url, args.sheet_name)
        else:
            if not os.path.exists(args.source_file):
                print(f"❌ Error: File not found: {args.source_file}")
                sys.exit(1)
            if args.source_file.lower().endswith(".json"):
                leads = load_from_json(args.source_file)
            else:
                leads = load_from_csv(args.source_file)

    if not leads:
        print("No leads found.")
        return

    if args.skip_first and not checkpoint_data:
        leads = leads[args.skip_first:]

    if args.max_leads and not checkpoint_data:
        leads = leads[: args.max_leads]

    normalize_leads(leads)

    to_process_indexes: List[int] = []
    for idx, lead in enumerate(leads):
        if not args.include_existing and has_decision_maker_email(lead):
            continue
        domain, company = get_domain_or_company(lead)
        if not domain and not company:
            continue
        if idx in processed_indexes:
            continue
        to_process_indexes.append(idx)

    total_leads = len(leads)
    total_to_process = len(to_process_indexes)
    estimated_max_cost = total_to_process * 2

    print("\n📧 Anymail Finder Decision Maker Emails")
    print("=======================================")
    print(f"Total leads: {total_leads}")
    print(f"Leads to process: {total_to_process}")
    print(f"Max leads limit: {args.max_leads}")
    print(f"Estimated cost range: 0 - {estimated_max_cost} credits")

    if args.yes:
        print("\n✅ Auto-confirmed with --yes flag")
    else:
        response = input("\n⚠️  Continue? (yes/no): ").strip().lower()
        if response != "yes":
            print("Cancelled.")
            return

    def process_index(index: int) -> Dict[str, Any]:
        lead = leads[index]
        domain, company = get_domain_or_company(lead)
        target = domain or company or ""
        result = call_anymail_finder(api_key, domain, company, args.categories, args.verbose)
        return {"target": target, "domain": domain, "result": result}

    processed = 0
    processed_since_checkpoint = 0
    max_workers = min(5, total_to_process) if total_to_process else 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(process_index, idx): idx for idx in to_process_indexes}
        for future in as_completed(future_map):
            idx = future_map[future]
            lead = leads[idx]
            payload = future.result()
            target = payload.get("target", "")
            domain = payload.get("domain") or ""
            result = payload.get("result", {})

            processed += 1
            if args.verbose and target:
                print(f"\n🔎 [{processed}/{total_to_process}] {target}")

            if "error" in result:
                lead["decision_maker_email_status"] = "error"
                lead["decision_maker_name"] = ""
                if args.verbose:
                    print(f"  ❌ Error: {result['error']}")
            else:
                email_status = result.get("email_status") or ""
                valid_email = result.get("valid_email") or ""
                lead["decision_maker_email"] = valid_email if email_status == "valid" else ""
                lead["decision_maker_email_status"] = email_status
                lead["decision_maker_name"] = result.get("person_full_name") or ""
                lead["decision_maker_title"] = result.get("person_job_title") or ""
                lead["decision_maker_linkedin"] = result.get("person_linkedin_url") or ""
                if lead["decision_maker_name"]:
                    print(f"[{processed}/{total_to_process}] {lead['decision_maker_name']} - {domain or target}")

            processed_indexes.append(idx)
            processed_since_checkpoint += 1
            if args.checkpoint_every > 0 and processed_since_checkpoint >= args.checkpoint_every:
                save_checkpoint(leads, processed_indexes)
                processed_since_checkpoint = 0

    if args.output_in_place:
        if not args.source_url:
            print("❌ Error: --output-in-place requires --source-url")
            sys.exit(1)
        if not GOOGLE_AVAILABLE:
            print("❌ Error: Google Sheets libraries not available")
            sys.exit(1)

        creds = authenticate_google()
        service = build("sheets", "v4", credentials=creds)
        spreadsheet_id, gid = extract_spreadsheet_id_and_gid(args.source_url)
        sheet_name = args.sheet_name
        if sheet_name is None and gid is not None:
            sheet_name = resolve_sheet_name(service, spreadsheet_id, gid)
        if sheet_name is None:
            sheet_name = "Sheet1"

        if not headers:
            headers = list(leads[0].keys()) if leads else []
        for field in OUTPUT_FIELDS:
            if field not in headers:
                headers.append(field)

        rows = [headers]
        for lead in leads:
            rows.append([str(lead.get(h, "")) for h in headers])

        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

        print(f"\n✅ Updated source sheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    else:
        filtered = [lead for lead in leads if lead.get("decision_maker_name")]
        if args.output:
            save_to_json(filtered, args.output)
            print(f"\n✅ Saved to JSON: {args.output}")
        else:
            save_to_google_sheets(filtered, args.output_sheet, args.folder_id)

    clear_checkpoint()


if __name__ == "__main__":
    main()
