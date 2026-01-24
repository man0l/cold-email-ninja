#!/usr/bin/env python3
"""
Casualise company names by removing common suffixes/descriptors.

Supports single name, JSON file, or Google Sheets column processing.
"""
import argparse
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
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

try:
    import requests
except ImportError:
    print("❌ Error: requests library not available.")
    print("   Install with: pip install requests")
    sys.exit(1)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

OPENAI_RATE_LIMIT_LOCK = threading.Lock()
OPENAI_LAST_CALL = 0.0
OPENAI_CONCURRENCY = threading.Semaphore(int(os.getenv("OPENAI_MAX_CONCURRENCY", "2")))
OPENAI_RETRY_MAX = int(os.getenv("OPENAI_RETRY_MAX", "4"))
OPENAI_RETRY_BASE = float(os.getenv("OPENAI_RETRY_BASE", "1.5"))


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


def index_to_column(index: int) -> str:
    index += 1
    column = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        column = chr(65 + remainder) + column
    return column


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def render_progress(current: int, total: int, prefix: str = "") -> None:
    if total <= 0:
        return
    percent = current / total
    bar_len = 30
    filled = int(round(bar_len * percent))
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stdout.write(f"\r{prefix}[{bar}] {current}/{total} {percent * 100:5.1f}%")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")


def sanitize_checkpoint_part(value: str) -> str:
    safe = re.sub(r"[^a-z0-9_\-]+", "_", value.lower().strip())
    return safe.strip("_") or "na"


def checkpoint_path(
    spreadsheet_id: str,
    sheet_name: Optional[str],
    column_name: Optional[str],
    output_column: str,
    checkpoint_id: Optional[str],
) -> str:
    sheet_part = sanitize_checkpoint_part(sheet_name or "sheet")
    column_part = sanitize_checkpoint_part(column_name or "company_name")
    output_part = sanitize_checkpoint_part(output_column or "casual_name")
    if checkpoint_id:
        unique_part = sanitize_checkpoint_part(checkpoint_id)
    else:
        unique_part = f"pid_{os.getpid()}"
    filename = (
        "casualise_company_name__"
        f"{sanitize_checkpoint_part(spreadsheet_id)}__{sheet_part}__"
        f"{column_part}__{output_part}__{unique_part}.json"
    )
    return os.path.join(".tmp", filename)


def load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_checkpoint(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(".tmp", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


SUFFIXES = [
    "professional services",
    "professional service",
    "production",
    "productions",
    "consulting",
    "consultants",
    "consultant",
    "consultancy",
    "partnership",
    "associates",
    "solutions",
    "technologies",
    "technology",
    "creative",
    "creatives",
    "services",
    "service",
    "studio",
    "studios",
    "software",
    "digital",
    "media",
    "agency",
    "agencies",
    "partners",
    "partner",
    "group",
    "limited",
    "corporation",
    "corp",
    "inc",
    "llc",
    "ltd",
    "company",
    "co",
]

DESCRIPTOR_ONLY = {
    "agency",
    "agencies",
    "services",
    "service",
    "consulting",
    "consultants",
    "consultant",
    "consultancy",
    "group",
    "partners",
    "partner",
    "solutions",
    "technologies",
    "technology",
    "digital",
    "media",
    "studio",
    "studios",
    "productions",
    "production",
    "creative",
    "creatives",
    "professional services",
    "professional service",
    "the agency",
}


def casualise_name(name: str, verbose: bool = False) -> str:
    original = normalize_space(str(name))
    if not original:
        return original
    return openai_casualise_name(original, verbose=verbose)


def openai_casualise_name(original: str, verbose: bool = False) -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("❌ Error: OPENAI_API_KEY not found in .env file")
        sys.exit(1)

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You shorten company names for casual outreach. "
                    "Return JSON only with key 'casual_name'."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Rules:\n"
                    "- Shorten the company name wherever possible.\n"
                    "- Remove legal suffixes and descriptors (Inc, LLC, Ltd, Corp, Company, "
                    "Agency, Services, Group, Partners, Consulting, Solutions, Technologies, "
                    "Media, Studio, Productions, Digital, Builders, Construction, Custom).\n"
                    "- Preserve the core brand (e.g., 'Love AMS' stays 'Love AMS').\n"
                    "- If shortening makes it too short (<2 chars) or removes the brand, keep original.\n"
                    "\nExamples:\n"
                    "AARON FLINT BUILDERS -> Aaron Flint\n"
                    "Westview Construction -> Westview\n"
                    "Redemption Custom Builders LLC -> Redemption\n"
                    "XYZ Agency -> XYZ\n"
                    "Love AMS Professional Services -> Love AMS\n"
                    "Love Mayo Inc. -> Love Mayo\n"
                    "\nCompany name: "
                    f"{original}"
                ),
            },
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    with OPENAI_CONCURRENCY:
        for attempt in range(1, OPENAI_RETRY_MAX + 1):
            try:
                if verbose:
                    print(
                        f"↳ GPT request ({attempt}/{OPENAI_RETRY_MAX}): {original}",
                        flush=True,
                    )
                with OPENAI_RATE_LIMIT_LOCK:
                    min_interval = float(os.getenv("OPENAI_MIN_INTERVAL", "1.0"))
                    elapsed = time.time() - OPENAI_LAST_CALL
                    if elapsed < min_interval:
                        time.sleep(min_interval - elapsed)
                    globals()["OPENAI_LAST_CALL"] = time.time()

                response = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json=payload,
                    timeout=45,
                )
                if response.status_code == 429:
                    if verbose:
                        print(
                            f"  ⚠ OpenAI rate limited (attempt {attempt}/{OPENAI_RETRY_MAX})",
                            flush=True,
                        )
                    time.sleep(OPENAI_RETRY_BASE ** attempt)
                    continue
                if response.status_code >= 400:
                    if verbose:
                        snippet = response.text[:500].replace("\n", " ")
                        print(f"  ⚠ GPT HTTP {response.status_code}: {snippet}", flush=True)
                    response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                result = json.loads(content)
                casual_name = normalize_space(str(result.get("casual_name", "")))
                if not casual_name or len(casual_name) < 2:
                    if verbose:
                        print("  ⚠ GPT returned empty/short; keeping original", flush=True)
                    return original
                if verbose:
                    print(f"  ✅ {original} -> {casual_name}", flush=True)
                return casual_name
            except Exception as exc:
                if verbose:
                    print(f"  ⚠ GPT error, retrying... ({exc})", flush=True)
                time.sleep(OPENAI_RETRY_BASE ** attempt)

    return heuristic_casualise_name(original, verbose=verbose)


def heuristic_casualise_name(original: str, verbose: bool = False) -> str:
    cleaned = original
    cleaned = re.sub(r"[,\s]+$", "", cleaned)

    changed = True
    while changed:
        changed = False
        for suffix in sorted(SUFFIXES, key=len, reverse=True):
            pattern = re.compile(rf"(?:,)?\s+{re.escape(suffix)}\.?$", re.IGNORECASE)
            if pattern.search(cleaned):
                if suffix in {"co", "company"} and re.search(r"(&|\band)\s+co\.?$", cleaned, re.IGNORECASE):
                    continue
                updated = pattern.sub("", cleaned).strip(" ,")
                if updated and updated != cleaned:
                    cleaned = normalize_space(updated)
                    changed = True
                break

    candidate = normalize_space(cleaned)
    if not candidate or len(candidate) < 2:
        return original
    if candidate.lower() in DESCRIPTOR_ONLY:
        return original

    if verbose:
        print(f"{original} -> {candidate}")
    return candidate


def extract_company_name(record: Dict[str, Any]) -> str:
    for key in ["company_name", "companyName", "Company Name", "name", "Name", "business_name", "Business Name"]:
        if key in record and str(record[key]).strip():
            return str(record[key]).strip()
    return ""


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


def save_json(data: List[Dict[str, Any]], output_file: str) -> None:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def process_json_file(
    source_file: str,
    output_file: str,
    output_field: str,
    verbose: bool,
    show_progress: bool,
    workers: int,
) -> None:
    records = load_json_file(source_file)
    companies = [extract_company_name(record) for record in records]
    if workers < 1:
        workers = 1
    if workers == 1:
        for idx, (record, company) in enumerate(zip(records, companies), start=1):
            if company:
                if verbose:
                    print(f"[{idx}/{len(records)}] {company}", flush=True)
                record[output_field] = casualise_name(company, verbose=verbose)
            if show_progress and not verbose:
                render_progress(idx, len(records), prefix="Processing ")
    else:
        def process_company(company: Optional[str]) -> Optional[str]:
            if not company:
                return None
            return casualise_name(company, verbose=verbose)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(process_company, companies)
            for idx, (record, company, result) in enumerate(
                zip(records, companies, results), start=1
            ):
                if company and result:
                    if verbose:
                        print(f"[{idx}/{len(records)}] {company}", flush=True)
                    record[output_field] = result
                if show_progress and not verbose:
                    render_progress(idx, len(records), prefix="Processing ")
    save_json(records, output_file)
    print(f"✅ Saved casualised names to {output_file}")


def find_column_index(headers: List[str], column_name: Optional[str]) -> Optional[int]:
    if column_name:
        for idx, header in enumerate(headers):
            if header.strip().lower() == column_name.strip().lower():
                return idx
        return None
    for name in ["company_name", "company name", "companyname", "name", "business name", "business_name"]:
        for idx, header in enumerate(headers):
            if header.strip().lower() == name:
                return idx
    return None


def update_google_sheet(
    source_url: str,
    sheet_name: Optional[str],
    column_name: Optional[str],
    output_column: str,
    limit: Optional[int],
    verbose: bool,
    show_progress: bool,
    checkpoint_id: Optional[str],
    workers: int,
) -> None:
    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = parse_spreadsheet_id(source_url)
    resolved_sheet = resolve_sheet_name(service, spreadsheet_id, sheet_name, source_url)
    checkpoint_file = checkpoint_path(
        spreadsheet_id, resolved_sheet, column_name, output_column, checkpoint_id
    )

    rows = load_sheet_rows(service, spreadsheet_id, resolved_sheet)
    if not rows:
        print("❌ No data found in sheet")
        sys.exit(1)

    headers = rows[0]
    if not headers:
        print("❌ Sheet is missing headers")
        sys.exit(1)

    source_idx = find_column_index(headers, column_name)
    if source_idx is None:
        print("❌ Company name column not found")
        sys.exit(1)

    output_idx = None
    for idx, header in enumerate(headers):
        if header.strip().lower() == output_column.strip().lower():
            output_idx = idx
            break
    if output_idx is None:
        headers.append(output_column)
        output_idx = len(headers) - 1

    output_col_letter = index_to_column(output_idx)
    header_range = f"'{resolved_sheet}'!{output_col_letter}1:{output_col_letter}1"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=header_range,
        valueInputOption="RAW",
        body={"values": [[headers[output_idx]]]},
    ).execute()

    data_rows = rows[1:]
    if limit:
        data_rows = data_rows[:limit]

    total_rows = len(data_rows)
    checkpoint = load_checkpoint(checkpoint_file)
    start_row = 0
    last_written_row = 0
    pending_rows: List[Tuple[int, str]] = []
    if checkpoint:
        if (
            checkpoint.get("spreadsheet_id") == spreadsheet_id
            and checkpoint.get("sheet_name") == resolved_sheet
            and checkpoint.get("column_name") == (column_name or "")
            and checkpoint.get("output_column") == output_column
        ):
            start_row = int(checkpoint.get("last_processed_row", 0))
            last_written_row = int(checkpoint.get("last_written_row", 0))
            pending_rows = [
                (int(item.get("row", 0)), str(item.get("value", "")))
                for item in checkpoint.get("pending_rows", [])
                if item.get("row")
            ]

    processed_since_checkpoint = 0

    def flush_pending_rows() -> None:
        nonlocal pending_rows, last_written_row
        while len(pending_rows) >= 100:
            chunk = pending_rows[:100]
            start = chunk[0][0]
            values = [[value] for _, value in chunk]
            first_sheet_row = start + 1
            last_sheet_row = start + len(values)
            target_range = (
                f"'{resolved_sheet}'!{output_col_letter}{first_sheet_row}:"
                f"{output_col_letter}{last_sheet_row}"
            )
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=target_range,
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
            last_written_row = start + len(values) - 1
            pending_rows = pending_rows[len(values) :]

    rows_to_process: List[Tuple[int, str]] = []
    for row_idx, row in enumerate(data_rows, start=1):
        if row_idx <= start_row:
            continue
        row = row + [""] * (len(headers) - len(row))
        company = row[source_idx] if source_idx < len(row) else ""
        if verbose:
            print(f"[{row_idx}/{total_rows}] {company}", flush=True)
        rows_to_process.append((row_idx, company))

    if workers < 1:
        workers = 1

    def process_company(item: Tuple[int, str]) -> str:
        _, company = item
        if not company:
            return ""
        return casualise_name(company, verbose=verbose)

    if workers == 1:
        results = map(process_company, rows_to_process)
        for (row_idx, _), casualised in zip(rows_to_process, results):
            pending_rows.append((row_idx, casualised))
            processed_since_checkpoint += 1

            if show_progress and not verbose:
                render_progress(row_idx, total_rows, prefix="Processing ")

            flush_pending_rows()

            if processed_since_checkpoint >= 10:
                save_checkpoint(
                    checkpoint_file,
                    {
                        "spreadsheet_id": spreadsheet_id,
                        "sheet_name": resolved_sheet,
                        "column_name": column_name or "",
                        "output_column": output_column,
                        "last_processed_row": row_idx,
                        "last_written_row": last_written_row,
                        "pending_rows": [
                            {"row": row, "value": value} for row, value in pending_rows
                        ],
                    },
                )
                processed_since_checkpoint = 0
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(process_company, rows_to_process)
            for (row_idx, _), casualised in zip(rows_to_process, results):
                pending_rows.append((row_idx, casualised))
                processed_since_checkpoint += 1

                if show_progress and not verbose:
                    render_progress(row_idx, total_rows, prefix="Processing ")

                flush_pending_rows()

                if processed_since_checkpoint >= 10:
                    save_checkpoint(
                        checkpoint_file,
                        {
                            "spreadsheet_id": spreadsheet_id,
                            "sheet_name": resolved_sheet,
                            "column_name": column_name or "",
                            "output_column": output_column,
                            "last_processed_row": row_idx,
                            "last_written_row": last_written_row,
                            "pending_rows": [
                                {"row": row, "value": value} for row, value in pending_rows
                            ],
                        },
                    )
                    processed_since_checkpoint = 0

    flush_pending_rows()
    if pending_rows:
        start = pending_rows[0][0]
        values = [[value] for _, value in pending_rows]
        first_sheet_row = start + 1
        last_sheet_row = start + len(values)
        target_range = (
            f"'{resolved_sheet}'!{output_col_letter}{first_sheet_row}:"
            f"{output_col_letter}{last_sheet_row}"
        )
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=target_range,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
        last_written_row = start + len(values) - 1
        pending_rows = []

    save_checkpoint(
        checkpoint_file,
        {
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": resolved_sheet,
            "column_name": column_name or "",
            "output_column": output_column,
            "last_processed_row": total_rows,
            "last_written_row": last_written_row,
            "pending_rows": [],
        }
    )

    print(f"✅ Updated {total_rows} rows in '{resolved_sheet}'")
    print(f"📊 Google Sheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def main():
    parser = argparse.ArgumentParser(description="Casualise company names")
    parser.add_argument("--name", help="Single company name to casualise")
    parser.add_argument("--source-file", help="Path to JSON file with leads")
    parser.add_argument("--output", default=".tmp/leads_casualised.json", help="Output JSON path")
    parser.add_argument("--output-field", default="casual_company_name", help="JSON output field")
    parser.add_argument("--source-url", help="Google Sheets URL")
    parser.add_argument("--sheet-name", help="Sheet name in spreadsheet")
    parser.add_argument("--column", help="Column name for company names")
    parser.add_argument("--output-column", default="Casual Name", help="Output column name")
    parser.add_argument("--limit", type=int, help="Max number of rows to process")
    parser.add_argument("--verbose", action="store_true", help="Print name transformations")
    parser.add_argument("--no-progress", action="store_false", dest="progress", help="Disable progress bar")
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("OPENAI_MAX_CONCURRENCY", "2")),
        help="Number of worker threads",
    )
    parser.add_argument(
        "--checkpoint-id",
        help="Stable checkpoint id to resume the same run (default: pid-based unique file)",
    )

    args = parser.parse_args()

    if args.name:
        print(casualise_name(args.name, verbose=args.verbose))
        return

    if args.source_file:
        process_json_file(
            args.source_file,
            args.output,
            args.output_field,
            args.verbose,
            args.progress,
            args.workers,
        )
        return

    if args.source_url:
        update_google_sheet(
            args.source_url,
            args.sheet_name,
            args.column,
            args.output_column,
            args.limit,
            args.verbose,
            args.progress,
            args.checkpoint_id,
            args.workers,
        )
        return

    print("❌ Provide --name, --source-file, or --source-url")
    sys.exit(1)


if __name__ == "__main__":
    main()
