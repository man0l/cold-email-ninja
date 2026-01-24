#!/usr/bin/env python3
"""
Clean Leads - Filter and Validate Script

Filters leads by category and review count, cleans URLs, and validates websites.
Only keeps leads with active websites (200 OK).
"""

import argparse
import os
import sys
import time
import re
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv
import requests
from requests import exceptions as req_exc
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Try to import Google Sheets libraries
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.oauth2.service_account import Credentials as ServiceAccountCredentials
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False
    print("❌ Error: Google Sheets libraries not available. Install with: pip install google-api-python-client google-auth")
    sys.exit(1)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Checkpoint Handling
def build_checkpoint_file(source_url: str, sheet_name: Optional[str], categories: Optional[List[str]]) -> str:
    """Build a stable checkpoint filename based on inputs."""
    if '/d/' in source_url:
        source_id = source_url.split('/d/')[1].split('/')[0]
    else:
        source_id = source_url

    normalized = [
        source_id.strip(),
        (sheet_name or "").strip().lower(),
        "|".join(sorted([c.strip().lower() for c in (categories or [])]))
    ]
    raw_key = "clean_leads__" + "__".join(normalized)
    safe_key = re.sub(r'[^a-z0-9_\-]+', '_', raw_key.lower())
    return f".tmp/{safe_key}.json"

def load_checkpoint(checkpoint_file: str) -> Dict[str, Any]:
    """Load checkpoint data"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_checkpoint(checkpoint_file: str, data: Dict[str, Any]):
    """Save checkpoint data"""
    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

def clear_checkpoint(checkpoint_file: str):
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
        except:
            pass

def print_progress(current, total, valid, invalid_reasons, width=40):
    """Print a progress bar"""
    percent = float(current) / total
    bar_length = int(width * percent)
    bar = '█' * bar_length + '-' * (width - bar_length)
    invalid_total = sum(invalid_reasons.values())
    invalid_summary = f" | Invalid: {invalid_total}"
    sys.stdout.write(
        f'\r[{bar}] {int(percent * 100)}% | Processed: {current}/{total} | Valid: {valid}{invalid_summary} '
    )
    sys.stdout.flush()

def authenticate_google():
    """Authenticate with Google Sheets API using credentials.json"""
    creds_path = 'credentials.json'
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    
    creds = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)
    return creds

def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load leads from Google Sheets"""
    creds = authenticate_google()
    service = build('sheets', 'v4', credentials=creds)
    
    # Extract spreadsheet ID from URL
    if '/d/' in spreadsheet_url:
        spreadsheet_id = spreadsheet_url.split('/d/')[1].split('/')[0]
    else:
        spreadsheet_id = spreadsheet_url
    
    try:
        if sheet_name:
            range_name = f"{sheet_name}!A:ZZ"
        else:
            range_name = "A:ZZ"
        
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        rows = result.get('values', [])
        if not rows:
            print("❌ No data found in sheet")
            return []
        
        headers = rows[0]
        leads = []
        for row in rows[1:]:
            row = row + [''] * (len(headers) - len(row))
            lead = {headers[i]: row[i] for i in range(len(headers))}
            leads.append(lead)
        
        return leads
    except HttpError as e:
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)

def save_to_google_sheets(leads: List[Dict[str, Any]], sheet_name: str, folder_id: Optional[str] = None, source_id: Optional[str] = None):
    """Save leads to a Google Sheet (creates new spreadsheet, optionally in specified folder)"""
    creds = authenticate_google()
    service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    target_spreadsheet_id = None

    # Always create a new spreadsheet (avoids permission issues with read-only sources)
    if folder_id:
        print(f"  📂 Creating new spreadsheet in folder: {folder_id}")
        try:
            file_metadata = {
                'name': sheet_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [folder_id]
            }
            file = drive_service.files().create(
                body=file_metadata, fields='id', supportsAllDrives=True
            ).execute()
            target_spreadsheet_id = file.get('id')
            print(f"  ✅ Created new spreadsheet: {target_spreadsheet_id}")
        except HttpError as e:
             print(f"❌ Error creating file: {e}")
             return
    else:
         # Create in root
         try:
            spreadsheet = {'properties': {'title': sheet_name}}
            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            target_spreadsheet_id = spreadsheet['spreadsheetId']
            print(f"  ✅ Created new spreadsheet in root: {target_spreadsheet_id}")
         except HttpError as e:
            print(f"❌ Error creating spreadsheet: {e}")
            return

    # If appending to existing, try to add sheet
    if target_spreadsheet_id and (source_id or folder_id):
          try:
             body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
             service.spreadsheets().batchUpdate(spreadsheetId=target_spreadsheet_id, body=body).execute()
             print(f"  ✅ Added tab '{sheet_name}'")
          except HttpError:
             pass # Sheet might exist or we just created it with Sheet1

    if not leads:
        print("❌ No leads to save")
        return
    
    headers = list(leads[0].keys())
    rows = [headers]
    for lead in leads:
        row = [str(lead.get(h, '')) for h in headers]
        rows.append(row)
    
    chunk_size = 2000
    total_rows = len(rows)
    print(f"  📤 Uploading {total_rows} rows...")
    
    for i in range(0, total_rows, chunk_size):
        chunk = rows[i:i + chunk_size]
        range_name = f"'{sheet_name}'!A1"
        try:
            service.spreadsheets().values().append(
                spreadsheetId=target_spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': chunk}
            ).execute()
        except HttpError as e:
            # Retry with Sheet1 if fresh file
            if i == 0:
                 try:
                    service.spreadsheets().values().append(
                        spreadsheetId=target_spreadsheet_id,
                        range="Sheet1!A1",
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body={'values': chunk}
                    ).execute()
                 except Exception:
                     print(f"❌ Error uploading chunk: {e}")
    
    print(f"\n✅ Saved to Google Sheet: https://docs.google.com/spreadsheets/d/{target_spreadsheet_id}")

def clean_url(url: str) -> Optional[str]:
    """Clean and normalize URL to root domain"""
    if not url:
        return None
    url = url.strip().lower()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        parsed = urlparse(url)
        host = parsed.netloc.strip().lower()
        if host.startswith("www."):
            host = host[4:]
        # Basic hostname regex to block malformed entries like ".example.com"
        if not re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}", host):
            return None
        # Reconstruct only scheme + netloc
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None

def normalize_url_key(url: str) -> str:
    """Normalize URL into a stable host key (strip scheme + www)."""
    if not url:
        return ""
    url = url.strip().lower()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        host = parsed.netloc
    except Exception:
        host = url
    if host.startswith("www."):
        host = host[4:]
    return host

def get_column_value(lead: Dict[str, Any], possible_names: List[str]) -> str:
    """Get value from lead using strict case-insensitive exact matching first, then fuzzy"""
    # 1. Exact match
    for key in lead.keys():
        if key in possible_names:
            return str(lead[key])
            
    # 2. Case insensitive match for exact words
    lead_keys_lower = {k.lower(): k for k in lead.keys()}
    for name in possible_names:
        if name.lower() in lead_keys_lower:
             return str(lead[lead_keys_lower[name.lower()]])
             
    # 3. Partial match (risky, but sometimes needed for "reviews")
    for key in lead.keys():
        for name in possible_names:
             if name.lower() in key.lower():
                 return str(lead[key])
    return ""

def check_website(url: str, timeout: int = 15, max_retries: int = 2, backoff: float = 0.6) -> Tuple[bool, str]:
    """Check if website returns 200 OK with reason if invalid.

    Retries transient failures to reduce false negatives from throttling or flakiness.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    retryable_statuses = {408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
    urls_to_try = [url]
    if url.startswith("https://"):
        urls_to_try.append("http://" + url[len("https://"):])
    elif url.startswith("http://"):
        urls_to_try.append("https://" + url[len("http://"):])

    last_reason = "request_error"

    for candidate_url in urls_to_try:
        for attempt in range(max_retries + 1):
            try:
                response = requests.head(candidate_url, headers=headers, timeout=timeout, allow_redirects=True)
                status = response.status_code
                if status == 200:
                    return True, "ok"

                # Retry with GET if HEAD fails (some servers block HEAD)
                if status in [405, 404, 403] or status >= 400:
                    response = requests.get(candidate_url, headers=headers, timeout=timeout, allow_redirects=True)
                    status = response.status_code
                    if status == 200:
                        return True, "ok"

                last_reason = f"http_{status}"

                if status in retryable_statuses and attempt < max_retries:
                    time.sleep(backoff * (2 ** attempt))
                    continue
                return False, last_reason
            except req_exc.Timeout:
                last_reason = "timeout"
            except req_exc.SSLError:
                last_reason = "ssl_error"
            except req_exc.TooManyRedirects:
                last_reason = "too_many_redirects"
            except req_exc.ConnectionError:
                last_reason = "connection_error"
            except req_exc.RequestException:
                last_reason = "request_error"

            if attempt < max_retries:
                time.sleep(backoff * (2 ** attempt))
                continue
            return False, last_reason

    return False, last_reason

def main():
    parser = argparse.ArgumentParser(description='Clean Leads - Filter and Validate')
    parser.add_argument('--source-url', required=True, help='Google Sheets URL')
    parser.add_argument('--output-sheet', required=True, help='Output Sheet/Tab Name')
    parser.add_argument('--sheet-name', help='Source sheet/tab name to read from')
    parser.add_argument('--folder-id', help='Google Drive Folder ID (falls back to GOOGLE_DRIVE_FOLDER_ID env var)')
    parser.add_argument('--category', nargs='+', help='Category filter(s) - accepts multiple values (substring match, OR logic)')
    parser.add_argument('--max-leads', type=int, help='Maximum leads to validate (optional)')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers (default: 10)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output (shows dropped leads)')
    
    args = parser.parse_args()
    
    print("\n🧹 Clean Leads Tool")
    print("=" * 50)

    
    # 1. Load Data
    print(f"Loading from: {args.source_url}")
    leads = load_from_google_sheets(args.source_url, args.sheet_name)
    if not leads:
        sys.exit(1)
    
    total_initial = len(leads)
    print(f"Total leads loaded: {total_initial}")
    
    # 2. Filter Process
    print("\n🔍 Filtering leads...")
    
    # Extended column mappings based on inspection
    category_cols = ['Category', 'category', 'Industry', 'industry', 'types.0', 'types', 'type']
    website_cols = ['website', 'Website', 'companyWebsite', 'domain', 'Domain']

    filtered_leads = []

    for lead in leads:
        drop_reason = None

        # Filter by Category (OR logic - matches if ANY category matches)
        if args.category:
            cat_val = get_column_value(lead, category_cols)
            cat_val_lower = cat_val.lower() if cat_val else ""
            matches_any = any(cat.lower() in cat_val_lower for cat in args.category)
            if not cat_val or not matches_any:
                drop_reason = f"Category mismatch ({cat_val})"

        # Clean URL
        clean = None
        if not drop_reason:
            url_val = get_column_value(lead, website_cols)
            clean = clean_url(url_val)
            if not clean:
                 drop_reason = "Invalid/Empty URL"
        
        if drop_reason:
            if args.verbose:
                name = lead.get('name', lead.get('business_id', 'Unknown'))
                print(f"  ❌ Dropped {name}: {drop_reason}")
            continue
        
        # Update lead with clean URL for validation
        lead['clean_website'] = clean
        filtered_leads.append(lead)
        
    print(f"Leads after filtering: {len(filtered_leads)}")
    
    # Apply max leads limit
    if args.max_leads and len(filtered_leads) > args.max_leads:
        print(f"  ⚠️ Limiting validation to first {args.max_leads} filtered leads")
        filtered_leads = filtered_leads[:args.max_leads]
    
    # 3. Validation Process (with Checkpoint)
    
    # Load checkpoint
    checkpoint_file = build_checkpoint_file(args.source_url, args.sheet_name, args.category)
    checkpoint_data = load_checkpoint(checkpoint_file)
    checked_stats_raw = checkpoint_data.get('checked_urls', {}) # URL -> bool or dict
    restored_leads = checkpoint_data.get('valid_leads', [])

    checked_stats: Dict[str, Dict[str, Any]] = {}
    for url, value in checked_stats_raw.items():
        if isinstance(value, dict):
            valid = bool(value.get('valid'))
            reason = value.get('reason') or ("ok" if valid else "unknown")
        else:
            valid = bool(value)
            reason = "ok" if valid else "unknown"
        key = normalize_url_key(url)
        if not key:
            continue
        existing = checked_stats.get(key)
        if not existing or (not existing["valid"] and valid):
            checked_stats[key] = {"valid": valid, "reason": reason}
    
    # Map restored leads by URL for easy lookup - Wait, we can just look up validity in checked_stats
    # But checking 'checked_stats' is only for status. If valid, we need the lead object.
    # The current 'lead' object has the original data. We just use that.
    
    leads_to_validate = []
    final_valid_leads = []
    skipped_count = 0
    
    # Identify what needs validation vs what is already done
    for lead in filtered_leads:
        url = lead.get('clean_website')
        key = normalize_url_key(url)
        if key in checked_stats:
            if checked_stats[key]["valid"]:
                # Valid
                final_valid_leads.append(lead)
            skipped_count += 1
        else:
            leads_to_validate.append(lead)
            
    if skipped_count > 0:
        print(f"  Found {skipped_count} processed URLs in checkpoint.")
    
    total_to_validate = len(leads_to_validate)
    print(f"\n🌐 Validating {total_to_validate} websites (Workers: {args.workers})...")

    if total_to_validate > 0:
        batch_size = args.workers
        processed_new = 0
        valid_new = 0

        invalid_reasons: Dict[str, int] = {}

        # Worker function
        def validate_lead(lead):
            url = lead.get('clean_website')
            is_valid, reason = check_website(url)
            return lead, is_valid, reason

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # We process in chunks to save checkpoints periodically
             for i in range(0, total_to_validate, batch_size):
                batch = leads_to_validate[i:i + batch_size]
                futures = {executor.submit(validate_lead, lead): lead for lead in batch}
                
                for future in as_completed(futures):
                    lead, is_valid, reason = future.result()
                    url = lead.get('clean_website')
                    key = normalize_url_key(url)
                    
                    processed_new += 1
                    
                    # Update stats
                    if key:
                        checked_stats[key] = {"valid": is_valid, "reason": reason}
                    
                    if is_valid:
                        final_valid_leads.append(lead)
                        valid_new += 1
                    else:
                        bucket = reason or "unknown"
                        invalid_reasons[bucket] = invalid_reasons.get(bucket, 0) + 1
                    
                    print_progress(processed_new, total_to_validate, valid_new, invalid_reasons)

                # Checkpoint every 100 *processed* leads
                if (processed_new % 100 == 0) or (processed_new == total_to_validate):
                    save_checkpoint(checkpoint_file, {
                        'checked_urls': checked_stats,
                        # We don't strictly need to save 'valid_leads' if we can reconstruct from source + checked_stats
                        # But if leads list changes (e.g. source sheet changes), 'valid_leads' in checkpoint might be stale.
                        # It's safer to rely on 'checked_urls' which is a cache of URL status.
                        # So let's just save 'checked_urls'. 
                        # But wait, logic above uses 'valid_leads' from checkpoint? NO, I removed that logic.
                        # I use 'checked_stats' to decide validity of current leads.
                        'valid_leads': [] # Not used anymore but kept structure
                    })
                    
        print() # Newline after progress bar
    else:
        print("  All leads already processed in checkpoint.")

    print(f"\n✅ Final valid leads: {len(final_valid_leads)} (Removed {total_initial - len(final_valid_leads)} total)")
    
    # 4. Save Data
    # Let's replace 'website' with 'clean_website' to ensure downstream tools work better
    for lead in final_valid_leads:
        original = get_column_value(lead, ['website', 'Website', 'companyWebsite'])
        if original:
             lead['original_website'] = original
        lead['website'] = lead.get('clean_website', '')
        # Clean up temporary field
        if 'clean_website' in lead:
            del lead['clean_website']
    
    source_id = None
    if '/d/' in args.source_url:
        source_id = args.source_url.split('/d/')[1].split('/')[0]

    # Use folder_id from args, or fall back to env var
    folder_id = args.folder_id or os.getenv('GOOGLE_DRIVE_FOLDER_ID')
    save_to_google_sheets(final_valid_leads, args.output_sheet, folder_id, source_id)
    
    # Clear checkpoint only after a successful full run.
    if total_to_validate == 0 or processed_new == total_to_validate:
        clear_checkpoint(checkpoint_file)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted! Progress saved to checkpoint.")
        sys.exit(0)
