#!/usr/bin/env python3
"""
Find Emails - Contact Enrichment Script

Enriches leads with email addresses, phone numbers, and social media contacts
by scraping company websites using the OpenWeb Ninja API.

Usage:
    # From Google Sheet to Google Sheet (Recommended)
    python execution/find_emails.py --source-url "SHEET_URL" --output-sheet "Enriched Leads" --max-leads 100
    
    # From Google Sheet to JSON
    python execution/find_emails.py --source-url "SHEET_URL" --output .tmp/enriched.json --max-leads 50
    
    # From CSV to JSON
    python execution/find_emails.py --source-file "data.csv" --output .tmp/enriched.json --max-leads 100
"""

import argparse
import csv
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from dotenv import load_dotenv
import requests
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

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def authenticate_google():
    """Authenticate with Google Sheets API using credentials.json (Service Account)"""
    creds_path = 'credentials.json'
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    
    creds = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)
    return creds


def load_from_csv(file_path: str) -> List[Dict[str, Any]]:
    """Load leads from CSV file"""
    leads = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            leads.append(dict(row))
    return leads


def load_from_json(file_path: str) -> List[Dict[str, Any]]:
    """Load leads from JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'leads' in data:
            return data['leads']
        else:
            print("❌ Error: JSON file must contain a list or have a 'leads' key")
            sys.exit(1)


def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load leads from Google Sheets"""
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available. Install with: pip install google-api-python-client google-auth")
        sys.exit(1)
    
    creds = authenticate_google()
    service = build('sheets', 'v4', credentials=creds)
    
    # Extract spreadsheet ID from URL
    if '/d/' in spreadsheet_url:
        spreadsheet_id = spreadsheet_url.split('/d/')[1].split('/')[0]
    else:
        spreadsheet_id = spreadsheet_url
    
    # Get sheet data
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
        
        # Convert to list of dicts
        headers = rows[0]
        leads = []
        for row in rows[1:]:
            # Pad row to match headers length
            row = row + [''] * (len(headers) - len(row))
            lead = {headers[i]: row[i] for i in range(len(headers))}
            leads.append(lead)
        
        return leads
    except HttpError as e:
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)


def save_to_json(leads: List[Dict[str, Any]], output_path: str):
    """Save leads to JSON file"""
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)


CHECKPOINT_FILE = ".tmp/find_emails_checkpoint.json"

def save_checkpoint(leads: List[Dict[str, Any]]):
    """Save checkpoint to prevent data loss (overwrites single file)"""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)

def load_checkpoint() -> Optional[List[Dict[str, Any]]]:
    """Load leads from checkpoint file if it exists"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading checkpoint: {e}")
    return None

def clear_checkpoint():
    """Remove checkpoint file after successful run"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("🧹 Checkpoint file cleaned up")
        except Exception:
            pass


def save_to_google_sheets(leads: List[Dict[str, Any]], sheet_name: str, spreadsheet_id: Optional[str] = None, folder_id: Optional[str] = None):
    """Save leads to a Google Sheet (adds tab if ID provided, else creates new in folder if provided)"""
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)
    
    creds = authenticate_google()
    service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Logic:
    # 1. If folder_id provided -> Create NEW spreadsheet in that folder (ignore source spreadsheet_id)
    # 2. If spreadsheet_id provided -> Append tab to that spreadsheet
    # 3. Else -> Create NEW spreadsheet in root (may fail)
    
    target_spreadsheet_id = spreadsheet_id
    
    if folder_id:
        print(f"  📂 Creating new spreadsheet in folder: {folder_id}")
        try:
            file_metadata = {
                'name': sheet_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [folder_id]
            }
            # Create file using Drive API first to place in folder
            file = drive_service.files().create(
                body=file_metadata, 
                fields='id',
                supportsAllDrives=True
            ).execute()
            target_spreadsheet_id = file.get('id')
            print(f"  ✅ Created new spreadsheet: {target_spreadsheet_id}")
            
            # Now we treat it as an existing spreadsheet to write to
            # But we might need to rename the first sheet or add header?
            # The 'values.update' below will handle writing to 'Sheet1!A1' or similar.
            # Usually new sheets have 'Sheet1'. Our sheet_name arg is used for filename here.
            # Let's write to the first sheet.
            
            # But wait, subsequent code uses `sheet_name!A1`.
            # If we created a new file, the default sheet is "Sheet1".
            # We should probably just use "Sheet1" or rename it.
            # To keep it simple, let's use "Sheet1" for the range if we just created it.
            # Or rename the sheet.
            
            # Let's stick to the plan: if we created a new file, target_spreadsheet_id is set.
            # The writing logic below uses `sheet_name`.
            # If `sheet_name` is "Enriched Leads", we should write to "Enriched Leads!A1".
            # But the new sheet has "Sheet1".
            # We should batchUpdate to rename "Sheet1" to `sheet_name`?
            # Or just write to "Sheet1" and rename the file (which we did).
            
            # Use 'Sheet1' for range if we just created it?
            # Actually, `save_to_google_sheets` takes `sheet_name` which is used for both Filename (in create) and Tab Name (in append).
            # This is slightly conflated.
            # Let's rename "Sheet1" to match `sheet_name` if possible, or just accept "Sheet1".
            
            # Simpler: just use `target_spreadsheet_id` and proceed.
            # If the range is `sheet_name!A1`, it will fail if tab doesn't exist.
            # So we MUST add the tab or rename.
            
            # Let's add the tab logic below.
            # If we just created it, it has Sheet1. We want `sheet_name`.
            # We can run the "addSheet" logic?
            pass

        except HttpError as e:
            print(f"❌ Error creating file in folder: {e}")
            return
            
    elif target_spreadsheet_id:
        # Append mode logic (same as before)
        pass
    else:
        # Create in root (fallback)
        pass

    # Ensure tab exists (for new or existing)
    if target_spreadsheet_id:
        # Try to add the sheet (tab)
        # If we just created the file, it has "Sheet1".
        # If we want a tab named `sheet_name`, we add it.
        # If `sheet_name` == "Sheet1", it exists.
        
        # If we created a new file, maybe we ignore `sheet_name` for the tab and just use "Sheet1"?
        # But if we append, we need unique name.
        
        # Let's try to add the sheet. If it exists, we catch error.
        try:
             body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
             service.spreadsheets().batchUpdate(
                spreadsheetId=target_spreadsheet_id,
                body=body
            ).execute()
             print(f"  ✅ Added tab '{sheet_name}'")
        except HttpError as e:
            if 'already exists' in str(e):
                pass # It exists, we will write to it
            else:
                # If we created new file, maybe we just write to Sheet1?
                # If `sheet_name` is different from "Sheet1", we added it above.
                pass
    else:
        # Fallback creation in root (if folder_id not provided and no source_id)
         try:
            spreadsheet = {
                'properties': {'title': sheet_name}
            }
            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            target_spreadsheet_id = spreadsheet['spreadsheetId']
            print(f"  ✅ Created new spreadsheet in root: {target_spreadsheet_id}")
         except HttpError as e:
            print(f"❌ Error creating new spreadsheet: {e}")
            return

    # Prepare data
    if not leads:
        print("❌ No leads to save")
        return
    
    headers = list(leads[0].keys())
    rows = [headers]
    for lead in leads:
        row = [str(lead.get(h, '')) for h in headers]
        rows.append(row)
    
    # Chunked upload using append to automatically add rows
    chunk_size = 2000
    total_rows = len(rows)
    print(f"  📤 Uploading {total_rows} rows in chunks of {chunk_size}...")
    
    # First chunk includes headers, so we can just append everything if we start fresh.
    # But wait, save_to_google_sheets prepares 'rows' which INCLUDES headers at rows[0].
    # So we just append all chunks sequentially.
    
    for i in range(0, total_rows, chunk_size):
        chunk = rows[i:i + chunk_size]
        
        # We use append, so we target the sheet, not specific rows
        range_name = f"'{sheet_name}'!A1"
        body = {'values': chunk}
        
        try:
            service.spreadsheets().values().append(
                spreadsheetId=target_spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            print(f"    ✓ Appended rows {i+1}-{i+len(chunk)}")
        except HttpError as e:
            # Fallback for first chunk only
            if i == 0 and "Unable to parse range" in str(e):
                 print(f"    ⚠️ Retrying chunk 1 with 'Sheet1'...")
                 try:
                    service.spreadsheets().values().append(
                        spreadsheetId=target_spreadsheet_id,
                        range="Sheet1!A1",
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body=body
                    ).execute()
                    print(f"    ✓ Appended rows {i+1}-{i+len(chunk)} to Sheet1")
                 except Exception as ex:
                     print(f"    ❌ Error appending chunk {i}: {ex}")
            else:
                 print(f"    ❌ Error appending chunk {i}: {e}")
                 
    print(f"\n✅ Saved to Google Sheet: https://docs.google.com/spreadsheets/d/{target_spreadsheet_id}")
    



def get_website_url(lead: Dict[str, Any]) -> Optional[str]:
    """Extract website URL from lead data"""
    # Try different field names
    for field in ['website', 'companyWebsite', 'company_website', 'domain', 'companyDomain', 'company_domain']:
        url = lead.get(field, '').strip()
        if url:
            # Ensure URL has protocol
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            return url
    return None


def has_email(lead: Dict[str, Any]) -> bool:
    """Check if lead already has an email"""
    email_fields = ['email', 'Email', 'personEmail', 'person_email', 'contactEmail', 'contact_email']
    for field in email_fields:
        if lead.get(field, '').strip():
            return True
    return False


def scrape_contacts(website_url: str, api_key: str, verbose: bool = False) -> Dict[str, Any]:
    """
    Scrape contact information from a website using OpenWeb Ninja API
    
    Returns:
        Dict with emails, phones, and social media profiles
    """
    url = "https://api.openwebninja.com/website-contacts-scraper/scrape-contacts"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    params = {"query": website_url}
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if verbose:
                    print(f"  ✓ Response: {json.dumps(data, indent=2)}")
                
                if data.get('status') == 'OK' and data.get('data'):
                    # Return the first result from the data list
                    return data['data'][0]
                else:
                    if verbose:
                        print(f"  ⚠ API returned no data: {data}")
                    return {}
            
            elif response.status_code == 429:  # Rate limit
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    if verbose:
                        print(f"  ⚠ Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  ❌ Rate limit exceeded after {max_retries} attempts")
                    return {}
            
            else:
                if verbose:
                    print(f"  ❌ API error {response.status_code}: {response.text}")
                return {}
        
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                if verbose:
                    print(f"  ⚠ Timeout, retrying...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"  ❌ Timeout after {max_retries} attempts")
                return {}
        
        except Exception as e:
            if verbose:
                print(f"  ❌ Error: {str(e)}")
            return {}
    
    return {}


def enrich_lead(lead: Dict[str, Any], api_key: str, verbose: bool = False) -> Dict[str, Any]:
    """Enrich a single lead with contact information"""
    website_url = get_website_url(lead)
    
    if not website_url:
        if verbose:
            print(f"  ⚠ No website URL found")
        return lead
    
    if verbose:
        print(f"  🔍 Scraping: {website_url}")
    
    contacts = scrape_contacts(website_url, api_key, verbose)
    
    if contacts:
        # Add emails - New structure: [{"value": "email@example.com", "sources": [...]}]
        emails_data = contacts.get('emails', [])
        emails = [e.get('value') for e in emails_data if e.get('value')]
        if emails:
            lead['email'] = emails[0]  # Primary email
            lead['emails'] = emails  # All emails
            if verbose:
                print(f"  ✓ Found {len(emails)} email(s): {', '.join(emails)}")
        
        # Add phones - New structure: [{"value": "1234567890", "sources": [...]}]
        phones_data = contacts.get('phone_numbers', [])
        phones = [p.get('value') for p in phones_data if p.get('value')]
        if phones:
            lead['phone'] = phones[0]  # Primary phone
            lead['phones'] = phones  # All phones
            if verbose:
                print(f"  ✓ Found {len(phones)} phone(s): {', '.join(phones)}")
        
        # Add social media profiles - New structure: Top level keys in data object
        social_platforms = ['facebook', 'instagram', 'linkedin', 'twitter', 'tiktok', 'github', 'youtube', 'pinterest', 'snapchat']
        found_social = 0
        for platform in social_platforms:
            url = contacts.get(platform)
            if url:
                lead[f'social_{platform}'] = url
                found_social += 1
        
        if verbose and found_social > 0:
            print(f"  ✓ Found {found_social} social profile(s)")
    else:
        if verbose:
            print(f"  ⚠ No contacts found")
    
    return lead


def main():
    parser = argparse.ArgumentParser(description='Find emails and contact information using OpenWeb Ninja API')
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--source-file', help='Path to CSV or JSON file')
    input_group.add_argument('--source-url', help='Google Sheets URL')
    
    # Output options
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument('--output', help='Output JSON file path')
    output_group.add_argument('--output-sheet', help='Output Google Sheet name')
    
    # Other options
    parser.add_argument('--sheet-name', help='Source sheet name (for Google Sheets input)')
    parser.add_argument('--max-leads', type=int, default=100, help='Maximum number of leads to process (default: 100)')
    parser.add_argument('--include-existing', action='store_true', help='Process leads that already have emails')
    parser.add_argument('--folder-id', help='Google Drive folder ID to create new spreadsheet in (falls back to GOOGLE_DRIVE_FOLDER_ID env var)')
    parser.add_argument('--yes', '-y', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Get API key
    api_key = os.getenv('OPENWEBNINJA_API_KEY')
    if not api_key:
        print("❌ Error: OPENWEBNINJA_API_KEY not found in .env file")
        sys.exit(1)
    api_key = api_key.strip()
    
    # Load leads
    print("\n📧 Find Emails Tool")
    print("=" * 50)
    
    if args.source_file:
        print(f"\n📂 Loading from file: {args.source_file}")
        if args.source_file.endswith('.csv'):
            leads = load_from_csv(args.source_file)
        elif args.source_file.endswith('.json'):
            leads = load_from_json(args.source_file)
        else:
            print("❌ Error: File must be .csv or .json")
            sys.exit(1)
    else:
        print(f"\n📊 Loading from Google Sheets: {args.source_url}")
        leads = load_from_google_sheets(args.source_url, args.sheet_name)
    
    # leads variable will hold the data
    leads = None
    
    # Check for checkpoint
    ckpt_leads = load_checkpoint()
    if ckpt_leads:
        print(f"\n⚠️  Found an interrupted run ({len(ckpt_leads)} leads).")
        resume = input("Resume from checkpoint? (yes/no): ").strip().lower()
        if resume == 'yes':
            leads = ckpt_leads
            print("📂 Loaded leads from checkpoint")
    
    # If no checkpoint loaded, load from source
    if not leads:
        if args.source_file:
            print(f"\n📂 Loading from file: {args.source_file}")
            if args.source_file.endswith('.csv'):
                leads = load_from_csv(args.source_file)
            elif args.source_file.endswith('.json'):
                leads = load_from_json(args.source_file)
            else:
                print("❌ Error: File must be .csv or .json")
                sys.exit(1)
        else:
            print(f"\n📊 Loading from Google Sheets: {args.source_url}")
            leads = load_from_google_sheets(args.source_url, args.sheet_name)
    
    if not leads:
        print("❌ No leads found")
        sys.exit(1)
    
    # Filter leads
    if args.include_existing:
        leads_to_process = leads
        print(f"\n📊 Total leads: {len(leads)}")
        print(f"   Will process: {len(leads_to_process)} leads (including existing emails)")
    else:
        # If resuming, some might already be processed, so we check again
        leads_to_process = [lead for lead in leads if not has_email(lead)]
        print(f"\n📊 Summary:")
        print(f"   Total leads: {len(leads)}")
        print(f"   Leads without email: {len(leads_to_process)}")
        # Check if we are done
        if not leads_to_process:
             print("✨ All leads appear to be processed! Checking if we just need to save...")
             # Just fall through to save? Or exit? Use max_leads to control flow?
             # If resuming and all done, maybe user just wants to export.
        
        limit = min(len(leads_to_process), args.max_leads)
        print(f"   Will process: {limit} leads (only empty emails)")
    
    # Apply max leads limit
    if len(leads_to_process) > args.max_leads:
        leads_to_process = leads_to_process[:args.max_leads]
    
    print(f"   Max leads limit: {args.max_leads}")
    print(f"   Estimated cost: ~{len(leads_to_process)} credits")
    
    # Ask for confirmation
    print(f"\n⚠️  WARNING: This will consume API credits!")
    print("=" * 50)
    if args.yes:
        print("\n✅ Auto-confirmed with --yes flag")
    else:
        response = input("\nContinue? (yes/no): ").strip().lower()
        if response != 'yes':
            print("❌ Cancelled")
            sys.exit(0)
    
    # Process leads with concurrent requests (5 per second)
    print(f"\n🔄 Processing {len(leads_to_process)} leads with 5 concurrent requests...")
    
    stats = {
        'emails_found': 0,
        'phones_found': 0,
        'social_found': 0,
        'no_contacts': 0,
        'processed': 0
    }
    
    def process_single_lead(lead_data):
        """Process a single lead - used for concurrent execution"""
        i, lead = lead_data
        lead_index = leads.index(lead)
        enriched = enrich_lead(lead, api_key, args.verbose)
        return i, lead_index, enriched
    
    # Process leads in batches of 5 concurrently
    batch_size = 5
    total_leads = len(leads_to_process)
    
    for batch_start in range(0, total_leads, batch_size):
        batch_end = min(batch_start + batch_size, total_leads)
        batch = [(i + 1, leads_to_process[i]) for i in range(batch_start, batch_end)]
        
        print(f"\n📦 Processing batch {batch_start//batch_size + 1} (leads {batch_start + 1}-{batch_end})...")
        
        # Process batch concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_single_lead, lead_data): lead_data for lead_data in batch}
            
            for future in as_completed(futures):
                i, lead_index, enriched = future.result()
                leads[lead_index] = enriched
                
                # Update stats
                stats['processed'] += 1
                if enriched.get('email'):
                    stats['emails_found'] += 1
                if enriched.get('phone'):
                    stats['phones_found'] += 1
                if any(k.startswith('social_') for k in enriched.keys()):
                    stats['social_found'] += 1
                if not enriched.get('email') and not enriched.get('phone'):
                    stats['no_contacts'] += 1
                
                print(f"  ✓ [{i}/{total_leads}] Completed")
        
        # Save checkpoint every 10 leads
        if batch_end % 10 == 0 or (batch_end > (batch_end // 10) * 10 and batch_end <= total_leads):
             save_checkpoint(leads)
             print(f"  💾 Checkpoint updated ({len(leads) - len(leads_to_process) + batch_end}/{len(leads)} total)")
        
        # Small delay between batches to respect rate limits (5 requests per second)
        if batch_end < total_leads:
            time.sleep(0.2)  # 200ms delay = 5 requests per second
    
    # Save results
    print(f"\n💾 Saving results...")
    if args.output:
        save_to_json(leads, args.output)
        print(f"✅ Saved to: {args.output}")
    else:
        # Determine target:
        # 1. If folder_id -> Create new file there (pass folder_id)
        # 2. If source_url -> Append to it (pass id)

        # Use folder_id from args, or fall back to env var
        folder_id = args.folder_id or os.getenv('GOOGLE_DRIVE_FOLDER_ID')

        source_id = None
        if not folder_id:  # Only use source_id if NOT creating new file
            if args.source_url and '/d/' in args.source_url:
                source_id = args.source_url.split('/d/')[1].split('/')[0]
            elif args.source_url:
                source_id = args.source_url

        save_to_google_sheets(leads_to_process, args.output_sheet, source_id, folder_id)
    
    # Clear checkpoint on success
    clear_checkpoint()
    
    # Print summary
    print(f"\n✅ Contact Enrichment Summary:")
    print(f"   Emails found: {stats['emails_found']} ({stats['emails_found']*100//stats['processed'] if stats['processed'] > 0 else 0}%)")
    print(f"   Phone numbers found: {stats['phones_found']} ({stats['phones_found']*100//stats['processed'] if stats['processed'] > 0 else 0}%)")
    print(f"   Social profiles found: {stats['social_found']} ({stats['social_found']*100//stats['processed'] if stats['processed'] > 0 else 0}%)")
    print(f"   No contacts found: {stats['no_contacts']} ({stats['no_contacts']*100//stats['processed'] if stats['processed'] > 0 else 0}%)")
    print(f"   Total processed: {stats['processed']}")
    print()


if __name__ == '__main__':
    main()
