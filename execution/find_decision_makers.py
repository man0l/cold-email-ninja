#!/usr/bin/env python3
"""
Find Decision Makers - Waterfall Enrichment Script

Waterfall logic:
1) About/Contact/Team/Leadership pages -> OpenAI extraction
2) Terms/Legal/Privacy pages -> OpenAI extraction
3) LinkedIn company page via DataForSEO search
4) LinkedIn employee profiles via RapidAPI li-data-scraper search + title filtering
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv

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
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CHECKPOINT_FILE = ".tmp/find_decision_makers_checkpoint.json"

OPENAI_RATE_LIMIT_LOCK = threading.Lock()
OPENAI_LAST_CALL = 0.0
OPENAI_CONCURRENCY = threading.Semaphore(int(os.getenv("OPENAI_MAX_CONCURRENCY", "2")))
OPENAI_RETRY_MAX = int(os.getenv("OPENAI_RETRY_MAX", "4"))
OPENAI_RETRY_BASE = float(os.getenv("OPENAI_RETRY_BASE", "1.5"))

DECISION_TITLES = [
    "owner",
    "founder",
    "ceo",
    "president",
    "managing director",
    "managing partner",
    "principal",
    "director",
    "partner",
    "coo",
    "cfo",
    "cto",
    "cmo",
    "cio",
    "general manager",
    "gm",
]


def authenticate_google():
    """Authenticate with Google Sheets API using credentials.json (Service Account)."""
    creds_path = "credentials.json"
    if not os.path.exists(creds_path):
        print(f"❌ Error: {creds_path} not found")
        sys.exit(1)
    creds = ServiceAccountCredentials.from_service_account_file(creds_path, scopes=SCOPES)
    return creds


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


def load_from_google_sheets(spreadsheet_url: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)

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
            lead = {headers[i]: row[i] for i in range(len(headers))}
            leads.append(lead)
        return leads
    except HttpError as e:
        print(f"❌ Error accessing Google Sheets: {e}")
        sys.exit(1)


def save_to_json(leads: List[Dict[str, Any]], output_path: str):
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)


def save_checkpoint(data: Dict[str, Any]):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_checkpoint() -> Optional[Dict[str, Any]]:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
                if isinstance(payload, list):
                    return {"legacy_leads": payload}
                if isinstance(payload, dict):
                    return payload
        except Exception as e:
            print(f"⚠️ Error loading checkpoint: {e}")
    return None


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print("🧹 Checkpoint file cleaned up")
        except Exception:
            pass


def build_headers(leads: List[Dict[str, Any]]) -> List[str]:
    required_fields = [
        "decision_maker_name",
        "decision_maker_title",
        "decision_maker_source",
        "decision_maker_confidence",
        "decision_maker_linkedin",
        "company_linkedin",
        "decision_maker_checked",
    ]
    all_keys: List[str] = []
    seen = set()
    for key in required_fields:
        if key not in seen:
            all_keys.append(key)
            seen.add(key)
    for lead in leads:
        for key in lead.keys():
            if key.startswith("_"):
                continue
            if key not in seen:
                all_keys.append(key)
                seen.add(key)
    return all_keys


def build_sheets_service():
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)
    creds = authenticate_google()
    return build("sheets", "v4", credentials=creds)


def build_drive_service():
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)
    creds = authenticate_google()
    return build("drive", "v3", credentials=creds)


def ensure_output_sheet(
    sheet_name: str,
    folder_id: Optional[str],
    existing_sheet_id: Optional[str],
) -> str:
    if existing_sheet_id:
        return existing_sheet_id

    service = build_sheets_service()
    drive_service = build_drive_service()
    try:
        if folder_id:
            file_metadata = {
                "name": sheet_name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            }
            file = drive_service.files().create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            spreadsheet_id = file.get("id")
        else:
            spreadsheet = {"properties": {"title": sheet_name}}
            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = spreadsheet["spreadsheetId"]
    except HttpError as e:
        print(f"❌ Error creating spreadsheet: {e}")
        sys.exit(1)

    try:
        sheet_info = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        default_sheet_id = sheet_info["sheets"][0]["properties"]["sheetId"]
        default_title = sheet_info["sheets"][0]["properties"]["title"]
        if default_title != sheet_name:
            body = {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": default_sheet_id, "title": sheet_name},
                            "fields": "title",
                        }
                    }
                ]
            }
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    except HttpError:
        pass

    return spreadsheet_id


def append_rows_to_google_sheet(spreadsheet_id: str, sheet_name: str, rows: List[List[str]]):
    if not rows:
        return
    service = build_sheets_service()
    range_name = f"'{sheet_name}'!A1"
    body = {"values": rows}
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
    except HttpError as e:
        print(f"    ❌ Error appending rows: {e}")


def save_to_google_sheets(leads: List[Dict[str, Any]], sheet_name: str, folder_id: Optional[str] = None):
    if not GOOGLE_AVAILABLE:
        print("❌ Error: Google Sheets libraries not available")
        sys.exit(1)

    creds = authenticate_google()
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    try:
        if folder_id:
            file_metadata = {
                "name": sheet_name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            }
            file = drive_service.files().create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            spreadsheet_id = file.get("id")
        else:
            spreadsheet = {"properties": {"title": sheet_name}}
            spreadsheet = service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = spreadsheet["spreadsheetId"]
    except HttpError as e:
        print(f"❌ Error creating spreadsheet: {e}")
        sys.exit(1)

    # Rename default sheet to desired tab name (if needed)
    try:
        sheet_info = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        default_sheet_id = sheet_info["sheets"][0]["properties"]["sheetId"]
        default_title = sheet_info["sheets"][0]["properties"]["title"]
        if default_title != sheet_name:
            body = {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": default_sheet_id, "title": sheet_name},
                            "fields": "title",
                        }
                    }
                ]
            }
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    except HttpError:
        pass

    if not leads:
        print("❌ No leads to save")
        return

    required_fields = [
        "decision_maker_name",
        "decision_maker_title",
        "decision_maker_source",
        "decision_maker_confidence",
        "decision_maker_linkedin",
        "company_linkedin",
        "decision_maker_checked",
    ]
    all_keys = []
    seen = set()
    for key in required_fields:
        if key not in seen:
            all_keys.append(key)
            seen.add(key)
    for lead in leads:
        for key in lead.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)
    headers = all_keys
    rows = [headers] + [[str(lead.get(h, "")) for h in headers] for lead in leads]

    chunk_size = 2000
    total_rows = len(rows)
    print(f"  📤 Uploading {total_rows} rows in chunks of {chunk_size}...")

    for i in range(0, total_rows, chunk_size):
        chunk = rows[i:i + chunk_size]
        range_name = f"'{sheet_name}'!A1"
        body = {"values": chunk}
        try:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
            print(f"    ✓ Appended rows {i + 1}-{i + len(chunk)}")
        except HttpError as e:
            print(f"    ❌ Error appending chunk {i}: {e}")
            break

    print(f"\n✅ Saved to Google Sheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def get_website_url(lead: Dict[str, Any]) -> Optional[str]:
    for field in ["website", "companyWebsite", "company_website", "domain", "companyDomain", "company_domain"]:
        url = str(lead.get(field, "")).strip()
        if url:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            return url
    return None


def get_company_name(lead: Dict[str, Any]) -> str:
    for field in ["name", "company", "company_name", "Company", "Business Name"]:
        value = str(lead.get(field, "")).strip()
        if value:
            return value
    return "the company"


def get_lead_key(lead: Dict[str, Any]) -> str:
    company_name = get_company_name(lead).strip().lower()
    website_url = get_website_url(lead) or ""
    domain = normalize_domain(website_url) or website_url.strip().lower()
    city = str(lead.get("city") or lead.get("City") or "").strip().lower()
    state = str(lead.get("state") or lead.get("State") or "").strip().lower()
    parts = [company_name, domain, city, state]
    return "|".join([p for p in parts if p])


def has_decision_maker(lead: Dict[str, Any]) -> bool:
    return bool(str(lead.get("decision_maker_name", "")).strip())


def was_processed(lead: Dict[str, Any]) -> bool:
    value = lead.get("decision_maker_checked", "")
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ["yes", "true", "1"]


def fetch_url(url: str, timeout: int = 20, verbose: bool = False) -> Optional[str]:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; DecisionMakerBot/1.0)"}
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code >= 400:
            if verbose:
                print(f"  ⚠ Fetch failed ({response.status_code}): {url}")
            return None
        if verbose:
            print(f"  ✓ Fetched: {url}")
        return response.text
    except requests.RequestException as e:
        if verbose:
            print(f"  ⚠ Fetch error: {url} ({e})")
        return None


class AnchorExtractor:
    def __init__(self):
        self.links: List[Tuple[str, str]] = []

    def feed(self, html: str):
        for match in re.finditer(r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL):
            href = match.group(1).strip()
            text = re.sub(r"<[^>]+>", "", match.group(2))
            text = unescape(text).strip()
            if href:
                self.links.append((href, text))


def extract_links(html: str) -> List[Tuple[str, str]]:
    extractor = AnchorExtractor()
    extractor.feed(html)
    return extractor.links


def clean_text(html: str) -> str:
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = unescape(html)
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def find_candidate_pages(base_url: str, html: str) -> Tuple[List[str], List[str]]:
    links = extract_links(html)
    about_keywords = ["about", "contact", "team", "leadership", "management", "owner", "founder"]
    legal_keywords = ["terms", "privacy", "legal", "imprint", "policy"]

    about_links = []
    legal_links = []
    base_domain = extract_domain(base_url)

    for href, text in links:
        if href.startswith(("mailto:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        link_domain = extract_domain(absolute)
        if base_domain and link_domain and link_domain != base_domain:
            continue
        combined = f"{href} {text}".lower()
        if any(k in combined for k in about_keywords):
            about_links.append(absolute)
        if any(k in combined for k in legal_keywords):
            legal_links.append(absolute)

    return list(dict.fromkeys(about_links)), list(dict.fromkeys(legal_links))


def openai_extract_decision_maker(text: str, company_name: str, source: str, verbose: bool = False) -> Optional[Dict[str, str]]:
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
                "content": "Extract the most likely decision maker (owner/founder/executive) from the text. Return JSON only.",
            },
            {
                "role": "user",
                "content": (
                    f"Company: {company_name}\n"
                    f"Source: {source}\n"
                    "Return JSON with keys: name, title, confidence (0-1), reason. "
                    "If no decision maker is found, return name as empty string."
                    "\n\nText:\n"
                    f"{text[:6000]}"
                ),
            },
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    try:
        with OPENAI_CONCURRENCY:
            for attempt in range(1, OPENAI_RETRY_MAX + 1):
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
                        print(f"  ⚠ OpenAI rate limited (attempt {attempt}/{OPENAI_RETRY_MAX})")
                    time.sleep(OPENAI_RETRY_BASE ** attempt)
                    continue
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                break
            else:
                if verbose:
                    print("  ⚠ OpenAI retries exhausted")
                return None
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            # Fallback: try to extract JSON object from text
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if not match:
                raise
            result = json.loads(match.group(0))
        name = str(result.get("name", "")).strip()
        if not name:
            if verbose:
                print(f"  ⚠ OpenAI no decision maker from {source}")
            return None
        return {
            "name": name,
            "title": str(result.get("title", "")).strip(),
            "confidence": str(result.get("confidence", "")),
            "reason": str(result.get("reason", "")).strip(),
        }
    except Exception as e:
        if verbose:
            print(f"  ⚠ OpenAI error ({source}): {e}")
        return None


def dataforseo_credentials() -> Tuple[str, str]:
    username = os.getenv("DATAFORSEO_USERNAME") or os.getenv("DATAFORSEO_USER")
    password = os.getenv("DATAFORSEO_PASSWORD") or os.getenv("DATA_FOR_SEO_API_KEY") or os.getenv("DATAFORSEO_API_KEY")

    if not username:
        # Fallback to provided username from directive context
        username = "manol@flowcraftpro.com"
    if not password:
        print("❌ Error: DataForSEO credentials not found in .env")
        sys.exit(1)
    return username, password


def _poll_dataforseo_task(task_id: str, username: str, password: str, verbose: bool) -> List[Dict[str, Any]]:
    for attempt in range(1, 11):
        if verbose:
            print(f"  ⏳ DataForSEO polling ({attempt}/10): {task_id}")
        time.sleep(2)
        get_resp = requests.get(
            f"https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced/{task_id}",
            auth=(username, password),
            timeout=60,
        )
        get_resp.raise_for_status()
        get_data = get_resp.json()
        get_tasks = get_data.get("tasks", [])
        if not get_tasks:
            continue
        result = get_tasks[0].get("result", [])
        if not result:
            continue
        items = result[0].get("items", [])
        if verbose:
            print(f"  ✓ DataForSEO results: {len(items) if isinstance(items, list) else 0}")
        return items if isinstance(items, list) else []
    if verbose:
        print("  ⚠ DataForSEO task not ready after polling")
    return []


def dataforseo_google_search(
    query: str,
    depth: int = 10,
    verbose: bool = False,
    executor: Optional[ThreadPoolExecutor] = None,
) -> List[Dict[str, Any]]:
    username, password = dataforseo_credentials()
    payload = [
        {
            "keyword": query,
            "location_name": "United States",
            "language_name": "English",
            "device": "desktop",
            "depth": depth,
        }
    ]

    try:
        post_resp = requests.post(
            "https://api.dataforseo.com/v3/serp/google/organic/task_post",
            auth=(username, password),
            json=payload,
            timeout=60,
        )
        post_resp.raise_for_status()
        post_data = post_resp.json()
        tasks = post_data.get("tasks", [])
        if not tasks:
            if verbose:
                print("  ⚠ DataForSEO no tasks returned")
            return []
        task_id = tasks[0].get("id")
        if not task_id:
            if verbose:
                print("  ⚠ DataForSEO missing task id")
            return []

        if verbose:
            print(f"  ⏳ DataForSEO task submitted: {task_id}")

        if executor:
            future = executor.submit(_poll_dataforseo_task, task_id, username, password, verbose)
            return future.result()
        return _poll_dataforseo_task(task_id, username, password, verbose)
    except Exception as e:
        if verbose:
            print(f"  ⚠ DataForSEO error: {e}")
        return []


class DataForSEORequest:
    def __init__(self, query: str, depth: int):
        self.query = query
        self.depth = depth
        self.event = threading.Event()
        self.result: List[Dict[str, Any]] = []


class DataForSEOBatcher:
    def __init__(
        self,
        username: str,
        password: str,
        poll_executor: ThreadPoolExecutor,
        verbose: bool = False,
        max_batch: int = 20,
    ):
        self.username = username
        self.password = password
        self.poll_executor = poll_executor
        self.verbose = verbose
        self.max_batch = max_batch
        self._queue: List[DataForSEORequest] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop = False
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def submit(self, query: str, depth: int = 10) -> List[Dict[str, Any]]:
        request = DataForSEORequest(query, depth)
        with self._condition:
            self._queue.append(request)
            self._condition.notify()
        request.event.wait()
        return request.result

    def close(self):
        with self._condition:
            self._stop = True
            self._condition.notify_all()
        self._thread.join(timeout=5)

    def _worker(self):
        while True:
            with self._condition:
                while not self._queue and not self._stop:
                    self._condition.wait(timeout=0.5)
                if self._stop and not self._queue:
                    return
                batch = self._queue[: self.max_batch]
                self._queue = self._queue[self.max_batch :]
            if batch:
                self._submit_batch(batch)

    def _submit_batch(self, batch: List[DataForSEORequest]):
        payload = [
            {
                "keyword": req.query,
                "location_name": "United States",
                "language_name": "English",
                "device": "desktop",
                "depth": req.depth,
            }
            for req in batch
        ]
        try:
            post_resp = requests.post(
                "https://api.dataforseo.com/v3/serp/google/organic/task_post",
                auth=(self.username, self.password),
                json=payload,
                timeout=60,
            )
            post_resp.raise_for_status()
            post_data = post_resp.json()
            tasks = post_data.get("tasks", [])
            if self.verbose:
                print(f"  ⏳ DataForSEO batch submitted: {len(tasks)} tasks")

            task_map: Dict[str, str] = {}
            for task in tasks:
                task_id = task.get("id")
                data = task.get("data") or {}
                keyword = str(data.get("keyword") or "")
                if task_id and keyword:
                    task_map[keyword] = task_id

            for req in batch:
                task_id = task_map.get(req.query)
                if not task_id:
                    if self.verbose:
                        print("  ⚠ DataForSEO missing task id for query")
                    req.result = []
                    req.event.set()
                    continue

                if self.verbose:
                    print(f"  ⏳ DataForSEO task submitted: {task_id}")

                future = self.poll_executor.submit(
                    _poll_dataforseo_task,
                    task_id,
                    self.username,
                    self.password,
                    self.verbose,
                )

                def _on_done(fut, target=req):
                    try:
                        target.result = fut.result()
                    except Exception:
                        target.result = []
                    target.event.set()

                future.add_done_callback(_on_done)
        except Exception as e:
            if self.verbose:
                print(f"  ⚠ DataForSEO batch error: {e}")
            for req in batch:
                req.result = []
                req.event.set()


def find_linkedin_company_url(
    company_name: str,
    domain: Optional[str],
    verbose: bool = False,
    executor: Optional[ThreadPoolExecutor] = None,
    batcher: Optional[DataForSEOBatcher] = None,
) -> Optional[str]:
    query = f"site:linkedin.com/company {company_name}"
    if domain:
        query += f" {domain}"
    if verbose:
        print(f"  🔎 DataForSEO company search: {query}")
    if batcher:
        items = batcher.submit(query, depth=10)
        if verbose:
            print(f"  ✓ DataForSEO results: {len(items) if isinstance(items, list) else 0}")
    else:
        items = dataforseo_google_search(query, depth=10, verbose=verbose, executor=executor)
    if verbose:
        urls = [str(item.get("url", "")).strip() for item in items if item.get("url")]
        linkedin_urls = [u for u in urls if "linkedin.com/company" in u]
        print(f"  🧪 QA DataForSEO: {len(items)} items, {len(linkedin_urls)} LinkedIn company urls")
        for preview in urls[:5]:
            print(f"    - {preview}")
    for item in items:
        url = item.get("url", "")
        if "linkedin.com/company" in url:
            return url
    return None


def parse_search_result_candidate(item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    title = str(item.get("title", "")).strip()
    url = str(item.get("url", "")).strip()
    if "linkedin.com/in" not in url:
        return None
    parts = [p.strip() for p in title.split(" - ") if p.strip()]
    if len(parts) < 2:
        return None
    name = parts[0]
    role = parts[1]
    return {"name": name, "title": role, "linkedin_url": url}


def is_decision_title(title: str) -> bool:
    lowered = title.lower()
    return any(keyword in lowered for keyword in DECISION_TITLES)


def rapidapi_key() -> str:
    api_key = (
        os.getenv("RAPIDAPI_LI_DATA_SCRAPER_KEY")
        or os.getenv("LI_DATA_SCRAPER_API_KEY")
        or os.getenv("RAPIDAPI_KEY")
    )
    if not api_key:
        print("❌ Error: RapidAPI key not found (set RAPIDAPI_LI_DATA_SCRAPER_KEY)")
        sys.exit(1)
    return api_key.strip()


def rapidapi_search_people(keywords: str, start: int = 0, verbose: bool = False) -> List[Dict[str, Any]]:
    api_key = rapidapi_key()
    url = "https://li-data-scraper.p.rapidapi.com/search-people"
    headers = {
        "x-rapidapi-host": "li-data-scraper.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }
    params = {"keywords": keywords, "start": start}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=45)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            if verbose:
                print(f"  ✓ RapidAPI results: {len(data)}")
            return data
        if isinstance(data, dict):
            for key in ["data", "items", "results"]:
                items = data.get(key)
                if isinstance(items, list):
                    if verbose:
                        print(f"  ✓ RapidAPI results: {len(items)}")
                    return items
        return []
    except Exception as e:
        if verbose:
            print(f"  ⚠ RapidAPI error: {e}")
        return []


def parse_rapidapi_person(item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    name = str(item.get("name") or item.get("fullName") or item.get("full_name") or "").strip()
    title = str(item.get("title") or item.get("headline") or item.get("position") or item.get("jobTitle") or "").strip()
    linkedin_url = str(
        item.get("profileUrl")
        or item.get("linkedinUrl")
        or item.get("profile_url")
        or item.get("url")
        or ""
    ).strip()

    if not name and not title:
        return None

    return {
        "name": name,
        "title": title,
        "linkedin_url": linkedin_url,
    }


def find_linkedin_employees(company_name: str, verbose: bool = False) -> List[Dict[str, str]]:
    keywords = " OR ".join([f'"{k}"' for k in DECISION_TITLES])
    query = f"{company_name} {keywords}"
    if verbose:
        print(f"  🔎 RapidAPI people search: {query}")
    items = rapidapi_search_people(query, start=0, verbose=verbose)
    candidates = []
    for item in items:
        candidate = parse_rapidapi_person(item)
        if not candidate:
            continue
        if candidate["title"] and is_decision_title(candidate["title"]):
            candidates.append(candidate)
    if verbose:
        print(f"  ✓ Decision-maker candidates: {len(candidates)}")
    return candidates


def openai_rank_candidates(candidates: List[Dict[str, str]], company_name: str, verbose: bool = False) -> Optional[Dict[str, str]]:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

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
                "content": "Pick the best decision maker for the company from the list. Return JSON only.",
            },
            {
                "role": "user",
                "content": json.dumps({
                    "company": company_name,
                    "candidates": candidates,
                    "instruction": "Choose the most senior decision maker. Return JSON with name, title, linkedin_url."
                }),
            },
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    try:
        with OPENAI_CONCURRENCY:
            for attempt in range(1, OPENAI_RETRY_MAX + 1):
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
                        print(f"  ⚠ OpenAI rate limited (attempt {attempt}/{OPENAI_RETRY_MAX})")
                    time.sleep(OPENAI_RETRY_BASE ** attempt)
                    continue
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                break
            else:
                if verbose:
                    print("  ⚠ OpenAI retries exhausted")
                return None
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if not match:
                raise
            result = json.loads(match.group(0))
        name = str(result.get("name", "")).strip()
        if not name:
            if verbose:
                print("  ⚠ OpenAI no best candidate")
            return None
        return {
            "name": name,
            "title": str(result.get("title", "")).strip(),
            "linkedin_url": str(result.get("linkedin_url", "")).strip(),
        }
    except Exception as e:
        if verbose:
            print(f"  ⚠ OpenAI rank error: {e}")
        return None


def extract_domain(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return None


def normalize_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    domain = extract_domain(url)
    if not domain:
        return None
    return domain.lower()


def enrich_lead(
    lead: Dict[str, Any],
    verbose: bool = False,
    executor: Optional[ThreadPoolExecutor] = None,
    dataforseo_batcher: Optional[DataForSEOBatcher] = None,
    use_dataforseo: bool = True,
) -> Dict[str, Any]:
    website_url = get_website_url(lead)
    company_name = get_company_name(lead)
    domain = extract_domain(website_url) if website_url else None

    if verbose:
        print(f"\n🏢 {company_name}")
        if website_url:
            print(f"  🌐 Website: {website_url}")
        else:
            print("  🌐 Website: (missing)")

    if website_url:
        homepage = fetch_url(website_url, verbose=verbose)
        if homepage:
            about_links, legal_links = find_candidate_pages(website_url, homepage)
            if verbose:
                print(f"  🔗 About/Contact pages: {len(about_links)}")
                for link in about_links[:3]:
                    print(f"    - {link}")
                print(f"  🔗 Legal/Terms pages: {len(legal_links)}")
                for link in legal_links[:2]:
                    print(f"    - {link}")

            for link in about_links[:3]:
                page_html = fetch_url(link, verbose=verbose)
                if not page_html:
                    continue
                text = clean_text(page_html)
                decision = openai_extract_decision_maker(text, company_name, "about/contact", verbose=verbose)
                if decision:
                    print(f"✅ Decision maker: {decision['name']} — {decision.get('title', '')}")
                    lead["decision_maker_name"] = decision["name"]
                    lead["decision_maker_title"] = decision["title"]
                    lead["decision_maker_source"] = "about_page"
                    lead["decision_maker_confidence"] = decision["confidence"] or "0.8"
                    lead["decision_maker_linkedin"] = ""
                    return lead

            for link in legal_links[:2]:
                page_html = fetch_url(link, verbose=verbose)
                if not page_html:
                    continue
                text = clean_text(page_html)
                decision = openai_extract_decision_maker(text, company_name, "terms/legal", verbose=verbose)
                if decision:
                    print(f"✅ Decision maker: {decision['name']} — {decision.get('title', '')}")
                    lead["decision_maker_name"] = decision["name"]
                    lead["decision_maker_title"] = decision["title"]
                    lead["decision_maker_source"] = "terms_page"
                    lead["decision_maker_confidence"] = decision["confidence"] or "0.6"
                    lead["decision_maker_linkedin"] = ""
                    return lead

    if use_dataforseo:
        company_linkedin = find_linkedin_company_url(
            company_name,
            domain,
            verbose=verbose,
            executor=executor,
            batcher=dataforseo_batcher,
        )
        if company_linkedin:
            print(f"🔗 LinkedIn company: {company_linkedin}")
            lead["company_linkedin"] = company_linkedin
            if verbose:
                print("  ⏭️ LinkedIn employee search disabled")
        else:
            if verbose:
                print("  ⚠ No LinkedIn company found")
            lead.setdefault("company_linkedin", "")
    else:
        lead.setdefault("company_linkedin", "")
        if verbose:
            print("  ⏭️ DataForSEO company search skipped")

    if verbose:
        print("  ⚠ No decision maker found")
    return lead


def main():
    parser = argparse.ArgumentParser(description="Find decision makers using waterfall enrichment")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--source-file", help="Path to CSV or JSON file")
    input_group.add_argument("--source-url", help="Google Sheets URL")

    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument("--output", help="Output JSON file path")
    output_group.add_argument("--output-sheet", help="Output Google Sheet name")

    parser.add_argument("--sheet-name", help="Source sheet name (for Google Sheets input)")
    parser.add_argument("--max-leads", type=int, default=100, help="Maximum number of leads to process")
    parser.add_argument("--skip-first", type=int, default=0, help="Skip the first N leads from the source")
    parser.add_argument("--workers", type=int, default=25, help="Concurrent lead workers (default: 25)")
    parser.add_argument("--include-existing", action="store_true", help="Process leads with existing decision maker")
    parser.add_argument("--folder-id", help="Google Drive folder ID for output (optional)")
    parser.add_argument("--skip-dataforseo", action="store_true", help="Skip DataForSEO Google search")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    print("\n👤 Find Decision Makers")
    print("=" * 50)

    leads: Optional[List[Dict[str, Any]]] = None
    resumed_from_checkpoint = False
    legacy_checkpoint = False
    processed_row_ids: set = set()
    processed_keys: set = set()
    output_sheet_id: Optional[str] = None
    headers: Optional[List[str]] = None
    checkpoint_skip_first: Optional[int] = None
    ckpt = load_checkpoint()
    if ckpt:
        if "legacy_leads" in ckpt:
            legacy_leads = ckpt["legacy_leads"]
            print(f"\n⚠️  Found an interrupted run ({len(legacy_leads)} leads).")
            resume = "yes" if args.yes else input("Resume from checkpoint? (yes/no): ").strip().lower()
            if resume == "yes":
                leads = legacy_leads
                resumed_from_checkpoint = True
                legacy_checkpoint = True
                print("📂 Loaded leads from checkpoint (legacy)")
        else:
            total = ckpt.get("total_leads")
            total_display = str(total) if total is not None else "unknown"
            print(f"\n⚠️  Found an interrupted run ({total_display} leads).")
            resume = "yes" if args.yes else input("Resume from checkpoint? (yes/no): ").strip().lower()
            if resume == "yes":
                resumed_from_checkpoint = True
                output_sheet_id = ckpt.get("output_sheet_id")
                headers = ckpt.get("headers")
                checkpoint_skip_first = ckpt.get("skip_first")
                processed_row_ids = set(ckpt.get("processed_row_ids", []))
                processed_keys = set(ckpt.get("processed_keys", []))
                print("📂 Loaded checkpoint metadata")

    if not leads:
        if args.source_file:
            print(f"\n📂 Loading from file: {args.source_file}")
            if args.source_file.endswith(".csv"):
                leads = load_from_csv(args.source_file)
            elif args.source_file.endswith(".json"):
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

    row_start = 1 if args.source_file else 2
    for idx, lead in enumerate(leads, start=row_start):
        lead["_row_id"] = idx

    if legacy_checkpoint:
        for lead in leads:
            if was_processed(lead):
                processed_row_ids.add(lead.get("_row_id"))
                key = get_lead_key(lead)
                if key:
                    processed_keys.add(key)
        headers = headers or build_headers(leads)

    skip_first = checkpoint_skip_first if checkpoint_skip_first is not None else args.skip_first

    original_total = len(leads)
    skipped_count = 0
    if not legacy_checkpoint and skip_first and skip_first > 0:
        skipped_count = min(skip_first, len(leads))
        leads = leads[skipped_count:]
    elif legacy_checkpoint and args.skip_first:
        print("ℹ️  Resuming from legacy checkpoint; --skip-first ignored.")

    def already_processed(lead: Dict[str, Any]) -> bool:
        row_id = lead.get("_row_id")
        if row_id in processed_row_ids:
            return True
        key = get_lead_key(lead)
        if key and key in processed_keys:
            return True
        return was_processed(lead)

    if args.include_existing:
        leads_to_process = leads
        print(f"\n📊 Summary:")
        print(f"   Total leads: {original_total}")
        if skipped_count:
            print(f"   Skipped first: {skipped_count}")
        print(f"   Leads considered: {len(leads)}")
        print(f"   Will process: {len(leads_to_process)} leads (including existing)")
    else:
        leads_to_process = [lead for lead in leads if not already_processed(lead)]
        print(f"\n📊 Summary:")
        print(f"   Total leads: {original_total}")
        if skipped_count:
            print(f"   Skipped first: {skipped_count}")
        print(f"   Leads considered: {len(leads)}")
        print(f"   Leads not yet processed: {len(leads_to_process)}")

    # Deduplicate by website domain for efficiency
    unique_by_domain: Dict[str, Dict[str, Any]] = {}
    domainless_leads: List[Dict[str, Any]] = []
    for lead in leads_to_process:
        website_url = get_website_url(lead)
        domain = normalize_domain(website_url)
        if not domain:
            domainless_leads.append(lead)
            continue
        if domain not in unique_by_domain:
            unique_by_domain[domain] = lead
    deduped_leads = list(unique_by_domain.values()) + domainless_leads
    if len(deduped_leads) != len(leads_to_process):
        print(f"   Deduped by domain: {len(leads_to_process)} -> {len(deduped_leads)} leads")
    leads_to_process = deduped_leads

    if len(leads_to_process) > args.max_leads:
        leads_to_process = leads_to_process[:args.max_leads]

    print(f"   Will process: {len(leads_to_process)} leads")
    print(f"   Max leads limit: {args.max_leads}")
    if args.skip_dataforseo:
        print("   Estimated cost: OpenAI + RapidAPI per lead (DataForSEO skipped)")
    else:
        print("   Estimated cost: OpenAI + RapidAPI + DataForSEO per lead (varies by pages found)")

    if args.yes:
        print("\n✅ Auto-confirmed with --yes flag")
    else:
        response = input("\n⚠️  Continue? (yes/no): ").strip().lower()
        if response != "yes":
            print("❌ Cancelled")
            sys.exit(0)

    def print_progress(current: int, total: int):
        if total <= 0:
            return
        bar_len = 30
        filled = int(bar_len * current / total)
        bar = "=" * filled + "-" * (bar_len - filled)
        sys.stdout.write(f"\rProgress: [{bar}] {current}/{total}")
        sys.stdout.flush()

    required_fields = [
        "decision_maker_name",
        "decision_maker_title",
        "decision_maker_source",
        "decision_maker_confidence",
        "decision_maker_linkedin",
        "company_linkedin",
        "decision_maker_checked",
    ]

    def ensure_required_fields(lead: Dict[str, Any]):
        for field in required_fields:
            lead.setdefault(field, "")

    headers = headers or build_headers(leads)

    output_sheet_created = False
    if args.output_sheet:
        folder_id = args.folder_id or os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        output_sheet_created = output_sheet_id is None
        output_sheet_id = ensure_output_sheet(args.output_sheet, folder_id, output_sheet_id)
        if output_sheet_created:
            append_rows_to_google_sheet(output_sheet_id, args.output_sheet, [headers])

        if legacy_checkpoint and output_sheet_created:
            legacy_processed = [lead for lead in leads if was_processed(lead)]
            if legacy_processed:
                print(f"\n💾 Backfilling {len(legacy_processed)} processed leads to output sheet...")
                chunk_size = 200
                for i in range(0, len(legacy_processed), chunk_size):
                    chunk = legacy_processed[i:i + chunk_size]
                    rows = []
                    for lead in chunk:
                        ensure_required_fields(lead)
                        rows.append([str(lead.get(h, "")) for h in headers])
                    append_rows_to_google_sheet(output_sheet_id, args.output_sheet, rows)

    def checkpoint_payload() -> Dict[str, Any]:
        return {
            "version": 2,
            "total_leads": original_total,
            "processed_row_ids": sorted([rid for rid in processed_row_ids if isinstance(rid, int)]),
            "processed_keys": sorted(processed_keys),
            "output_sheet_id": output_sheet_id,
            "output_sheet_name": args.output_sheet,
            "headers": headers,
            "source_url": args.source_url,
            "sheet_name": args.sheet_name,
            "skip_first": skip_first,
        }

    if output_sheet_id:
        save_checkpoint(checkpoint_payload())

    processed = 0
    processed_results: List[Dict[str, Any]] = []
    append_buffer: List[List[str]] = []
    append_batch_size = 50

    def flush_append_buffer():
        if args.output_sheet and output_sheet_id and append_buffer:
            append_rows_to_google_sheet(output_sheet_id, args.output_sheet, append_buffer)
            append_buffer.clear()

    with ThreadPoolExecutor(max_workers=args.workers) as lead_executor:
        if args.skip_dataforseo:
            batcher = None
            futures = {
                lead_executor.submit(
                    enrich_lead,
                    lead,
                    args.verbose,
                    lead_executor,
                    batcher,
                    False,
                ): lead
                for lead in leads_to_process
            }
            for future in as_completed(futures):
                lead = futures[future]
                try:
                    enriched = future.result()
                    lead.update(enriched)
                    lead["decision_maker_checked"] = True
                    ensure_required_fields(lead)
                    processed_row_ids.add(lead.get("_row_id"))
                    key = get_lead_key(lead)
                    if key:
                        processed_keys.add(key)
                    if args.output:
                        processed_results.append(lead)
                    if args.output_sheet and output_sheet_id:
                        append_buffer.append([str(lead.get(h, "")) for h in headers])
                        if len(append_buffer) >= append_batch_size:
                            flush_append_buffer()
                except Exception as e:
                    print(f"  ⚠ Lead processing error: {e}")
                processed += 1
                print_progress(processed, len(leads_to_process))
                if processed % 10 == 0:
                    save_checkpoint(checkpoint_payload())
                    print(f"  💾 Checkpoint updated ({processed}/{len(leads_to_process)})")
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as dseo_executor:
                username, password = dataforseo_credentials()
                batcher = DataForSEOBatcher(username, password, dseo_executor, verbose=args.verbose, max_batch=20)
                futures = {
                    lead_executor.submit(
                        enrich_lead,
                        lead,
                        args.verbose,
                        lead_executor,
                        batcher,
                        True,
                    ): lead
                    for lead in leads_to_process
                }
                for future in as_completed(futures):
                    lead = futures[future]
                    try:
                        enriched = future.result()
                        lead.update(enriched)
                        lead["decision_maker_checked"] = True
                        ensure_required_fields(lead)
                        processed_row_ids.add(lead.get("_row_id"))
                        key = get_lead_key(lead)
                        if key:
                            processed_keys.add(key)
                        if args.output:
                            processed_results.append(lead)
                        if args.output_sheet and output_sheet_id:
                            append_buffer.append([str(lead.get(h, "")) for h in headers])
                            if len(append_buffer) >= append_batch_size:
                                flush_append_buffer()
                    except Exception as e:
                        print(f"  ⚠ Lead processing error: {e}")
                    processed += 1
                    print_progress(processed, len(leads_to_process))
                    if processed % 10 == 0:
                        save_checkpoint(checkpoint_payload())
                        print(f"  💾 Checkpoint updated ({processed}/{len(leads_to_process)})")
                batcher.close()

    flush_append_buffer()

    print()
    print("\n💾 Saving results...")
    if args.output:
        save_to_json(processed_results, args.output)
        print(f"✅ Saved to: {args.output}")
    else:
        print(f"✅ Incremental results saved to: https://docs.google.com/spreadsheets/d/{output_sheet_id}")

    clear_checkpoint()
    print("\n✅ Decision maker enrichment complete.")


if __name__ == "__main__":
    main()
