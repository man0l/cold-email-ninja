#!/usr/bin/env python3
"""
Scrape Google Maps Leads

Scrapes business leads from Google Maps using the RapidAPI Maps Data API.
Supports multiple keywords, location-based searching, and exports to Google Sheets.

Usage:
    # Basic usage
    python execution/scrape_google_maps.py --keywords "Custom Home Builder" --output-sheet "Leads"

    # Multiple keywords
    python execution/scrape_google_maps.py --keywords "Plumber" "HVAC" --output-sheet "Home Services" --leads 500

    # Test run only
    python execution/scrape_google_maps.py --keywords "Custom Home Builder" --output-sheet "Test" --test-only
"""

import argparse
import csv
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv
import requests
from fix_location_columns import extract_country, parse_address_city_state_zip

# Load environment variables
load_dotenv()

# API Configuration
API_URL = "https://maps-data.p.rapidapi.com/searchmaps.php"
DEFAULT_LOCATIONS_FILE = "data/us_locations.csv"
DEFAULT_LEADS_LIMIT = 1000
RESULTS_PER_REQUEST = 20
CHECKPOINT_FILE = ".tmp/google_maps_checkpoint.json"
CHECKPOINT_INTERVAL = 50
DEFAULT_CONCURRENT_REQUESTS = 20


def load_locations(file_path: str) -> List[Dict[str, str]]:
    """Load and deduplicate locations from CSV file"""
    locations = []
    seen = set()

    if not os.path.exists(file_path):
        print(f"❌ Error: Locations file not found: {file_path}")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row.get('City', '').strip()
            state = row.get('State', '').strip()
            zip_code = row.get('Zip', '').strip()
            country = row.get('Country', 'USA').strip()

            if not city or not state:
                continue

            # Create unique key for deduplication
            key = f"{city}|{state}|{zip_code}".lower()
            if key in seen:
                continue

            seen.add(key)
            locations.append({
                'city': city,
                'state': state,
                'zip': zip_code,
                'country': country
            })

    return locations


def format_location_string(location: Dict[str, str]) -> str:
    """Format location as search string"""
    parts = [location['city'], location['state']]
    if location.get('zip'):
        parts.append(location['zip'])
    return ', '.join(parts)


def search_google_maps(query: str, api_key: str, limit: int = 20, offset: int = 0,
                       verbose: bool = False) -> List[Dict[str, Any]]:
    """Search Google Maps API and return results"""
    headers = {
        'x-rapidapi-host': 'maps-data.p.rapidapi.com',
        'x-rapidapi-key': api_key
    }

    params = {
        'query': query,
        'limit': limit,
        'country': 'us',
        'lang': 'en',
        'offset': offset,
        'zoom': 13
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if verbose:
            print(f"  API response status: {data.get('status', 'unknown')}")

        results = data.get('data', [])
        if isinstance(results, list):
            return results
        return []

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ API error: {e}")
        return []
    except json.JSONDecodeError:
        print("  ⚠️ Invalid JSON response from API")
        return []


def parse_lead(result: Dict[str, Any], keyword: str, location: str) -> Dict[str, Any]:
    """Parse API result into standardized lead format"""
    # Parse address components with robust parsing (shared with fix_location_columns)
    full_address = result.get('full_address', '') or ''
    country, address_without_country = extract_country(full_address)
    address_without_country = address_without_country or full_address

    city, state, zip_code = '', '', ''
    parsed_city, parsed_state, parsed_zip = parse_address_city_state_zip(address_without_country)
    if parsed_city:
        city = parsed_city
    if parsed_state:
        state = parsed_state
    if parsed_zip:
        zip_code = parsed_zip

    parts = [part.strip() for part in address_without_country.split(",") if part.strip()]
    street = ''
    if len(parts) >= 3:
        street = ", ".join(parts[:-2])
    elif len(parts) == 2:
        if state or zip_code:
            street = ''
            if not city:
                city = parts[0]
        else:
            street = parts[0]
            if not city:
                city = parts[1]
    elif len(parts) == 1:
        street = parts[0]

    country = country or 'USA'

    name = result.get('name', '')
    if street and name:
        name_lower = name.strip().lower()
        street_strip = street.strip()
        if name_lower and street_strip.lower().startswith(name_lower):
            remainder = street_strip[len(name_lower):].lstrip(" ,")
            street = remainder or street_strip

    # Extract category from 'types' array (API returns types, not category)
    types = result.get('types', [])
    category = types[0] if types else ''

    return {
        'name': name,
        'address': street,
        'city': city,
        'state': state,
        'zip': zip_code,
        'country': country,
        'phone': result.get('phone_number', ''),
        'website': result.get('website', ''),
        'rating': result.get('rating', ''),
        'reviews': result.get('review_count', ''),
        'category': category,
        'place_id': result.get('place_id', ''),
        'latitude': result.get('latitude', ''),
        'longitude': result.get('longitude', ''),
        'search_keyword': keyword,
        'search_location': location
    }


def save_checkpoint(leads: List[Dict[str, Any]], processed_locations: Set[str],
                    current_keyword: str, current_location_idx: int):
    """Save progress checkpoint"""
    os.makedirs('.tmp', exist_ok=True)
    checkpoint = {
        'leads': leads,
        'processed_locations': list(processed_locations),
        'current_keyword': current_keyword,
        'current_location_idx': current_location_idx
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f)


def load_checkpoint() -> Optional[Dict[str, Any]]:
    """Load checkpoint if exists"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data['processed_locations'] = set(data.get('processed_locations', []))
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return None


def delete_checkpoint():
    """Delete checkpoint file"""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


def scrape_location(keyword: str, location: Dict[str, str], api_key: str,
                    verbose: bool = False, max_pages: int = 10) -> Tuple[str, List[Dict[str, Any]], str]:
    """Scrape a single location and return (location_key, leads, location_str)

    Fetches ALL available leads from the location by paginating until no more results.
    max_pages limits pagination to avoid infinite loops (default: 10 pages = 200 results max)
    """
    location_str = format_location_string(location)
    location_key = f"{keyword}|{location_str}"
    query = f"{keyword} in {location_str}"

    leads = []
    offset = 0
    pages = 0

    while pages < max_pages:
        results = search_google_maps(query, api_key, limit=RESULTS_PER_REQUEST,
                                     offset=offset, verbose=verbose)
        if not results:
            break

        for result in results:
            lead = parse_lead(result, keyword, location_str)
            leads.append(lead)

        # If we got fewer results than requested, no more pages
        if len(results) < RESULTS_PER_REQUEST:
            break

        offset += RESULTS_PER_REQUEST
        pages += 1

    return location_key, leads, location_str


def run_qa_test(keywords: List[str], locations: List[Dict[str, str]],
                api_key: str, verbose: bool = False) -> bool:
    """Run QA test with 20 leads from first location for each keyword"""
    print("\n🧪 Running QA Test")
    print("=" * 50)

    if not locations:
        print("❌ No locations available for testing")
        return False

    test_location = locations[0]
    location_str = format_location_string(test_location)

    for keyword in keywords:
        query = f"{keyword} in {location_str}"
        print(f"\n📍 Testing: \"{keyword}\" in {location_str}")
        print("-" * 40)

        results = search_google_maps(query, api_key, limit=20, verbose=verbose)

        if not results:
            print(f"  ⚠️ No results found for this query")
            print(f"  💡 Consider adjusting the keyword")
            continue

        print(f"  ✅ Found {len(results)} results")
        print(f"\n  Sample businesses:")

        # Show first 5 results
        phones_count = 0
        websites_count = 0
        categories = set()

        for i, result in enumerate(results[:5]):
            name = result.get('name', 'Unknown')
            phone = result.get('phone_number', '')
            website = result.get('website', '')
            types = result.get('types', [])
            category = types[0] if types else ''

            phone_display = phone if phone else '(no phone)'
            website_display = website[:40] + '...' if website and len(website) > 40 else (website or '(no website)')

            print(f"    {i+1}. {name[:35]}")
            print(f"       📞 {phone_display}")
            print(f"       🌐 {website_display}")
            if category:
                print(f"       📂 {category}")

        # Calculate data completeness
        for result in results:
            if result.get('phone_number'):
                phones_count += 1
            if result.get('website'):
                websites_count += 1
            types = result.get('types', [])
            if types:
                categories.add(types[0])

        print(f"\n  📊 Data Completeness:")
        print(f"     - Phone numbers: {phones_count}/{len(results)} ({100*phones_count//len(results)}%)")
        print(f"     - Websites: {websites_count}/{len(results)} ({100*websites_count//len(results)}%)")
        print(f"\n  📂 Categories found:")
        for cat in list(categories)[:5]:
            print(f"     - {cat}")

    print("\n" + "=" * 50)
    response = input("\n✅ Results look good? Proceed with full scrape? (yes/no): ").strip().lower()
    return response in ['yes', 'y']


def main():
    parser = argparse.ArgumentParser(description='Scrape Google Maps leads')
    parser.add_argument('--keywords', nargs='+', required=True,
                        help='Search keywords (e.g., "Custom Home Builder")')
    parser.add_argument('--locations', default=DEFAULT_LOCATIONS_FILE,
                        help='Path to locations CSV file')
    parser.add_argument('--output-sheet', required=True,
                        help='Name for the output (used in export command suggestion)')
    parser.add_argument('--leads', type=int, default=DEFAULT_LEADS_LIMIT,
                        help='Total number of leads to scrape')
    parser.add_argument('--test-only', action='store_true',
                        help='Run only QA test (20 leads from first location)')
    parser.add_argument('--skip-test', action='store_true',
                        help='Skip QA test and go directly to full scrape')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show detailed progress')
    parser.add_argument('--yes', '-y', action='store_true',
                        help='Auto-confirm all prompts (non-interactive mode)')
    parser.add_argument('--concurrent', '-c', type=int, default=DEFAULT_CONCURRENT_REQUESTS,
                        help=f'Number of concurrent requests (default: {DEFAULT_CONCURRENT_REQUESTS})')

    args = parser.parse_args()

    # Get API key
    api_key = os.getenv('RAPIDAPI_MAPS_DATA_API_KEY')
    if not api_key:
        print("❌ Error: RAPIDAPI_MAPS_DATA_API_KEY not found in .env file")
        sys.exit(1)

    print("🗺️  Google Maps Lead Scraper")
    print("=" * 50)
    print(f"Keywords: {', '.join(args.keywords)}")
    print(f"Locations file: {args.locations}")
    print(f"Target leads: {args.leads}")
    print(f"Output: {args.output_sheet}")
    print("=" * 50)

    # Load locations
    print("\n📍 Loading locations...")
    locations = load_locations(args.locations)
    print(f"   Found {len(locations)} unique locations")

    if not locations:
        print("❌ No valid locations found")
        sys.exit(1)

    # QA Test
    if args.test_only:
        run_qa_test(args.keywords, locations, api_key, args.verbose)
        print("\n✅ Test complete. Run without --test-only for full scrape.")
        return

    if not args.skip_test:
        if not run_qa_test(args.keywords, locations, api_key, args.verbose):
            print("\n❌ QA test not approved. Exiting.")
            sys.exit(0)

    print(f"\n📊 Scrape Plan:")
    print(f"   Locations: {len(locations)}")
    print(f"   Keywords: {len(args.keywords)}")
    print(f"   Leads per location: ALL (paginated)")
    print(f"   Concurrent requests: {args.concurrent}")
    print(f"   Target leads: {args.leads}")

    if args.yes:
        print("\n✅ Auto-confirming scrape (--yes flag)")
    else:
        response = input("\n⚠️  Proceed with scraping? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            print("❌ Scraping cancelled.")
            sys.exit(0)

    # Check for checkpoint
    checkpoint = load_checkpoint()
    all_leads = []
    seen_place_ids: Set[str] = set()
    processed_locations: Set[str] = set()
    start_keyword_idx = 0
    start_location_idx = 0

    if checkpoint:
        print("\n🔄 Resuming from checkpoint...")
        all_leads = checkpoint.get('leads', [])
        processed_locations = checkpoint.get('processed_locations', set())
        for lead in all_leads:
            if lead.get('place_id'):
                seen_place_ids.add(lead['place_id'])
        print(f"   Loaded {len(all_leads)} leads from checkpoint")

    # Main scraping loop with concurrent requests
    print("\n🚀 Starting scrape...")
    total_processed = len(all_leads)

    # Thread-safe lock for updating shared state
    lock = threading.Lock()

    def process_result(future, loc_idx, total_locations):
        """Process result from a completed future"""
        nonlocal total_processed, all_leads, seen_place_ids, processed_locations

        try:
            location_key, leads, location_str = future.result()

            with lock:
                # Skip if already processed (from checkpoint)
                if location_key in processed_locations:
                    return

                # Add leads, deduplicating by place_id
                new_leads = 0
                for lead in leads:
                    place_id = lead.get('place_id', '')
                    if place_id and place_id in seen_place_ids:
                        continue
                    if place_id:
                        seen_place_ids.add(place_id)
                    all_leads.append(lead)
                    new_leads += 1
                    total_processed += 1

                processed_locations.add(location_key)

                # Progress update
                if len(processed_locations) % 10 == 0:
                    print(f"  [{len(processed_locations)}/{total_locations}] {location_str} - {total_processed}/{args.leads} leads ({100*total_processed//args.leads}%)")

                # Checkpoint
                if total_processed % CHECKPOINT_INTERVAL == 0:
                    save_checkpoint(all_leads, processed_locations, args.keywords[0], loc_idx)
                    print(f"  💾 Checkpoint: {total_processed} leads saved")

        except Exception as e:
            print(f"  ⚠️ Error processing location: {e}")

    try:
        for keyword in args.keywords:
            print(f"\n🔍 Keyword: {keyword}")

            # Filter out already processed locations
            locations_to_scrape = [
                (idx, loc) for idx, loc in enumerate(locations)
                if f"{keyword}|{format_location_string(loc)}" not in processed_locations
            ]

            if not locations_to_scrape:
                print("  All locations already processed")
                continue

            print(f"  Scraping {len(locations_to_scrape)} locations with {args.concurrent} concurrent requests...")

            # Use ThreadPoolExecutor for concurrent requests
            with ThreadPoolExecutor(max_workers=args.concurrent) as executor:
                futures = {}

                for loc_idx, location in locations_to_scrape:
                    if total_processed >= args.leads:
                        break

                    future = executor.submit(
                        scrape_location,
                        keyword, location, api_key, args.verbose
                    )
                    futures[future] = (loc_idx, len(locations))

                # Process results as they complete
                for future in as_completed(futures):
                    if total_processed >= args.leads:
                        print(f"\n✅ Reached target of {args.leads} leads")
                        # Cancel remaining futures
                        for f in futures:
                            f.cancel()
                        break

                    loc_idx, total_locations = futures[future]
                    process_result(future, loc_idx, total_locations)

            if total_processed >= args.leads:
                break

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted! Saving checkpoint...")
        save_checkpoint(all_leads, processed_locations, keyword, 0)
        print(f"   Saved {len(all_leads)} leads. Resume by running the same command.")
        sys.exit(0)

    # Final save
    save_checkpoint(all_leads, processed_locations, args.keywords[0], 0)

    # Summary
    print("\n" + "=" * 50)
    print("✅ Scrape Complete!")
    print("=" * 50)
    print(f"   Total leads: {len(all_leads)}")
    print(f"   Unique place IDs: {len(seen_place_ids)}")
    print(f"   Locations processed: {len(processed_locations)}")
    print(f"\n📁 Output: {CHECKPOINT_FILE}")
    print(f"\n💡 To export to Google Sheets, run:")
    print(f"   python execution/export_to_sheets.py --input {CHECKPOINT_FILE} --output-sheet \"{args.output_sheet}\"")


if __name__ == '__main__':
    main()
