# Scrape Google Maps Leads SOP

## Goal
Scrape business leads from Google Maps using the RapidAPI Maps Data API. Saves leads to a checkpoint file, then exports to Google Sheets using a separate tool.

## Inputs
- **Keywords**: One or more search terms (e.g., "Custom Home Builder", "Plumber")
- **Locations CSV**: Path to a CSV file containing locations to search (default: `data/us_locations.csv`)
- **Target Leads**: Maximum number of leads to scrape (default: 1000, use high number like 999999 to get all)
- **Output Sheet Name**: Name for the Google Sheet (used in export step)
- **Concurrent Requests**: Number of parallel API requests (default: 20)
- **API Key**: RapidAPI Maps Data API key (from `.env` as `RAPIDAPI_MAPS_DATA_API_KEY`)

## Algorithm

### Phase 1: Location Preparation
1. **Load Locations**: Read the locations CSV file.
2. **Deduplicate**: Remove duplicate locations based on City + State + Zip combination.
3. **Parse Locations**: Extract city, state, zip, and country for each unique location.

### Phase 2: Quality Assurance Test Run (Optional)
1. **Test Scrape**: For each keyword, run a test scrape of **20 leads** from the first location.
2. **Review Results**: Display sample results for validation.
3. **Confirm**: Ask user to confirm keywords are returning relevant results.
4. **Skip**: Use `--skip-test` to bypass this step for automation.

### Phase 3: Full Scrape (Concurrent)
1. **Scrape All Locations**: Using ThreadPoolExecutor with configurable concurrency:
   - Query the API with `<keyword> in <City>, <State> <Zip>`
   - Collect **ALL available leads** per location (paginated, up to 200 per location)
   - 20 concurrent requests by default for fast scraping
2. **Deduplicate Results**: Remove duplicate businesses (by place_id).
3. **Checkpoints**: Save progress every 50 leads to `.tmp/google_maps_checkpoint.json`.
4. **Resume Support**: Script automatically resumes from checkpoint if interrupted.

### Phase 4: Export to Google Sheets
1. **Use export_to_sheets.py**: Run the export tool with the checkpoint file.
2. **Creates Google Sheet**: In the shared Drive folder specified by `GOOGLE_DRIVE_FOLDER_ID`.

## Tools
- `execution/scrape_google_maps.py` - Scrapes leads and saves to checkpoint file
- `execution/export_to_sheets.py` - Exports checkpoint data to Google Sheets

## Workflow

> [!IMPORTANT]
> **ALWAYS export after scraping.** When executing this directive, run both steps automatically without waiting for user confirmation. The scrape is not complete until the data is in Google Sheets.

### Step 1: Scrape Leads

```bash
source .venv/bin/activate
python execution/scrape_google_maps.py \
  --keywords "Garage Door" \
  --output-sheet "Garage Doors - Google Maps" \
  --leads 999999 \
  --skip-test --yes
```

Output: `.tmp/google_maps_checkpoint.json`

### Step 2: Export to Google Sheets (ALWAYS RUN)

```bash
source .venv/bin/activate
python execution/export_to_sheets.py \
  --input .tmp/google_maps_checkpoint.json \
  --output-sheet "Garage Doors - Google Maps"
```

Output: Google Sheet URL

### Step 3: Clean Up (ALWAYS RUN after successful export)

```bash
rm .tmp/google_maps_checkpoint.json
```

**Return the Google Sheet URL to the user when complete.**

## Output
A Google Sheet containing:
| Column | Description |
|--------|-------------|
| name | Business name |
| address | Full street address |
| city | City |
| state | State |
| zip | Postal code |
| country | Country |
| phone | Phone number |
| website | Website URL |
| rating | Google rating (1-5) |
| reviews | Number of reviews |
| category | Business category |
| place_id | Google Place ID (for deduplication) |
| latitude | Latitude |
| longitude | Longitude |
| search_keyword | Keyword used to find this lead |
| search_location | Location searched |

## Safety Rules

> [!IMPORTANT]
> **API COST AWARENESS**
> - The RapidAPI Maps Data API charges per request
> - Each API call returns up to 20 results
> - Test runs use minimal API calls (1 per keyword)
> - Concurrent requests are fast but use more API calls

> [!WARNING]
> **RATE LIMITS**
> - Respect API rate limits (check RapidAPI dashboard)
> - Reduce `--concurrent` if hitting rate limits
> - Default: 20 concurrent requests

## Scraper Options Reference

```bash
python execution/scrape_google_maps.py \
  --keywords "Keyword1" "Keyword2" \
  --locations "data/us_locations.csv" \
  --output-sheet "Sheet Name" \
  --leads 10000 \
  --concurrent 20 \
  --skip-test --yes --verbose
```

- `--keywords`: One or more search keywords (required)
- `--locations`: Path to locations CSV (default: `data/us_locations.csv`)
- `--output-sheet`: Name for the sheet (required, used in export suggestion)
- `--leads`: Target leads to scrape (default: 1000)
- `--concurrent`, `-c`: Number of concurrent requests (default: 20)
- `--test-only`: Run only the QA test (20 leads from first location)
- `--skip-test`: Skip QA test and go directly to full scrape
- `--yes`, `-y`: Auto-confirm all prompts (non-interactive mode)
- `--verbose`, `-v`: Show detailed progress and API responses

## Export Options Reference

```bash
python execution/export_to_sheets.py \
  --input .tmp/google_maps_checkpoint.json \
  --output-sheet "My Leads" \
  --folder-id "optional_folder_id"
```

- `--input`, `-i`: Path to JSON file (checkpoint or array format) (required)
- `--output-sheet`, `-o`: Name for the Google Sheet (required)
- `--folder-id`, `-f`: Google Drive folder ID (default: from `GOOGLE_DRIVE_FOLDER_ID` env var)
- `--no-folder`: Create in Drive root instead of default folder

## Complete Workflow Example

```bash
source .venv/bin/activate

# Optional: Create sample locations (1/5 of full dataset)
head -1 data/us_locations.csv > .tmp/us_locations_sample.csv
awk 'NR > 1 && (NR - 1) % 5 == 0' data/us_locations.csv >> .tmp/us_locations_sample.csv

# Step 1: Scrape
python execution/scrape_google_maps.py \
  --keywords "Garage Door" \
  --locations ".tmp/us_locations_sample.csv" \
  --output-sheet "Garage Doors - Sample" \
  --leads 999999 \
  --skip-test --yes

# Step 2: Export (ALWAYS run immediately after scrape)
python execution/export_to_sheets.py \
  --input .tmp/google_maps_checkpoint.json \
  --output-sheet "Garage Doors - Sample"

# Step 3: Clean up checkpoint after successful export
rm .tmp/google_maps_checkpoint.json

# Return the Google Sheet URL to the user
```

## Resume From Interruption

If the scraper is interrupted, just run the same command again - it will resume from the checkpoint:

```bash
# This will resume from where it left off
python execution/scrape_google_maps.py \
  --keywords "Garage Door" \
  --output-sheet "Garage Doors" \
  --leads 10000 \
  --skip-test --yes
```

## API Details

**RapidAPI Maps Data API**
- **Endpoint**: `https://maps-data.p.rapidapi.com/searchmaps.php`
- **Method**: GET
- **Headers**:
  - `x-rapidapi-host: maps-data.p.rapidapi.com`
  - `x-rapidapi-key: <RAPIDAPI_MAPS_DATA_API_KEY>`

**Parameters**:
| Parameter | Description | Default |
|-----------|-------------|---------|
| query | Search term | Required |
| limit | Results per request | 20 |
| country | Country code | us |
| lang | Language | en |
| offset | Pagination offset | 0 |
| zoom | Map zoom level | 13 |

## Locations CSV Format

The locations CSV must have these columns:
- `City` - City name
- `State` - State abbreviation (e.g., NY, CA)
- `Zip` - ZIP code
- `Country` (optional) - Country code (default: USA)

Example:
```csv
City,State,Zip,Country
New York,NY,10001,USA
Los Angeles,CA,90001,USA
Chicago,IL,60601,USA
```

The default file `data/us_locations.csv` contains 6000 US locations.

## Troubleshooting

- **Error: RAPIDAPI_MAPS_DATA_API_KEY not found**: Add your API key to `.env`
- **Error: GOOGLE_DRIVE_FOLDER_ID not found**: Add folder ID to `.env`
- **Error: Google Sheets permission denied**: Service account needs access to the shared drive
- **Rate limit exceeded**: Reduce `--concurrent` (try 5-10)
- **No results found**: Try broader keywords or verify location exists

## Environment Variables

Required in `.env`:
```
RAPIDAPI_MAPS_DATA_API_KEY=your_api_key_here
GOOGLE_DRIVE_FOLDER_ID=your_folder_id_here
```

## Notes

- **Concurrent scraping**: 20 parallel requests by default for fast scraping
- **Full pagination**: Fetches ALL leads per location (up to 200 per location, 10 pages max)
- **Deduplication**: By `place_id` to avoid duplicate businesses across locations
- **Checkpoint/Resume**: Progress saved every 50 leads, auto-resumes on restart
- **Two-step process**: Scrape → Export (allows re-export if sheets export fails)
- **Shared Drive support**: Export tool uses `supportsAllDrives=True` for Google Workspace
