# Find Emails SOP

## Goal
Find and extract email addresses, phone numbers, and social media contacts from company websites using the OpenWeb Ninja Website Contacts Scraper API. Accepts leads from CSV files or Google Sheets and enriches them with contact information scraped from their websites.

## Inputs
- **Input Source**: ONE of the following:
  - `source-file`: Path to a CSV or JSON file (e.g., `data.csv`)
  - `source-url`: Full URL of a Google Sheet
- **Output Destination**: ONE of the following:
  - `output`: Path for JSON output
  - `output-sheet`: Name of the new sheet (tab) to create in the existing Google Sheet
- **Folder ID** (Optional): `--folder-id` to create the sheet in a specific Drive folder. Falls back to `GOOGLE_DRIVE_FOLDER_ID` env var if not specified.
- **Max Leads** (Optional): Limit validation to N leads (Default: 100)
  - ⚠️ Each API call costs credits, so this limit prevents accidental overspending
- **API Key**: OpenWeb Ninja API key (from `.env` file as `OPENWEBNINJA_API_KEY`)

## Environment Variables
- `OPENWEBNINJA_API_KEY`: Required. API key for OpenWeb Ninja.
- `GOOGLE_DRIVE_FOLDER_ID`: Optional. Default folder ID for creating Google Sheets. Can be overridden with `--folder-id`.

## Algorithm
1. **Load Data**: Reads leads from CSV, JSON file, or Google Sheet.
2. **Extract Website URLs**: Identifies website URLs from fields like `website`, `companyWebsite`, `company_website`, or `domain`.
3. **Filter Leads**: **BY DEFAULT**, only processes leads with empty email addresses (use `--include-existing` to override).
4. **Validate Input**: Ensures each lead has a valid website URL.
5. **Permission Check**: **REQUIRED** - Asks for user confirmation before running, displaying:
   - Total number of leads
   - Number of leads without emails (that will be processed)
   - Estimated API cost (approximate credits per lead)
   - Max leads limit
6. **API Calls**: Processes leads in **batches of 5 concurrently** (5 requests per second) for maximum speed.
7. **Update Leads**: Enriches the lead data with:
   - Email addresses (multiple if found)
   - Phone numbers
   - Social media profile URLs (Facebook, Instagram, TikTok, LinkedIn, Twitter, GitHub, YouTube, Pinterest, Snapchat)
8. **Checkpoints**: Automatically saves progress to a single checkpoint file (`.tmp/find_emails_checkpoint.json`) every 10 leads. Allows resuming from interruption. Checkpoint is deleted on success.
9. **Output**: Exports the final result to a **new sheet (tab)** in the existing Google Sheet (or file) that includes all original columns plus new columns for **Heading**, **found emails**, phones, and social media profiles.

## Tools
- `execution/find_emails.py` - Email and contact enrichment script using OpenWeb Ninja API

## Output
- **If Source is CSV/JSON**: A new JSON file containing leads with enriched contact data.
- **If Source is Google Sheet**: A **new sheet (tab)** in the existing spreadsheet with enriched lead data.

## Safety Rules

> [!IMPORTANT]
> **SAFETY GUARDS**
> - **Only enriches leads with EMPTY email addresses by default** - this prevents accidental overwriting
> - Use `--include-existing` flag if you want to process ALL leads (including those with emails)
> - This operation uses paid API credits
> - Always asks for user confirmation before running
> - Displays the exact number of leads that will be processed and estimated cost
> - Respects the max leads limit to prevent overspending

> [!WARNING]
> **NEVER OVERWRITE DATA**
> - Always create a **NEW** file or **NEW** sheet (tab) in the same spreadsheet for the enriched output
> - Do not use the same filename for input and output
> - Preserve all original lead fields

## Instructions

### Option 1: Enrich from Google Sheet → New Sheet (Recommended)

```bash
source .venv/bin/activate
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/1Fb_KCo9pCKfyI46Wab13MNJ9RXLf0tk7gDDOnnk_xxs/edit" \
  --output-sheet "Enriched Leads (Jan 15)" \
  --max-leads 100
```

### Option 2: Enrich from Google Sheet → New JSON

```bash
source .venv/bin/activate
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/1Fb_KCo9pCKfyI46Wab13MNJ9RXLf0tk7gDDOnnk_xxs/edit" \
  --output .tmp/enriched_leads.json \
  --max-leads 100
```

### Option 3: Enrich from CSV → New JSON

```bash
source .venv/bin/activate
python execution/find_emails.py \
  --source-file "custom home builders-1 - Data.csv" \
  --output .tmp/enriched_home_builders.json \
  --max-leads 100
```

### Option 4: Enrich from CSV → New Sheet (Tab)

```bash
source .venv/bin/activate
python execution/find_emails.py \
  --source-file "custom home builders-1 - Data.csv" \
  --output-sheet "Enriched Home Builders (Jan 15)" \
  --max-leads 50
```

### Optional Flags
- `--verbose` or `-v`: Display detailed information for each API call
- `--sheet-name "Sheet1"`: Specify source sheet name when using Google Sheets (default: first sheet)
- `--include-existing`: Process ALL leads including those with existing emails (by default, only leads with empty emails are enriched)

### Example Workflow

1. **Enrich contacts from Google Sheets** (Recommended):
   ```bash
   source .venv/bin/activate
   python execution/find_emails.py \
     --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
     --output-sheet "Enriched Leads" \
     --folder-id "YOUR_DRIVE_FOLDER_ID" \
     --max-leads 100 \
     --verbose
   ```

2. **Or enrich from CSV**:
   ```bash
   source .venv/bin/activate
   python execution/find_emails.py \
     --source-file "custom home builders-1 - Data.csv" \
     --output .tmp/enriched_leads.json \
     --max-leads 100 \
     --verbose
   ```

## Performance

- **Speed**: Processes **5 leads concurrently** (5 requests per second)
- **Checkpoints**: Single auto-updating checkpoint file; deletes on success
- **Estimated time**: ~20 seconds per 100 leads (vs ~50 seconds sequential)
- **Safe to interrupt**: Resume exactly where you left off

3. **Review results**:
   The script will show:
   ```
   📧 Find Emails Tool
   ==================================================
   
   📊 Summary:
     Total leads: 150
     Leads without email: 87
     Will process: 87 leads (only empty emails)
     Max leads limit: 100
     Estimated cost: ~87 credits
   
   ⚠️  WARNING: This will consume API credits!
   ==================================================
   
   Continue? (yes/no): yes
   
   Processing leads... ━━━━━━━━━━━━━━━━ 100/100 100%
   
   ✅ Contact Enrichment Summary:
      Emails found: 78 (78%)
      Phone numbers found: 65 (65%)
      Social profiles found: 82 (82%)
      No contacts found: 22 (22%)
      Total processed: 100
   
   Saved to: .tmp/enriched_leads.json
   ```

## API Details

OpenWeb Ninja Website Contacts Scraper API:
- **Method**: GET
- **Endpoint**: `https://api.openwebninja.com/website-contacts`
- **Authentication**: API key via `x-api-key` header
- **Parameters**: `url` (website URL to scrape)

Response format:
```json
{
  "success": true,
  "data": {
    "emails": ["contact@company.com", "info@company.com"],
    "phones": ["+1-234-567-8900"],
    "social": {
      "facebook": "https://facebook.com/company",
      "instagram": "https://instagram.com/company",
      "linkedin": "https://linkedin.com/company/company",
      "twitter": "https://twitter.com/company",
      "tiktok": "https://tiktok.com/@company",
      "github": "https://github.com/company",
      "youtube": "https://youtube.com/company",
      "pinterest": "https://pinterest.com/company",
      "snapchat": "https://snapchat.com/add/company"
    }
  }
}
```

The script enriches leads with:
- `heading`: Short heading/title for the row (used as the sheet column header field)
- `email`: Primary email address (first from the list)
- `emails`: All found email addresses (array)
- `phone`: Primary phone number (first from the list)
- `phones`: All found phone numbers (array)
- `social_facebook`, `social_instagram`, `social_linkedin`, `social_twitter`, `social_tiktok`, `social_github`, `social_youtube`, `social_pinterest`, `social_snapchat`: Social media profile URLs

## Required Fields

For the API to work, each lead must have:
- **Website URL**: `website`, `companyWebsite`, `company_website`, or `domain`

Leads missing a valid website URL will be skipped.

## Cost Management

- Default max leads: **100**
- Each API call costs approximately **1 credit**
- **By default, only processes leads with empty emails** - this is the primary cost control guard
- The script will **always ask for permission** before making API calls
- Shows exact count of leads that will be processed before asking for confirmation
- Use `--max-leads` to limit processing and control costs
- Use `--include-existing` flag ONLY if you want to process leads that already have emails (usually not needed)

## Troubleshooting

- **Error: OPENWEBNINJA_API_KEY not found**: Add your API key to the `.env` file
- **Error: Missing website URL**: Ensure leads have a valid website field
- **No contacts found**: The website may not have publicly visible contact information
- **Rate limit errors**: The script includes automatic retry with exponential backoff
- **Permission denied**: User must confirm with "yes" to proceed

## Notes

- The script preserves all original fields in the output
- Existing email fields are preserved unless `--include-existing` is used
- Empty or invalid responses from OpenWeb Ninja leave the contact fields empty
- Progress bar shows real-time processing status
- All errors are logged with details for debugging
- Social media profiles are only added if found by the API
