# Find Emails Tool

This project integrates the OpenWeb Ninja Website Contacts Scraper API to find emails, phone numbers, and social media profiles from company websites.

## Setup

1. **Install Python dependencies**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure API Key**:
   - Copy `.env.example` to `.env`
   - Get your API key from [OpenWeb Ninja](https://app.openwebninja.com/api/website-contacts-scraper)
   - Add your API key to `.env`:
     ```
     OPENWEBNINJA_API_KEY=your_actual_api_key_here
     ```

3. **Google Sheets (Recommended)**:
   - Place your `credentials.json` in the project root
   - This is a service account credentials file from Google Cloud Console

## Usage

### Quick Start - Find Emails from Google Sheets (Recommended)

```bash
source .venv/bin/activate
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/1Fb_KCo9pCKfyI46Wab13MNJ9RXLf0tk7gDDOnnk_xxs/edit" \
  --output-sheet "Enriched Leads (Jan 15)" \
  --max-leads 100
```

### All Options

See the full directive at: `directives/find_emails.md`

**From Google Sheets to Google Sheets** (Recommended):
```bash
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Enriched Leads (Jan 15)" \
  --max-leads 100
```


**From Google Sheets to JSON**:
```bash
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output .tmp/enriched.json \
  --max-leads 100
```

**From CSV to JSON**:
```bash
python execution/find_emails.py \
  --source-file "data.csv" \
  --output .tmp/enriched.json \
  --max-leads 100
```

**Verbose mode** (see detailed API responses):
```bash
python execution/find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Enriched Leads" \
  --max-leads 10 \
  --verbose
```

## Features

- ✅ **Multiple Input Formats**: CSV, JSON, Google Sheets
- ✅ **Multiple Output Formats**: JSON, Google Sheets
- ✅ **Smart Filtering**: Only processes leads without emails by default
- ✅ **Cost Control**: Max leads limit and confirmation prompt
- ✅ **Comprehensive Data**: Emails, phones, and social media profiles
- ✅ **Error Handling**: Automatic retries with exponential backoff
- ✅ **Progress Tracking**: Real-time progress updates

## API Data Extracted

The OpenWeb Ninja API extracts:
- **Emails**: All email addresses found on the website
- **Phone Numbers**: All phone numbers found
- **Social Media Profiles**:
  - Facebook
  - Instagram
  - LinkedIn
  - Twitter
  - TikTok
  - GitHub
  - YouTube
  - Pinterest
  - Snapchat

## Safety Features

- **Default Filtering**: Only processes leads with empty email fields
- **Confirmation Required**: Always asks before consuming API credits
- **Cost Estimation**: Shows estimated credits before running
- **Max Leads Limit**: Prevents accidental overspending
- **No Data Overwriting**: Always creates new output files/sheets

## Project Structure

```
cold-email-ninja/
├── directives/
│   └── find_emails.md            # Full SOP documentation
├── execution/
│   └── find_emails.py            # Python script
├── .env.example                  # API key template
├── .env                          # Your API keys (create this)
├── credentials.json              # Google service account
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Troubleshooting

**Error: OPENWEBNINJA_API_KEY not found**
- Make sure you created `.env` file (copy from `.env.example`)
- Add your actual API key to the `.env` file

**Error: Missing website URL**
- The CSV must have a `website` column (or similar: `companyWebsite`, `company_website`, `domain`)

**No contacts found**
- Some websites don't have publicly visible contact information
- The API may not be able to scrape certain websites

**Rate limit errors**
- The script includes automatic retry logic
- Consider reducing `--max-leads` if you hit rate limits frequently

## Documentation

Full documentation is available in `directives/find_emails.md`

## License

This project follows the 3-layer architecture pattern for AI-driven workflows.
