#!/usr/bin/env python3
"""
Clean CSV by removing rows with JSON-formatted icebreaker fields.
Filters out rows where ice_breaker contains JSON arrays or objects.
"""
import pandas as pd
import re

def is_json_icebreaker(text):
    """
    Check if the icebreaker field contains JSON formatting.

    Args:
        text: The icebreaker field content

    Returns:
        True if it's JSON formatted (contains {"icebreaker":...}), False otherwise
    """
    if pd.isna(text):
        return False

    text_str = str(text).strip()

    # Check for JSON array markers
    if text_str.startswith('[') and text_str.endswith(']'):
        return True

    # Check for JSON object with "icebreaker" key
    if '"icebreaker"' in text_str or "'icebreaker'" in text_str:
        return True

    # Check for curly braces suggesting JSON object
    if text_str.startswith('{') and text_str.endswith('}'):
        return True

    return False

def clean_csv(input_file, output_file):
    """
    Remove rows with JSON-formatted icebreaker fields.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    # Read the CSV file
    df = pd.read_csv(input_file)

    print(f"Total rows before cleaning: {len(df)}")

    # Find rows with JSON icebreakers
    json_mask = df['ice_breaker'].apply(is_json_icebreaker)
    json_count = json_mask.sum()

    print(f"Rows with JSON icebreakers: {json_count}")

    # Filter out JSON icebreakers
    df_clean = df[~json_mask].copy()

    print(f"Total rows after cleaning: {len(df_clean)}")
    print(f"Removed {json_count} rows")

    # Save cleaned CSV
    df_clean.to_csv(output_file, index=False)

    print(f"✓ Saved cleaned CSV to: {output_file}")

    # Show some examples of removed rows
    if json_count > 0:
        print(f"\nExample of removed icebreakers:")
        removed_df = df[json_mask]
        for idx, row in removed_df.head(3).iterrows():
            company = row.get('company_name', 'N/A')
            icebreaker = str(row['ice_breaker'])[:100]
            print(f"  - {company}: {icebreaker}...")

if __name__ == "__main__":
    input_file = "first-380-leads.csv"
    output_file = "first-380-leads-cleaned.csv"

    clean_csv(input_file, output_file)
