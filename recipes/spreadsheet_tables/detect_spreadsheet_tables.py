#!/usr/bin/env python3
"""
Quick test script for spreadsheet table extraction using the SDK.

Usage:
    python test_spreadsheet_sdk.py <xlsx_file_path> [--url URL] [--api-key KEY]

For example:
    python test_spreadsheet_sdk.py data/sample-1-sheet.xlsx --api-key YOUR_API_KEY
"""

import argparse
import sys
from pathlib import Path

from datalab_sdk import DatalabClient, ConvertOptions


def main():
    parser = argparse.ArgumentParser(description="Test XLSX table extraction with SDK")
    parser.add_argument("xlsx_file", help="Path to XLSX file to test")
    parser.add_argument("--api-key", help="API key for authenticated requests")

    args = parser.parse_args()

    API_URL = "https://api.datalab.to"
    xlsx_path = Path(args.xlsx_file)
    if not xlsx_path.exists():
        print(f"Error: File not found: {xlsx_path}", file=sys.stderr)
        sys.exit(1)

    #
    # Create client
    #
    client = DatalabClient(api_key=args.api_key, base_url=API_URL)

    #
    # Specify extras options to force XLSX into spreadsheet flow.
    #
    options = ConvertOptions(extras="spreadsheet_table_rec", skip_cache=True)

    print(f"Extracting tables from: {xlsx_path}")

    try:
        #
        # Convert with spreadsheet detection.
        #
        result = client.convert(xlsx_path, options=options)

        if not result.success:
            print(f"Error: {result.error}", file=sys.stderr)
            sys.exit(1)

        print(f"✓ Processing completed successfully")

        tables_by_sheet = result.get_tables_by_sheet()
        sheet_names = result.get_sheet_names()
        table_count = result.get_table_count()

        if table_count > 0:
            print(f"Found {table_count} table(s) across {len(sheet_names)} sheet(s):")
            for sheet_name in sheet_names:
                tables = tables_by_sheet[sheet_name]
                print(f"  - {sheet_name}: {len(tables)} table(s)")

            #
            # Pretty print detected tables in HTML viewer.
            #
            html_path = xlsx_path.parent / f"{xlsx_path.stem}_tables.html"
            result.save_html_viewer(html_path)
            print(f"\n✓ Saved HTML viewer to: {html_path}")

            #
            # Optionally open in browser :D
            #
            print(f"Opening in browser...")
            result.open_in_browser(html_path)

            #
            # Save JSON
            #
            json_path = xlsx_path.parent / f"{xlsx_path.stem}_result.json"
            result.save_json(json_path)
            print(f"✓ Saved JSON to: {json_path}")
        else:
            print("No tables found in result")
            if result.json:
                print(f"JSON keys: {list(result.json.keys())}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

