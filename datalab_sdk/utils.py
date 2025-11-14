"""
Utility functions for the Datalab SDK
"""

from typing import Dict, List


def generate_spreadsheet_html_viewer(
    sheets: Dict[str, List[dict]], title: str = "XLSX Tables"
) -> str:
    """
    Generate HTML viewer with tabs for each sheet.

    Args:
        sheets: Dictionary mapping sheet names to lists of table blocks.
                Each table block should have an 'html' field containing the table HTML.
        title: Title to display in the HTML viewer

    Returns:
        HTML string with tabs per sheet
    """
    if not sheets:
        return "<html><body><p>No tables found.</p></body></html>"

    # Generate HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - XLSX Tables</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            padding: 20px;
            background: #2c3e50;
            color: white;
        }}
        .header h1 {{
            margin: 0;
            font-size: 24px;
        }}
        .header p {{
            margin: 8px 0 0 0;
            opacity: 0.9;
            font-size: 14px;
        }}
        .tabs {{
            display: flex;
            background: #ecf0f1;
            border-bottom: 2px solid #bdc3c7;
            overflow-x: auto;
        }}
        .tab {{
            padding: 12px 24px;
            cursor: pointer;
            background: #ecf0f1;
            border: none;
            border-right: 1px solid #bdc3c7;
            font-size: 14px;
            font-weight: 500;
            color: #34495e;
            transition: background 0.2s;
            white-space: nowrap;
        }}
        .tab:hover {{
            background: #d5dbdb;
        }}
        .tab.active {{
            background: white;
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            margin-bottom: -2px;
        }}
        .tab-content {{
            display: none;
            padding: 20px;
            overflow-x: auto;
        }}
        .tab-content.active {{
            display: block;
        }}
        .table-container {{
            margin-bottom: 30px;
        }}
        .table-header {{
            font-size: 16px;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 12px;
            padding-bottom: 8px;
            border-bottom: 2px solid #ecf0f1;
        }}
        .table-wrapper {{
            overflow-x: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        table th {{
            background: #f8f9fa;
            font-weight: 600;
            padding: 10px;
            text-align: left;
            border-bottom: 2px solid #dee2e6;
        }}
        table td {{
            padding: 8px 10px;
            border-bottom: 1px solid #e9ecef;
        }}
        table tr:hover {{
            background: #f8f9fa;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{title}</h1>
            <p>Extracted {sum(len(tables) for tables in sheets.values())} table(s) from {len(sheets)} sheet(s)</p>
        </div>
        <div class="tabs">
"""

    # Add tabs
    sheet_names = sorted(sheets.keys())
    for idx, sheet_name in enumerate(sheet_names):
        active_class = "active" if idx == 0 else ""
        html_content += f'            <button class="tab {active_class}" onclick="showTab({idx})">{sheet_name}</button>\n'

    html_content += "        </div>\n"

    # Add tab content
    for idx, sheet_name in enumerate(sheet_names):
        active_class = "active" if idx == 0 else ""
        html_content += f'        <div class="tab-content {active_class}" id="tab-{idx}">\n'

        tables = sheets[sheet_name]
        for table_idx, block in enumerate(tables):
            html = block.get("html", "")
            if not html:
                continue

            html_content += f'            <div class="table-container">\n'
            if len(tables) > 1:
                html_content += f'                <div class="table-header">Table {table_idx + 1}</div>\n'
            html_content += f'                <div class="table-wrapper">\n'
            html_content += f'                    {html}\n'
            html_content += f'                </div>\n'
            html_content += f'            </div>\n'

        html_content += "        </div>\n"

    html_content += """    </div>
    <script>
        function showTab(index) {
            // Hide all tabs and content
            const tabs = document.querySelectorAll('.tab');
            const contents = document.querySelectorAll('.tab-content');

            tabs.forEach(tab => tab.classList.remove('active'));
            contents.forEach(content => content.classList.remove('active'));

            // Show selected tab and content
            tabs[index].classList.add('active');
            contents[index].classList.add('active');
        }
    </script>
</body>
</html>"""

    return html_content

