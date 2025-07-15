# Datalab SDK

A Python SDK for the [Datalab API](https://www.datalab.to) - a document intelligence platform powered by [marker](https://github.com/VikParuchuri/marker) and [surya](https://github.com/VikParuchuri/surya).

## Installation

```bash
pip install datalab-sdk
```

## Quick Start

### Authentication

Get your API key from [https://www.datalab.to/app/keys](https://www.datalab.to/app/keys):

```bash
export DATALAB_API_KEY="your_api_key_here"
```

### Basic Usage

```python
from datalab_sdk import DatalabClient

client = DatalabClient() # use env var from above, or pass api_key="your_api_key_here"

# Convert PDF to markdown
result = client.convert("document.pdf")
print(result.markdown)

# OCR a document
ocr_result = client.ocr("document.pdf")
print(ocr_result.pages)  # Get all text as string
```

### Async Usage

```python
import asyncio
from datalab_sdk import AsyncDatalabClient

async def main():
    async with AsyncDatalabClient(api_key="YOUR_API_KEY") as client:
        # Convert PDF to markdown
        result = await client.convert("document.pdf")
        print(result.markdown)
        
        # OCR a document
        ocr_result = await client.ocr("document.pdf")
        print(f"OCR found {len(ocr_result.pages)} pages")

asyncio.run(main())
```

## API Methods

### Document Conversion

Convert PDFs, Office documents, and images to markdown, HTML, or JSON.

```python
# Basic conversion
result = client.convert("document.pdf")

# With options
from datalab_sdk import ConvertOptions
options = ConvertOptions(
    force_ocr=True,
    output_format="html",
    use_llm=True,
    max_pages=10
)
result = client.convert("document.pdf", options=options)

# Convert and save automatically
result = client.convert("document.pdf", save_output="output/result")
```

### OCR

Extract text with bounding boxes from documents.

```python
# Basic OCR
result = client.ocr("document.pdf")
print(result.get_text())

# OCR with options
from datalab_sdk import OCROptions
options = OCROptions(
    max_pages=2
)
result = client.ocr("document.pdf", options)

# OCR and save automatically
result = client.ocr("document.pdf", save_output="output/ocr_result")
```

## CLI Usage

The SDK includes a command-line interface:

```bash
# Convert document to markdown
datalab convert document.pdf

# OCR with JSON output
datalab ocr document.pdf --output-format json
```

## Error Handling

```python
from datalab_sdk import DatalabAPIError, DatalabTimeoutError

try:
    result = client.convert("document.pdf")
except DatalabAPIError as e:
    print(f"API Error: {e}")
except DatalabTimeoutError as e:
    print(f"Timeout: {e}")
```

## Supported File Types

- **PDF**: `pdf`
- **Images**: `png`, `jpeg`, `webp`, `gif`, `tiff`
- **Office Documents**: `docx`, `xlsx`, `pptx`, `doc`, `xls`, `ppt`
- **Other**: `html`, `epub`, `odt`, `ods`, `odp`

## Rate Limits

- 200 requests per 60 seconds
- Maximum 200 concurrent requests
- 200MB file size limit

* email hi@datalab.to for higher limits.

## Examples

### Extract JSON Data

```python
from datalab_sdk import DatalabClient, ConvertOptions

client = DatalabClient(api_key="YOUR_API_KEY")
options = ConvertOptions(output_format="json")
result = client.convert("research_paper.pdf", options=options)

# Parse JSON to find equations
import json
data = json.loads(result.json)
equations = [block for block in data if block.get('block_type') == 'Formula']
print(f"Found {len(equations)} equations")
```

### Batch Process Documents

```python
import asyncio
from pathlib import Path
from datalab_sdk import AsyncDatalabClient

async def process_documents():
    files = list(Path("documents/").glob("*.pdf"))
    
    async with AsyncDatalabClient(api_key="YOUR_API_KEY") as client:
        for file in files[:5]:
            result = await client.convert(str(file), save_output=f"output/{file.stem}")
            print(f"{file.name}: {result.page_count} pages")

asyncio.run(process_documents())
```

## License

MIT License