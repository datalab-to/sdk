# Datalab SDK

A Python SDK for the [Datalab API](https://www.datalab.to) - a document intelligence platform powered by [marker](https://github.com/VikParuchuri/marker) and [surya](https://github.com/VikParuchuri/surya).

See the full documentation at [https://documentation.datalab.to](https://documentation.datalab.to).

## Installation

```bash
pip install datalab-python-sdk
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

## CLI Usage

The SDK includes a command-line interface:

```bash
# Convert document to markdown
datalab convert document.pdf

# OCR with JSON output
datalab ocr document.pdf --output-format json
```

## License

MIT License