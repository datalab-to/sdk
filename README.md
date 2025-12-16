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
```

## Workflows

Workflows allow you to chain multiple document processing steps together. Each workflow consists of one or more steps that can depend on previous steps.

**Note:** All workflow operations require authentication. Make sure you have set your `DATALAB_API_KEY` environment variable or pass `api_key` when creating the client (see [Authentication](#authentication) section above).

For more Workflow tips, see our [examples here](./examples/README.md) and [documentation](https://documentation.datalab.to/docs/recipes/workflows/workflow-concepts).

## CLI Usage

The SDK includes a command-line interface:

```bash
# Convert document to markdown
datalab convert document.pdf

# OCR with JSON output
datalab ocr document.pdf --output-format json

# Workflow commands
datalab create-workflow --help
datalab execute-workflow --help
datalab get-execution-status --help
datalab list-workflows --help
datalab get-workflow --help
datalab visualize-workflow --help
```

## License

MIT License
