# Datalab API - Node.js Examples

Examples demonstrating how to use the Datalab API with Node.js.

## Prerequisites

- Node.js 18+ (for native `fetch` support)
- A Datalab API key from [https://www.datalab.to/app/keys](https://www.datalab.to/app/keys)

## Usage

1. Replace the `API_KEY` constant in the example file with your actual API key:

```javascript
const API_KEY = "dlab_your_actual_api_key_here";
```

2. Run the example:

```bash
# Convert a sample document
node datalab-example.js

# Or provide your own document URL
node datalab-example.js https://example.com/your-document.pdf
```

## Available Endpoints

The examples demonstrate usage of the following Datalab API endpoints:

| Endpoint | Description |
|----------|-------------|
| `POST /api/v1/marker` | Convert documents to Markdown, HTML, or JSON |
| `POST /api/v1/ocr` | Perform OCR on documents |
| `POST /api/v1/fill` | Fill PDF forms with field data |

## Authentication

All requests require the `X-Api-Key` header:

```javascript
headers: {
  "X-Api-Key": "your_api_key_here"
}
```

## API Flow

1. **Submit** - POST your document to the endpoint
2. **Poll** - GET the `request_check_url` until `status` is `"complete"`
3. **Process** - Use the returned markdown/HTML/JSON data

## Documentation

For full API documentation, visit [https://documentation.datalab.to](https://documentation.datalab.to).
