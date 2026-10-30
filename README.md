# Datalab SDK

A Python SDK for the [Datalab API](https://www.datalab.to/?utm_source=gh-sdk) - it turns documents into verifiable knowledge your agents act on. Datalab is the team behind [Chandra](https://github.com/datalab-to/chandra), [marker](https://github.com/datalab-to/marker), [surya](https://github.com/datalab-to/surya), and [lift](https://github.com/datalab-to/lift).

See the full documentation at [https://documentation.datalab.to](https://documentation.datalab.to).

## Installation

```bash
pip install datalab-python-sdk
```

## Quick Start

### Authentication

[Sign up](https://www.datalab.to/?utm_source=gh-sdk), then get your API key from [https://www.datalab.to/app/keys](https://www.datalab.to/app/keys):

```bash
export DATALAB_API_KEY="your_api_key_here"
```

Or sign in from a terminal, which also works for coding agents:

```bash
datalab login
```

It shows a code and a link. Open the link, sign in or sign up, approve the code, and type the 6-digit code that we email you. The SDK saves the key to `~/.config/datalab/config.json`, readable only by your user, and uses it when `DATALAB_API_KEY` is not set. `datalab logout` deletes it.

### Basic Usage

```python
from datalab_sdk import DatalabClient

client = DatalabClient() # use env var from above, or pass api_key="your_api_key_here"

# Convert PDF to markdown
result = client.convert("document.pdf")
print(result.markdown)
```

`AsyncDatalabClient` has the same methods as `DatalabClient`, as coroutines.

## Progress callbacks

With SDK 0.8.0 or later, pass `on_progress` to processing calls:

```python
result = client.convert(
    "document.pdf",
    on_progress=lambda percent: print(f"Processing: {percent}%"),
)
```

The same argument works with `extract`, `segment`, `run_agent`, `track_changes`,
`create_document`, `ocr`, `fill`, and `form_fields`, including `stream_response_to`.
`run_pipeline` and `get_pipeline_execution` also support it; set `max_polls`
above 1 to keep polling a pipeline. Result models expose `progress_percent`
when the API supplies it.

Callbacks receive increasing integer percentages from existing polls, skipping
missing values and duplicates. Percentages may pause or jump and are not an ETA.
A callback receives 100 only after successful completion and any signed-result
download. Check the returned status and success to determine the outcome.
`AsyncDatalabClient` also accepts async callbacks, which it awaits. Keep callbacks
short: they run inline, and their exceptions propagate to your call. Callback
errors stop local polling; they do not cancel the server request. No extra API
requests are made, and polling intervals are unchanged.

## Conversion options

```python
from datalab_sdk import ConvertOptions

options = ConvertOptions(
    mode="balanced",               # lite, fast, balanced, accurate (SDK default: fast)
    output_format="markdown,html", # markdown, html, json, chunks (comma-separated)
    page_range="0-4",
    extras="chart_understanding,table_cell_bboxes",
    merge_cross_page=True,         # merge tables, paragraphs and lists split across pages
    word_bboxes=True,              # per-word bounding boxes in the HTML output
    save_checkpoint=True,          # reuse the parse in extract() or segment()
)
result = client.convert("document.pdf", options=options)
```

Other options include `paginate`, `add_block_ids`, `disable_html_prettify`, `exact_transcription`, `token_efficient_markdown`, `include_markdown_in_chunks`, `disable_image_extraction`, `disable_image_captions`, `fence_synthetic_captions`, `keep_spreadsheet_formatting`, `keep_pageheader_in_output`, `keep_pagefooter_in_output`, `additional_config`, `webhook_url`, `skip_cache` and `processing_location`. The [API reference](https://documentation.datalab.to/api-reference/convert-document) describes each option.

## Structured extraction

```python
from datalab_sdk import ExtractOptions

result = client.extract(
    "invoice.pdf",
    options=ExtractOptions(
        page_schema='{"properties": {"total": {"type": "number"}}}',  # or schema_id="sch_..."
        extraction_mode="balanced",  # turbo, fast, balanced, accurate
    ),
)
print(result.extraction_schema_json, result.extraction_score_average)
```

When you do not set the parse `mode`, the API picks the parse mode that matches `extraction_mode`.

## Other endpoints

| Method | Endpoint |
| --- | --- |
| `run_agent()` | **Beta:** run an existing published processor (`AgentOptions(processor="cp_...")`) |
| `segment()` | Split a document into sections (`SegmentOptions`) |
| `fill()` | Fill a PDF or image form (`FormFillingOptions`) |
| `form_fields()` | Find the fillable fields in a PDF or image |
| `track_changes()` | Read tracked changes and comments from DOCX or PDF |
| `create_document()` | Make a DOCX from markdown with track changes markup |
| `generate_extraction_schemas()` | Suggest extraction schemas for a saved checkpoint |
| `delete_result()` | Result deletion, by `result.request_id` |
| `upload_files()`, `list_files()`, ... | File storage. Pass the returned `datalab://file-...` reference as `file_url` |
| `create_extraction_schema()`, ... | Saved extraction schemas |
| `create_pipeline()`, `run_pipeline()`, ... | [Pipelines](https://documentation.datalab.to/docs/welcome/sdk/pipelines) |

`run_custom_processor()` and `ocr()` call deprecated API endpoints. Use `run_agent()` and `convert()` instead.

Published-agent execution is in open beta; the API may change. Processor design and raw-prompt execution are not included.

## Inputs, polling and errors

- Send a local file with `file_path`, or a URL or a `datalab://file-...` reference with `file_url`. When you set `processing_location`, send the document as `file_url`.
- Processing methods poll until the result is ready. Set `max_polls` and `poll_interval` to change the limit (default: 300 polls, 1 second apart). Use `stream_response_to` to write a large result straight to disk.
- The SDK retries a submission that gets HTTP 429 or 503, and polls that get 408, 429, 5xx or a connection error. It waits for the `Retry-After` header when the API sends one.
- API errors raise `DatalabAPIError` (with `status_code` and `response_data`) or one of its subclasses: `DatalabAuthenticationError` (401, 403), `DatalabPaymentRequiredError` (402), `DatalabNotFoundError` (404), `DatalabRateLimitError` (429) and `DatalabServerError` (5xx). Invalid arguments raise `DatalabValidationError`, which is also a `ValueError`.

## Workflows

The Datalab API removed the Workflows API. The workflow methods and CLI commands in this SDK are deprecated, and the API does not answer them. Use pipelines instead.

## CLI Usage

The SDK includes a command-line interface:

```bash
# Convert a document or a directory of documents
datalab convert document.pdf --mode balanced --format markdown,html

# Extract with a saved schema
datalab extract invoice.pdf --schema_id sch_XXXXX --extraction_mode fast

# Other commands
datalab agent --help
datalab segment --help
datalab fill --help
datalab form-fields --help
datalab track-changes --help
datalab create-document --help
```

## Contributing

Issues and bug reports are welcome here. We develop the SDK in a private repository and mirror each release to this one, so we do not merge pull requests here directly. Open an issue, or open a pull request to show the change, and we will port it and credit you.

## License

MIT License
