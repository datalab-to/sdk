# Changelog

The newest release is at the top. Each export to this repository uses the
top section as its commit message.

## 0.6.0

Breaking changes:

- The SDK requires Python 3.11 or later. Python 3.10 reached end of life in
  October 2026.
- `ExtractOptions.mode` now defaults to `None`. The SDK sends `mode` only when
  you set it, so the API picks the parse mode from `extraction_mode`. To keep
  the old behavior, set `mode="fast"`.
- App-only evaluation controls are no longer accepted by SDK calls.

New:

- **Open beta:** `run_agent()` and the `agent` CLI command run existing
  published processors. Processor design is not included.
- `form_fields()`, `generate_extraction_schemas()` and `delete_result()`,
  each with a sync version on `DatalabClient`.
- Convert options: `merge_cross_page`, `word_bboxes`, `disable_html_prettify`,
  `exact_transcription`, `keep_pageheader_in_output` and
  `keep_pagefooter_in_output`.
- `processing_location` on the extract, segment, custom processor, track
  changes, OCR, fill, document and upload calls.
- `run_pipeline()` sends `checkpoint_id` and `processing_location`.
  `create_pipeline()` accepts `reuse_draft`.
- CLI commands `fill` and `form-fields`, and CLI flags for the new
  convert and extract options.

Fixes:

- API errors keep the message from the API and map HTTP errors to exception
  classes.
- The SDK downloads signed result URLs and keeps result fields that it
  dropped before.
- The User-Agent header and `datalab --version` show the installed version.
- The CLI `--skip_cache` option is a flag. `--format` accepts a
  comma-separated list.

Deprecated:

- The workflow methods, because the API no longer serves workflows. Use
  pipelines.
- `run_custom_processor()`. Use `run_agent()`.
