# Changelog

The newest release is at the top. Each export to this repository uses the
top section as its commit message.

## 0.8.0

New:

- Optional `on_progress` callbacks on synchronous and asynchronous document
  processing helpers and pipeline polling. Callbacks receive increasing overall
  percentages from existing polls; async callbacks are also supported. Missing
  percentages are skipped, and 100 is reported only after successful completion
  and any signed-result download. Callback exceptions propagate to the caller.
- Optional `progress_percent` on processing results, streamed `FileResult`,
  pipeline executions and execution steps. Percentages are estimates, may be
  unavailable, and are not an ETA or a replacement for status and success.
- Existing polling intervals and request counts are unchanged.

## 0.7.1

Fixed:

- Device login now identifies requests with the SDK user-agent, matching document
  requests. This prevents the default Python user-agent from being rejected by
  Cloudflare before the device-code API is reached.

## 0.7.0

New:

- `datalab login`: sign in from a terminal or a coding agent with a device
  code. Approve it in the browser, type the emailed 6-digit code, and the SDK
  saves an API key to `~/.config/datalab/config.json` (mode 0600).
  `datalab logout` deletes it.
- The clients use the saved key when no `api_key` is passed and
  `DATALAB_API_KEY` is not set.

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
