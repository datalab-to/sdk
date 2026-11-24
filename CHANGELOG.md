# Changelog

The newest release is at the top. Each export to this repository uses the
top section as its commit message.

## 0.14.0

Behavior changes:

- The CLI exits with status 1 when any file fails, and `compare run --wait`
  exits with status 1 when the comparison or any of its files fails. Before,
  both exited 0, so scripts and CI couldn't tell. The summary names the
  output directory only when a file was saved, says `No output was saved.`
  when none was, and a run that saves nothing leaves no empty output
  directory.
- The key saved by `datalab login` is only sent to the host it was saved
  for: the same scheme, host and port. A client or CLI command with another
  `base_url`, including `https://datalab.to` for a key saved for
  `https://www.datalab.to`, no longer uses it, and the error names the saved
  host and `datalab login --host`. Before, the client sent the saved key to
  any `base_url`.
- An empty `api_key` (or `--api_key ""`) raises an error instead of being
  sent. An empty or blank `DATALAB_API_KEY` counts as unset, and whitespace
  around a key, such as a newline from a `.env` file, is removed.
- The CLI saves results to `./output/` when `--output_dir` is not given,
  instead of the working directory. Scripts that read results from the
  working directory, such as `./report/report.md` after
  `datalab convert report.pdf`, now find them at `./output/report/report.md`.
  Pass `--output_dir .` to keep the old location. The CLI prints the full
  output path when a run starts and in its summary.
- The CLI's output folders mirror the input directory: `a/report.pdf`
  saves to `<output_dir>/a/report/report.md`. Before, every input saved to
  `<output_dir>/<stem>/`, so `a/report.pdf` and `b/report.pdf` overwrote
  each other. With the same `--output_dir`, a single file keeps its old
  path.

Fixed:

- `import datalab_sdk` no longer raises a `DeprecationWarning` with
  tenacity 9.2 or later, so it works under
  `python -W error::DeprecationWarning`. Retries keep the same backoff.
- An error from the sync `DatalabClient` is no longer chained to
  `RuntimeError: There is no current event loop`. This happened whenever no
  event loop was set: in every script on Python 3.14, and on earlier
  versions in a worker thread or after an earlier `asyncio.run()`.
- The sync `DatalabClient` works under `-W error::DeprecationWarning` on
  Python 3.12 and 3.13. Before, every call failed with "There is no current
  event loop".
- A CLI directory scan skips folders that the CLI saved results to: each
  has a `.datalab-output` file. Before, a second run in the same folder
  processed and billed the first run's HTML and images as new inputs.
  Results saved by 0.13.0 and earlier have no such file, so move or delete
  them before you scan a folder that holds them.
- The CLI checks every output folder before it submits anything. It stops
  when two inputs would save to the same folder (such as `report.pdf` and
  `report.docx` side by side, or names that differ only in case on a file
  system that ignores case), when one input's folder would be inside
  another's (such as `report.pdf` next to a `report/` folder of documents),
  when a folder already exists and is not a CLI output folder, or when a
  folder holds another input's results from an earlier run. The error for
  inputs with the same name suggests `--extensions` to process one type.
- Every `datalab` CLI command uses the key saved by `datalab login`. Before,
  `convert`, `create-document`, `compare` and the other processing commands
  asked for `--api_key` or `DATALAB_API_KEY`. The SDK and the CLI now pick a
  key the same way: `api_key`, then `DATALAB_API_KEY`, then the saved key.

## 0.13.0

New:

- `get_result()` fetches the result of a request that was already
  submitted, without submitting the document again. Pass the check URL, or a
  request ID and its `endpoint`. It returns the same result type as the
  method that submitted the request, and accepts `save_output`,
  `stream_response_to`, `max_polls`, `poll_interval` and `on_progress`.
- An error that a document-processing method or `get_result()` raises after
  the API accepted the request has `request_id` and `check_url`. Before, a
  timeout or failure gave no way to find the running request, which kept
  running and was charged. For `run_pipeline()`, `request_id` holds the
  execution ID; resume with `get_pipeline_execution()`.
- `DatalabPollingTimeoutError`: the polling budget ran out and the request
  is still running. It has the `status` and `progress_percent` of the last
  poll.
- `DatalabTransferTimeoutError`: the request finished, but its result timed
  out while it downloaded.

Both subclass `DatalabTimeoutError`, so existing `except` clauses still
catch them.

Fixed:

- With `stream_response_to`, a signed `result_url` download that timed out
  was retried with the poll, up to 10 polls and 10 downloads, even with
  `max_polls=1`. With the default timeout, that could take about an hour
  before an error. It now raises `DatalabTransferTimeoutError` at once, as
  the call without `stream_response_to` does.

## 0.12.0

New:

- `FileResult`, the result of a call with `stream_response_to`, now has
  `request_id`, `page_count`, `parse_quality_score`, `versions`, `runtime`,
  and `cost_breakdown`. They come from the poll response and are `None` when
  it does not send them; `request_id` falls back to the submit response. For
  a result from a signed `result_url`, the poll response has no `versions`,
  and `runtime` or cost can be unset on an early poll. The saved file can
  contain `versions` and `runtime`.

Fixed:

- With `stream_response_to`, a result from a signed `result_url` (for example
  with `processing_location`) lost the cost, page count and quality score,
  because the download does not contain them. `FileResult` now returns them
  from the poll response. The download is still copied to disk without
  parsing.
- With `stream_response_to`, a connection that dropped while the result
  streamed ended the call without a retry. The SDK now retries the poll as it
  does for other poll errors; it does not submit the document again. An error
  response whose body is cut off or stalls keeps its HTTP status, so a 404
  still fails at once.
- With `stream_response_to`, a timeout while the result streamed surfaced as
  a raw `asyncio.CancelledError`. It now raises `DatalabTimeoutError`. As
  before, it is not retried: `timeout` limits the whole request, so a result
  too large to transfer within it would time out again.
- Without `stream_response_to`, a failed download of a signed `result_url` is
  now retried on a connection error or HTTP 408, 429 or 5xx, and honors
  `Retry-After`. A timeout, or an expired URL (HTTP 403), still fails at once.

## 0.11.0

Comparisons (open beta):

- Evals are now Comparisons. Use `create_comparison()`, `get_comparison()`,
  `list_comparisons()`, `get_comparison_differences()`, and
  `wait_for_comparison()` on both clients. They call the new
  `/api/v1/comparisons` routes. The comparison API may change.
- New models: `Comparison`, `ComparisonVariant`, `ComparisonVariantResult`,
  `ComparisonDocumentResult`, `ComparisonFile`, `ComparisonList`,
  `ComparisonDifference`, `ComparisonDifferences`, and `ComparisonField`.
  `Comparison.comparison_id` replaces `eval_id`, and
  `ComparisonList.comparisons` replaces `evals`.
- New CLI commands: `datalab compare run`, `datalab compare get`,
  `datalab compare list`, and `datalab compare differences`.

Deprecated:

- The 0.10.0 names still work. `create_eval()`, `get_eval()`, `list_evals()`,
  `get_eval_differences()`, and `wait_for_eval()` call the comparison methods
  and raise a `DeprecationWarning`. The `Eval*` model names are aliases of the
  `Comparison*` models. `Comparison.eval_id` and `ComparisonList.evals` are
  read-only aliases. `datalab eval` runs the `datalab compare` commands, prints
  a note, and keeps the 0.10.0 keys in its `--json` output. A future version
  will remove these names.

## 0.10.0

New:

- **Open beta:** Evals on both clients and the CLI. `create_eval()` runs 2 to
  4 variants of a pipeline on the same files or collection and compares them;
  `get_eval()`, `list_evals()`, and `get_eval_differences()` read the status,
  results, and per-document differences; `wait_for_eval()` polls until the
  Eval completes or fails. New models: `Eval`, `EvalVariant`,
  `EvalVariantResult`, `EvalDocumentResult`, `EvalFile`, `EvalDifference`, and
  `EvalDifferences`. New CLI commands: `datalab eval run`, `datalab eval get`,
  `datalab eval list`, and `datalab eval differences`. The Eval API may
  change.

## 0.9.2

New:

- **Open beta:** `ExtractOptions(citation_granularity="phrase")` and the
  `extract --citation_granularity phrase` CLI flag. Each citation then gives
  the exact words of the value in its block, their offset and length, a match
  score, and a box in page pixels when the phrase has its own box. The default
  stays block IDs. The citation format may change.

## 0.9.1

Changed:

- Result polls check every 0.3 seconds for the first 8 seconds of a request,
  then every `poll_interval`. Most single-page results are ready within a few
  seconds, so they arrive sooner. The polling budget is unchanged: the waits
  between polls add up to `max_polls * poll_interval` seconds. A
  `poll_interval` above 1 second turns the fast phase off.

## 0.9.0

New:

- **Open beta:** `redact()` and `get_redaction_presets()` on both clients,
  with `RedactOptions` and `RedactResult`. Results preserve the verification
  verdict and failed checks, including when a completed run returns an
  available PDF with failed verification. Regional signed results use the same
  verified result contract. Use save_output to save the PDF; streaming redaction
  results is not supported. The redaction API may change.
- `upload_files(..., max_concurrency=5)` bounds active uploads, streams
  files from disk, and reuses storage connections within a batch. Results
  retain input order. Uploads respect the configured request timeout.

Fixed:

- `run_pipeline()` and `get_pipeline_execution()` stop polling when an
  execution ends as `needs_review` (its output is ready, but a blocking Check
  failed or it reached its cost limit) and report it to `on_progress` as
  complete. Before, they polled until `max_polls` ran out.
- Synchronous calls propagate an operation's `RuntimeError` without attempting
  to rerun its coroutine. Calling the synchronous client inside an active event
  loop now directs callers to `AsyncDatalabClient`.

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
