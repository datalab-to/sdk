#!/usr/bin/env python3
"""
Datalab SDK Command Line Interface
"""

import os
import sys
import asyncio
from pathlib import Path
from typing import Optional, List
import click
from tqdm import tqdm

from datalab_sdk.client import AsyncDatalabClient, DatalabClient
from datalab_sdk.mimetypes import SUPPORTED_EXTENSIONS
from datalab_sdk.models import (
    OCROptions,
    ConvertOptions,
    ExtractOptions,
    SegmentOptions,
    CustomPipelineOptions,
    TrackChangesOptions,
    FormFillingOptions,
    FormFieldsOptions,
    AgentOptions,
    ProcessingOptions,
    WorkflowStep,
    InputConfig,
)
from datalab_sdk.exceptions import DatalabError
from datalab_sdk.settings import settings
import json


OUTPUT_FORMATS = ("markdown", "html", "json", "chunks")


def _validate_output_formats(allowed):
    """Make a click callback that checks a comma-separated format list"""

    def callback(ctx, param, value):
        if value is None:
            return value
        formats = [f.strip() for f in value.split(",") if f.strip()]
        invalid = [f for f in formats if f not in allowed]
        if invalid or not formats:
            raise click.BadParameter(
                f"Each format must be one of: {', '.join(allowed)}. "
                "Separate formats with commas."
            )
        return ",".join(formats)

    return callback


def _load_json_option(value: Optional[str], name: str):
    """Load a JSON option from a file path or from an inline JSON string"""
    if value is None:
        return None
    path = Path(value)
    try:
        if path.exists() and path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"{name} is not valid JSON: {e}")


def _schema_option_text(value: Optional[str]) -> Optional[str]:
    """Return a schema as a JSON string. The value is a path or inline JSON."""
    if value is None:
        return None
    path = Path(value)
    if path.exists() and path.is_file():
        return path.read_text(encoding="utf-8")
    return value


# Common CLI options
def common_options(func):
    """Common options for all commands"""
    func = click.option("--api_key", required=False, help="Datalab API key")(func)
    func = click.option(
        "--output_dir", "-o", required=False, type=click.Path(), help="Output directory"
    )(func)
    func = click.option(
        "--max_pages", type=int, help="Maximum number of pages to process"
    )(func)
    func = click.option(
        "--extensions", help="Comma-separated list of file extensions (for directories)"
    )(func)
    func = click.option(
        "--max_concurrent", default=5, type=int, help="Maximum concurrent requests"
    )(func)
    func = click.option(
        "--base_url", default=settings.DATALAB_HOST, help="API base URL"
    )(func)
    func = click.option(
        "--page_range", help='Page range to process (e.g., "0-2" or "0,1,2")'
    )(func)
    func = click.option(
        "--skip_cache", is_flag=True, help="Skip the cache when running inference"
    )(func)
    func = click.option(
        "--max_polls", default=300, type=int, help="Maximum number of polling attempts"
    )(func)
    func = click.option(
        "--poll_interval", default=1, type=int, help="Polling interval in seconds"
    )(func)
    return func


def webhook_option(func):
    return click.option(
        "--webhook_url", help="Webhook URL that the API calls when a request is done"
    )(func)


def marker_options(func):
    """Options specific to convert command"""
    func = click.option(
        "--format",
        "output_format",
        default="markdown",
        callback=_validate_output_formats(OUTPUT_FORMATS),
        help="Output format: markdown, html, json or chunks. Separate several with commas.",
    )(func)
    func = click.option(
        "--paginate", is_flag=True, help="Add page delimiters to output"
    )(func)
    func = click.option(
        "--disable_image_extraction", is_flag=True, help="Disable extraction of images"
    )(func)
    func = click.option(
        "--disable_image_captions",
        is_flag=True,
        help="Disable synthetic image captions/descriptions in output",
    )(func)
    func = click.option(
        "--add_block_ids", is_flag=True, help="Add block IDs to HTML output"
    )(func)
    func = click.option(
        "--mode",
        type=click.Choice(["lite", "fast", "balanced", "accurate"]),
        default="fast",
        help="Processing mode (the SDK default is fast)",
    )(func)
    return func


def convert_extra_options(func):
    """Convert options that the API added after the first CLI release"""
    flags = [
        (
            "--merge_cross_page",
            "Merge tables, paragraphs and lists split across pages (billed as a surcharge)",
        ),
        ("--word_bboxes", "Predict per-word bounding boxes (HTML output)"),
        ("--disable_html_prettify", "Return compact HTML"),
        ("--exact_transcription", "Transcribe text exactly as printed"),
        (
            "--fence_synthetic_captions",
            "Wrap synthetic image captions in comment markers",
        ),
        ("--token_efficient_markdown", "Use compact markdown for LLMs"),
        ("--include_markdown_in_chunks", "Add markdown to each chunk or JSON block"),
        ("--keep_spreadsheet_formatting", "Keep spreadsheet formatting"),
        ("--keep_pageheader_in_output", "Keep page headers in markdown and HTML"),
        ("--keep_pagefooter_in_output", "Keep page footers in markdown and HTML"),
        ("--save_checkpoint", "Save a checkpoint for later extract or segment calls"),
    ]
    for name, help_text in flags:
        func = click.option(name, is_flag=True, help=help_text)(func)
    func = click.option(
        "--extras",
        help="Comma-separated extras, e.g. track_changes,chart_understanding,"
        "table_cell_bboxes,list_item_bboxes,extract_links,extract_bookmarks,"
        "infographic,new_block_types",
    )(func)
    func = click.option(
        "--additional_config", help="Additional config as JSON or a path to a JSON file"
    )(func)
    func = webhook_option(func)
    return func


def find_files_in_directory(
    directory: Path, extensions: Optional[List[str]] = None
) -> List[Path]:
    """Find all supported files in a directory"""
    if extensions is None:
        extensions = SUPPORTED_EXTENSIONS

    files = []
    for file_path in directory.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in extensions:
            files.append(file_path)

    return files


async def process_files_async(
    files: List[Path],
    output_dir: Path,
    method: str,
    options: Optional[ProcessingOptions] = None,
    max_concurrent: int = 5,
    api_key: str | None = None,
    base_url: str | None = None,
    max_polls: int = 300,
    poll_interval: int = 1,
) -> List[dict]:
    """Process files asynchronously"""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def call_api(client, file_path, output_path):
        """Make API call - client handles retries for rate limits"""
        api_method = getattr(client, method)
        # For extract/segment with checkpoint_id, don't pass file_path
        has_checkpoint = (
            options is not None
            and hasattr(options, "checkpoint_id")
            and options.checkpoint_id is not None
        )
        if has_checkpoint:
            return await api_method(
                options=options,
                save_output=output_path,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        return await api_method(
            file_path,
            options=options,
            save_output=output_path,
            max_polls=max_polls,
            poll_interval=poll_interval,
        )

    async def process_single_file(file_path: Path) -> dict:
        async with semaphore:
            try:
                # Create output path
                relative_path = file_path.name
                output_path = (
                    output_dir / Path(relative_path).stem / Path(relative_path).stem
                )

                async with AsyncDatalabClient(
                    api_key=api_key, base_url=base_url
                ) as client:
                    result = await call_api(client, file_path, output_path)

                return {
                    "file_path": str(file_path),
                    "output_path": str(output_path),
                    "success": result.success,
                    "error": result.error,
                    "page_count": result.page_count,
                }
            except Exception as e:
                return {
                    "file_path": str(file_path),
                    "output_path": None,
                    "success": False,
                    "error": str(e),
                    "page_count": None,
                }

    # Process all files concurrently with progress bar
    tasks = [asyncio.create_task(process_single_file(file_path)) for file_path in files]
    results = []

    with tqdm(total=len(tasks), desc="Processing", unit="file") as pbar:
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            # Update progress bar description with current file
            filename = Path(result["file_path"]).name
            status = "✓" if result["success"] else "✗"
            pbar.set_postfix_str(f"{status} {filename[:30]}")
            pbar.update(1)

    return results


def setup_output_directory(output_dir: Optional[str]) -> Path:
    """Setup and return output directory"""
    if output_dir is None:
        output_dir = os.getcwd()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def parse_extensions(extensions: Optional[str]) -> Optional[List[str]]:
    """Parse file extensions from comma-separated string"""
    if not extensions:
        return None

    file_extensions = [ext.strip() for ext in extensions.split(",")]
    return [ext if ext.startswith(".") else f".{ext}" for ext in file_extensions]


def get_files_to_process(
    path: Path, file_extensions: Optional[List[str]]
) -> List[Path]:
    """Get list of files to process"""
    if path.is_file():
        # Single file processing
        if file_extensions and path.suffix.lower() not in file_extensions:
            click.echo(f"Skipping {path}: unsupported file type", err=True)
            sys.exit(1)
        return [path]
    else:
        # Directory processing
        return find_files_in_directory(path, file_extensions)


def show_results(results: List[dict], operation: str, output_dir: Path):
    """Display processing results"""
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    click.echo(f"\n{operation} Summary:")
    click.echo(f"   Successfully processed: {successful} files")
    if failed > 0:
        click.echo(f"   Failed: {failed} files")

        # Show failed files
        click.echo("\n   Failed files:")
        for result in results:
            if not result["success"]:
                click.echo(f"      - {result['file_path']}: {result['error']}")

    click.echo(f"\nOutput saved to: {output_dir}")


def process_documents(
    path: str,
    method: str,
    api_key: Optional[str],
    output_dir: Optional[str],
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
    # Convert-specific options
    output_format: Optional[str] = None,
    paginate: bool = False,
    disable_image_extraction: bool = False,
    disable_image_captions: bool = False,
    add_block_ids: bool = False,
    mode: Optional[str] = "fast",
    # Extract-specific
    page_schema: Optional[str] = None,
    checkpoint_id: Optional[str] = None,
    # Segment-specific
    segmentation_schema: Optional[str] = None,
    # Custom pipeline-specific
    pipeline_id: Optional[str] = None,
    # Options object override
    options_override: Optional[ProcessingOptions] = None,
):
    """Unified document processing function"""
    try:
        # Validate inputs
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        if base_url is None:
            base_url = settings.DATALAB_HOST

        output_dir_path = setup_output_directory(output_dir)
        file_extensions = parse_extensions(extensions)

        # Get files to process
        path = Path(path)
        to_process = get_files_to_process(path, file_extensions)

        if not to_process:
            click.echo(f"No supported files found in {path}", err=True)
            sys.exit(1)

        click.echo(f"Found {len(to_process)} files to process")

        # Create processing options based on method
        if options_override:
            options = options_override
        elif method == "convert":
            options = ConvertOptions(
                output_format=output_format or "markdown",
                max_pages=max_pages,
                paginate=paginate,
                disable_image_extraction=disable_image_extraction,
                disable_image_captions=disable_image_captions,
                page_range=page_range,
                skip_cache=skip_cache,
                add_block_ids=add_block_ids,
                mode=mode or "fast",
            )
        elif method == "extract":
            options = ExtractOptions(
                page_schema=page_schema or "",
                checkpoint_id=checkpoint_id,
                output_format=output_format or "markdown",
                max_pages=max_pages,
                page_range=page_range,
                skip_cache=skip_cache,
            )
            if mode is not None:
                options.mode = mode
        elif method == "segment":
            options = SegmentOptions(
                segmentation_schema=segmentation_schema or "",
                checkpoint_id=checkpoint_id,
                mode=mode or "fast",
                max_pages=max_pages,
                page_range=page_range,
                skip_cache=skip_cache,
            )
        elif method in ("run_custom_pipeline", "_run_custom_processor"):
            method = "_run_custom_processor"
            options = CustomPipelineOptions(
                pipeline_id=pipeline_id or "",
                mode=mode or "fast",
                output_format=output_format or "markdown",
                max_pages=max_pages,
                page_range=page_range,
                skip_cache=skip_cache,
            )
        elif method == "track_changes":
            options = TrackChangesOptions(
                output_format=output_format or "markdown,html,chunks",
                paginate=paginate,
                max_pages=max_pages,
                page_range=page_range,
                skip_cache=skip_cache,
            )
        else:  # method == "ocr"
            options = OCROptions(
                max_pages=max_pages,
                page_range=page_range,
                skip_cache=skip_cache,
            )

        results = asyncio.run(
            process_files_async(
                to_process,
                output_dir_path,
                method,
                options=options,
                max_concurrent=max_concurrent,
                api_key=api_key,
                base_url=base_url,
                max_polls=max_polls,
                poll_interval=poll_interval,
            )
        )

        # Show results
        operation_names = {
            "convert": "Conversion",
            "extract": "Extraction",
            "segment": "Segmentation",
            "_run_custom_processor": "Custom Pipeline",
            "track_changes": "Track Changes",
            "fill": "Form Filling",
            "form_fields": "Form Fields",
            "run_agent": "Agent (Beta)",
            "ocr": "OCR",
        }
        operation = operation_names.get(method, method.title())
        show_results(results, operation, output_dir_path)

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.group()
@click.version_option(version=settings.VERSION)
def cli():
    pass


@click.command()
@click.option(
    "--host",
    default=None,
    help="API host (default: DATALAB_HOST or https://www.datalab.to)",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Print the sign-in URL instead of opening a browser",
)
def login(host: Optional[str], no_browser: bool):
    """Sign in from this terminal and save an API key (device code)."""
    from datalab_sdk.auth import device_login, save_api_key

    host = (host or settings.DATALAB_HOST).rstrip("/")

    def show(code: dict):
        click.echo("To sign in, open this page and approve the code:")
        click.echo(f"  {code['verification_uri_complete']}")
        click.echo(f"  Code: {code['user_code']}")
        click.echo("Then type the 6-digit code that we email you.")
        click.echo("Waiting for approval...")

    try:
        result = device_login(host=host, open_browser=not no_browser, on_code=show)
    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    path = save_api_key(result["api_key"], host, result.get("team_name"))
    click.echo(f"Signed in to team {result.get('team_name')}. API key saved to {path}.")
    click.echo(
        "DATALAB_API_KEY in your environment still takes priority over the saved key."
    )


@click.command()
def logout():
    """Delete the API key that `datalab login` saved. The key itself stays valid until you revoke it."""
    from datalab_sdk.auth import config_path, delete_saved_api_key

    if delete_saved_api_key():
        click.echo(
            f"Removed {config_path()}. Revoke the key at https://www.datalab.to/app/keys if you no longer need it."
        )
    else:
        click.echo("No saved API key.")


@click.command()
@click.argument("path", type=click.Path(exists=True))
@common_options
@marker_options
@convert_extra_options
def convert(
    path: str,
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
    output_format: str,
    paginate: bool,
    disable_image_extraction: bool,
    disable_image_captions: bool,
    add_block_ids: bool,
    mode: str,
    merge_cross_page: bool,
    word_bboxes: bool,
    disable_html_prettify: bool,
    exact_transcription: bool,
    fence_synthetic_captions: bool,
    token_efficient_markdown: bool,
    include_markdown_in_chunks: bool,
    keep_spreadsheet_formatting: bool,
    keep_pageheader_in_output: bool,
    keep_pagefooter_in_output: bool,
    save_checkpoint: bool,
    extras: Optional[str],
    additional_config: Optional[str],
    webhook_url: Optional[str],
):
    """Convert documents to markdown, HTML, JSON, or chunks"""
    options = ConvertOptions(
        output_format=output_format or "markdown",
        max_pages=max_pages,
        page_range=page_range,
        skip_cache=skip_cache,
        paginate=paginate,
        disable_image_extraction=disable_image_extraction,
        disable_image_captions=disable_image_captions,
        add_block_ids=add_block_ids,
        mode=mode,
        merge_cross_page=merge_cross_page,
        word_bboxes=word_bboxes,
        disable_html_prettify=disable_html_prettify,
        exact_transcription=exact_transcription,
        fence_synthetic_captions=fence_synthetic_captions,
        token_efficient_markdown=token_efficient_markdown,
        include_markdown_in_chunks=include_markdown_in_chunks,
        keep_spreadsheet_formatting=keep_spreadsheet_formatting,
        keep_pageheader_in_output=keep_pageheader_in_output,
        keep_pagefooter_in_output=keep_pagefooter_in_output,
        save_checkpoint=save_checkpoint,
        extras=extras,
        additional_config=_load_json_option(additional_config, "--additional_config"),
        webhook_url=webhook_url,
    )
    process_documents(
        path=path,
        method="convert",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--page_schema",
    help="JSON schema for extraction (must contain 'properties'), inline or a path to a JSON file. "
    "Give --page_schema or --schema_id.",
)
@click.option("--schema_id", help="ID of a saved extraction schema (sch_XXXXX)")
@click.option(
    "--schema_version", type=int, help="Version of the saved schema (with --schema_id)"
)
@click.option(
    "--checkpoint_id", help="Checkpoint ID from a previous convert (skips re-parsing)"
)
@click.option(
    "--format",
    "output_format",
    default="markdown",
    callback=_validate_output_formats(OUTPUT_FORMATS),
    help="Output format of the parsed text: markdown, html, json or chunks",
)
@click.option(
    "--mode",
    type=click.Choice(["fast", "balanced", "accurate"]),
    default=None,
    help="Parse mode. When omitted, the API picks the mode that matches --extraction_mode.",
)
@click.option(
    "--extraction_mode",
    type=click.Choice(["turbo", "fast", "balanced", "accurate"]),
    default=None,
    help="Extraction mode (the API default is balanced)",
)
@click.option(
    "--save_checkpoint", is_flag=True, help="Save a checkpoint for later calls"
)
@webhook_option
@common_options
def extract(
    path: str,
    page_schema: Optional[str],
    schema_id: Optional[str],
    schema_version: Optional[int],
    checkpoint_id: Optional[str],
    output_format: str,
    mode: Optional[str],
    extraction_mode: Optional[str],
    save_checkpoint: bool,
    webhook_url: Optional[str],
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """Extract structured data from documents using a JSON schema"""
    if bool(page_schema) == bool(schema_id):
        raise click.UsageError("Give exactly one of --page_schema or --schema_id.")
    options = ExtractOptions(
        page_schema=_schema_option_text(page_schema) or "",
        schema_id=schema_id,
        schema_version=schema_version,
        checkpoint_id=checkpoint_id,
        output_format=output_format or "markdown",
        extraction_mode=extraction_mode,
        save_checkpoint=save_checkpoint,
        webhook_url=webhook_url,
        max_pages=max_pages,
        page_range=page_range,
        skip_cache=skip_cache,
    )
    if mode is not None:
        options.mode = mode
    process_documents(
        path=path,
        method="extract",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--segmentation_schema",
    required=True,
    help="Segmentation schema as JSON, inline or a path to a JSON file",
)
@click.option(
    "--checkpoint_id", help="Checkpoint ID from a previous convert (skips re-parsing)"
)
@click.option(
    "--mode",
    type=click.Choice(["fast", "balanced", "accurate"]),
    default="fast",
    help="Processing mode",
)
@click.option(
    "--save_checkpoint", is_flag=True, help="Save a checkpoint for later calls"
)
@webhook_option
@common_options
def segment(
    path: str,
    segmentation_schema: str,
    checkpoint_id: Optional[str],
    mode: str,
    save_checkpoint: bool,
    webhook_url: Optional[str],
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """Segment documents into sections using a schema"""
    options = SegmentOptions(
        segmentation_schema=_schema_option_text(segmentation_schema) or "",
        checkpoint_id=checkpoint_id,
        mode=mode,
        save_checkpoint=save_checkpoint,
        webhook_url=webhook_url,
        max_pages=max_pages,
        page_range=page_range,
        skip_cache=skip_cache,
    )
    process_documents(
        path=path,
        method="segment",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command("custom-pipeline")
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--pipeline_id",
    required=True,
    help="Custom pipeline ID to execute (cp_XXXXX format)",
)
@click.option(
    "--format",
    "output_format",
    default="markdown",
    callback=_validate_output_formats(OUTPUT_FORMATS),
    help="Output format",
)
@click.option(
    "--mode",
    type=click.Choice(["fast", "balanced", "accurate"]),
    default="fast",
    help="Processing mode",
)
@common_options
def custom_pipeline(
    path: str,
    pipeline_id: str,
    output_format: str,
    mode: str,
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """[Deprecated] Run a custom pipeline. Use datalab agent --processor cp_XXXXX instead."""
    click.echo(
        "Warning: custom-pipeline is deprecated. "
        "Use datalab agent --processor cp_XXXXX instead.",
        err=True,
    )
    process_documents(
        path=path,
        method="_run_custom_processor",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        output_format=output_format,
        mode=mode,
        pipeline_id=pipeline_id,
    )


@click.command("track-changes")
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--format",
    "output_format",
    default="markdown,html,chunks",
    callback=_validate_output_formats(("markdown", "html", "chunks")),
    help="Comma-separated output formats (markdown, html, chunks)",
)
@click.option("--paginate", is_flag=True, help="Separate output by page")
@webhook_option
@common_options
def track_changes(
    path: str,
    output_format: str,
    paginate: bool,
    webhook_url: Optional[str],
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """Extract tracked changes and comments from DOCX or PDF documents"""
    options = TrackChangesOptions(
        output_format=output_format,
        paginate=paginate,
        webhook_url=webhook_url,
        max_pages=max_pages,
        page_range=page_range,
        skip_cache=skip_cache,
    )
    process_documents(
        path=path,
        method="track_changes",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--field_data",
    required=True,
    help='Field data as JSON or a path to a JSON file: {"key": {"value": "...", "description": "..."}}',
)
@click.option("--context", help="Optional context to guide form filling")
@common_options
def fill(
    path: str,
    field_data: str,
    context: Optional[str],
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """Fill PDF or image forms with field data"""
    if max_pages is not None:
        raise click.UsageError("fill does not accept --max_pages. Use --page_range.")
    options = FormFillingOptions(
        field_data=_load_json_option(field_data, "--field_data"),
        context=context,
        page_range=page_range,
        skip_cache=skip_cache,
    )
    process_documents(
        path=path,
        method="fill",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=None,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command("form-fields")
@click.argument("path", type=click.Path(exists=True))
@webhook_option
@common_options
def form_fields(
    path: str,
    webhook_url: Optional[str],
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    page_range: Optional[str],
    skip_cache: bool,
    max_polls: int,
    poll_interval: int,
):
    """Find the fillable fields in PDFs or images"""
    options = FormFieldsOptions(
        webhook_url=webhook_url,
        max_pages=max_pages,
        page_range=page_range,
        skip_cache=skip_cache,
    )
    process_documents(
        path=path,
        method="form_fields",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=max_pages,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=page_range,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--processor",
    required=True,
    help="Published agent processor, e.g. cp_abc123 or cp_abc123@2.0",
)
@click.option(
    "--verification_effort",
    type=click.Choice(["fast", "balanced", "accurate"]),
    default=None,
    help="Verification effort (the API default is fast)",
)
@click.option(
    "--mode",
    type=click.Choice(["fast", "balanced"]),
    default=None,
    help="Run mode (the API default is balanced)",
)
@click.option("--return_images", is_flag=True, help="Return images inline")
@click.option("--skip_cache", is_flag=True, help="Skip the cache")
@webhook_option
@click.option("--api_key", required=False, help="Datalab API key")
@click.option(
    "--output_dir", "-o", required=False, type=click.Path(), help="Output directory"
)
@click.option(
    "--extensions", help="Comma-separated list of file extensions (for directories)"
)
@click.option(
    "--max_concurrent", default=5, type=int, help="Maximum concurrent requests"
)
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
@click.option(
    "--max_polls", default=300, type=int, help="Maximum number of polling attempts"
)
@click.option(
    "--poll_interval", default=1, type=int, help="Polling interval in seconds"
)
def agent(
    path: str,
    processor: str,
    verification_effort: Optional[str],
    mode: Optional[str],
    return_images: bool,
    skip_cache: bool,
    webhook_url: Optional[str],
    api_key: Optional[str],
    output_dir: Optional[str],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
    max_polls: int,
    poll_interval: int,
):
    """[Beta] Run a published agent processor on documents"""
    options = AgentOptions(
        processor=processor,
        verification_effort=verification_effort,
        mode=mode,
        return_images=return_images,
        skip_cache=skip_cache,
        webhook_url=webhook_url,
    )
    process_documents(
        path=path,
        method="run_agent",
        api_key=api_key,
        output_dir=output_dir,
        max_pages=None,
        extensions=extensions,
        max_concurrent=max_concurrent,
        base_url=base_url,
        page_range=None,
        skip_cache=skip_cache,
        max_polls=max_polls,
        poll_interval=poll_interval,
        options_override=options,
    )


@click.command("create-document")
@click.option(
    "--markdown",
    "markdown_input",
    required=True,
    help="Markdown content or path to markdown file",
)
@click.option(
    "--output",
    "-o",
    "output_path",
    required=True,
    type=click.Path(),
    help="Output file path for the DOCX",
)
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def create_document(
    markdown_input: str,
    output_path: str,
    api_key: Optional[str],
    base_url: str,
):
    """Create a DOCX document from markdown"""
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        # Check if markdown_input is a file path
        md_path = Path(markdown_input)
        if md_path.exists() and md_path.is_file():
            markdown_content = md_path.read_text(encoding="utf-8")
        else:
            markdown_content = markdown_input

        client = DatalabClient(api_key=api_key, base_url=base_url)
        result = client.create_document(
            markdown=markdown_content,
            save_output=output_path,
        )

        if result.success:
            click.echo(
                f"Document created successfully: {Path(output_path).with_suffix('.docx')}"
            )
        else:
            click.echo(f"Document creation failed: {result.error}", err=True)
            sys.exit(1)

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# Workflow commands
def _echo_workflows_removed():
    click.echo(
        "Warning: the Datalab API removed the Workflows API, so workflow "
        "commands fail. Use pipelines instead (see the SDK README).",
        err=True,
    )


@click.command()
@click.option("--name", required=True, help="Name of the workflow")
@click.option("--team_id", required=True, type=int, help="Team ID for the workflow")
@click.option(
    "--steps",
    required=True,
    help="JSON string or path to JSON file with workflow steps",
)
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def create_workflow(
    name: str,
    team_id: int,
    steps: str,
    api_key: Optional[str],
    base_url: str,
):
    """[Deprecated: the API removed workflows] Create a new workflow"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        # Parse steps from JSON string or file
        steps_path = Path(steps)
        if steps_path.exists():
            with open(steps_path, "r") as f:
                steps_data = json.load(f)
        else:
            steps_data = json.loads(steps)

        # Create WorkflowStep objects
        workflow_steps = [
            WorkflowStep(
                step_key=step["step_key"],
                unique_name=step["unique_name"],
                settings=step["settings"],
                depends_on=step.get("depends_on", []),
            )
            for step in steps_data
        ]

        # Create workflow
        client = DatalabClient(api_key=api_key, base_url=base_url)
        workflow = client.create_workflow(name=name, steps=workflow_steps)

        click.echo("Workflow created successfully!")
        click.echo(f"   ID: {workflow.id}")
        click.echo(f"   Name: {workflow.name}")
        click.echo(f"   Team ID: {workflow.team_id}")
        click.echo(f"   Steps: {len(workflow.steps)}")

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option("--workflow_id", required=True, type=int, help="ID of the workflow")
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def get_workflow(workflow_id: int, api_key: Optional[str], base_url: str):
    """[Deprecated: the API removed workflows] Get a workflow by ID"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        client = DatalabClient(api_key=api_key, base_url=base_url)
        workflow = client.get_workflow(workflow_id)

        click.echo("Workflow Details:")
        click.echo(f"   ID: {workflow.id}")
        click.echo(f"   Name: {workflow.name}")
        click.echo(f"   Team ID: {workflow.team_id}")
        click.echo(f"   Steps: {len(workflow.steps)}")
        click.echo(f"   Created: {workflow.created}")

        for i, step in enumerate(workflow.steps, 1):
            click.echo(f"\n   Step {i}: {step.unique_name}")
            click.echo(f"      Type: {step.step_key}")
            click.echo(f"      Settings: {json.dumps(step.settings, indent=8)}")
            if step.depends_on:
                click.echo(f"      Depends on: {', '.join(step.depends_on)}")

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def get_step_types(api_key: Optional[str], base_url: str):
    """[Deprecated: the API removed workflows] Get all available workflow step types"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        client = DatalabClient(api_key=api_key, base_url=base_url)
        response = client.get_step_types()

        step_types = response.get("step_types", [])
        if not step_types:
            click.echo("No step types found.")
            return

        click.echo(f"Found {len(step_types)} step type(s):\n")
        for step_type in step_types:
            click.echo("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            click.echo(f"Key:         {step_type.get('type')}")
            click.echo(f"Version:     {step_type.get('version')}")
            click.echo(f"Name:        {step_type.get('name')}")
            if step_type.get("description"):
                click.echo(f"Description: {step_type['description']}")

            if step_type.get("settings_schema"):
                click.echo("\nSettings Schema:")
                click.echo(json.dumps(step_type["settings_schema"], indent=2))
            click.echo("")

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def list_workflows(api_key: Optional[str], base_url: str):
    """[Deprecated: the API removed workflows] List all workflows for your team"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        client = DatalabClient(api_key=api_key, base_url=base_url)
        workflows = client.list_workflows()

        if not workflows:
            click.echo("No workflows found.")
            return

        click.echo(f"Found {len(workflows)} workflow(s):\n")
        for workflow in workflows:
            click.echo(f"   ID: {workflow.id}")
            click.echo(f"   Name: {workflow.name}")
            click.echo(f"   Team ID: {workflow.team_id}")
            click.echo(f"   Steps: {len(workflow.steps)}")
            click.echo(f"   Created: {workflow.created}")
            click.echo("")

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option("--workflow_id", required=True, type=int, help="ID of the workflow")
@click.option(
    "--input_config",
    required=True,
    help="JSON string or path to JSON file with input configuration",
)
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def execute_workflow(
    workflow_id: int,
    input_config: str,
    api_key: Optional[str],
    base_url: str,
):
    """[Deprecated: the API removed workflows] Trigger a workflow execution"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        # Parse input_config from JSON string or file
        input_path = Path(input_config)
        if input_path.exists():
            with open(input_path, "r") as f:
                config_data = json.load(f)
        else:
            config_data = json.loads(input_config)

        # Create InputConfig object
        input_cfg = InputConfig(
            file_urls=config_data.get("file_urls"),
            bucket=config_data.get("bucket"),
            prefix=config_data.get("prefix"),
            pattern=config_data.get("pattern"),
            storage_type=config_data.get("storage_type"),
        )

        client = DatalabClient(api_key=api_key, base_url=base_url)

        click.echo(f"Triggering workflow execution for workflow {workflow_id}...")
        execution = client.execute_workflow(
            workflow_id=workflow_id,
            input_config=input_cfg,
        )

        click.echo("\nSuccessfully triggered workflow execution!")
        click.echo(f"   Execution ID: {execution.id}")
        click.echo(f"   Status: {execution.status}")
        click.echo("\nTo check the status, run:")
        click.echo(f"   datalab get-execution-status --execution_id {execution.id}")
        click.echo("\n   Or poll until complete:")
        click.echo(
            f"   datalab get-execution-status --execution_id {execution.id} --max_polls 300 --poll_interval 2"
        )

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option("--execution_id", required=True, type=int, help="ID of the execution")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output file path to save execution results",
)
@click.option(
    "--max_polls",
    default=1,
    type=int,
    help="Maximum number of polling attempts (1 for single check)",
)
@click.option(
    "--poll_interval", default=1, type=int, help="Polling interval in seconds"
)
@click.option(
    "--download",
    is_flag=True,
    help="Download actual results from presigned URLs (default: just show URLs)",
)
@click.option("--api_key", required=False, help="Datalab API key")
@click.option("--base_url", default=settings.DATALAB_HOST, help="API base URL")
def get_execution_status(
    execution_id: int,
    output: Optional[str],
    max_polls: int,
    poll_interval: int,
    download: bool,
    api_key: Optional[str],
    base_url: str,
):
    """[Deprecated: the API removed workflows] Get the status of a workflow execution"""
    _echo_workflows_removed()
    try:
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable. "
                "Get a free API key at https://www.datalab.to/app/keys"
            )

        client = DatalabClient(api_key=api_key, base_url=base_url)
        execution = client.get_execution_status(
            execution_id=execution_id,
            max_polls=max_polls,
            poll_interval=poll_interval,
            download_results=download,
        )

        click.echo("Execution Status:")
        click.echo(f"   Execution ID: {execution.id}")
        click.echo(f"   Workflow ID: {execution.workflow_id}")
        click.echo(f"   Status: {execution.status}")
        click.echo(f"   Success: {execution.success}")
        click.echo(f"   Created: {execution.created}")

        if execution.steps:
            click.echo("\n   Step Results:")
            for step_name, step_data in execution.steps.items():
                click.echo(f"\n   [{step_name}]")

                # Iterate through file_ids/aggregated keys
                for file_key, file_step_data in step_data.items():
                    if isinstance(file_step_data, dict):
                        click.echo(f"\n      File/Group: {file_key}")
                        click.echo(
                            f"         Step ID: {file_step_data.get('id', 'N/A')}"
                        )
                        click.echo(
                            f"         Status: {file_step_data.get('status', 'N/A')}"
                        )

                        if file_step_data.get("started_at"):
                            click.echo(
                                f"         Started: {file_step_data.get('started_at')}"
                            )
                        if file_step_data.get("finished_at"):
                            click.echo(
                                f"         Finished: {file_step_data.get('finished_at')}"
                            )
                        if file_step_data.get("file_ids"):
                            click.echo(
                                f"         Files: {', '.join(file_step_data.get('file_ids'))}"
                            )

                        if "output_url" in file_step_data and not download:
                            click.echo(
                                f"         Output URL: {file_step_data.get('output_url')}"
                            )
                        elif download and "downloaded_data" in file_step_data:
                            click.echo(
                                f"         Results: {json.dumps(file_step_data['downloaded_data'], indent=12)}"
                            )
                    else:
                        click.echo(f"      {file_step_data}")

        # Save output if requested
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            execution.save_output(output_path)
            click.echo(f"\nResults saved to: {output_path}")

    except DatalabError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option(
    "--definition", required=True, help="Path to workflow definition JSON file"
)
def visualize_workflow(definition: str):
    """Visualize workflow DAG from a JSON definition file"""
    try:
        import json
        from pathlib import Path
        from collections import defaultdict, deque

        # Load workflow definition
        definition_path = Path(definition)
        if not definition_path.exists():
            raise DatalabError(f"Definition file not found: {definition}")

        with open(definition_path, "r") as f:
            workflow_def = json.load(f)

        name = workflow_def.get("name", "Unnamed Workflow")
        steps = workflow_def.get("steps", [])

        if not steps:
            click.echo("No steps found in workflow definition")
            return

        # Build dependency graph
        step_map = {step["unique_name"]: step for step in steps}

        click.echo(f"\n{'=' * 70}")
        click.echo(f"Workflow: {name}")
        click.echo(f"{'=' * 70}\n")

        # Build parent/child maps
        children = defaultdict(list)
        for step in steps:
            step_name = step["unique_name"]
            for dep in step.get("depends_on", []):
                children[dep].append(step_name)

        # Topological sort to determine layers
        in_degree = {
            step["unique_name"]: len(step.get("depends_on", [])) for step in steps
        }
        layers = []
        queue = deque(
            [s["unique_name"] for s in steps if in_degree[s["unique_name"]] == 0]
        )

        while queue:
            layer = []
            for _ in range(len(queue)):
                node = queue.popleft()
                layer.append(node)
                for child in children[node]:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        queue.append(child)
            layers.append(layer)

        # Render DAG as simple tree
        _render_dag_simple(layers, children, step_map)

        click.echo(f"\nTotal steps: {len(steps)}")

    except json.JSONDecodeError as e:
        click.echo(f"Invalid JSON: {e}", err=True)
        sys.exit(1)
    except KeyError as e:
        click.echo(f"Missing required field in workflow definition: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _render_dag_simple(layers, children, step_map):
    """Simple DAG rendering with topological sort"""
    for layer_idx, layer in enumerate(layers):
        # Show layer info with color
        layer_info = f"Layer {layer_idx}"
        if layer_idx == 0:
            layer_info += " (start)"
            color = "green"
        elif layer_idx == len(layers) - 1:
            layer_info += " (end)"
            color = "red"
        elif len(layer) > 1:
            color = "yellow"
        else:
            color = "cyan"

        click.echo(click.style(f"{layer_info}:", fg=color, bold=True))

        for step_name in layer:
            step = step_map[step_name]
            step_key = step.get("step_key", "unknown")
            depends = step.get("depends_on", [])

            # Show the step - color code by step type
            step_color = "blue" if "marker" in step_key else "magenta"
            click.echo(
                f"  • {click.style(step_name, fg=step_color)} ({click.style(step_key, fg='white', dim=True)})"
            )

            # Show dependencies if any
            if depends:
                click.echo(
                    click.style(
                        f"    ← depends on: {', '.join(depends)}", fg="white", dim=True
                    )
                )

        # Show what comes next
        if layer_idx < len(layers) - 1:
            click.echo(click.style("  |", fg="white", dim=True))
            click.echo(click.style("  v", fg="white", dim=True))
            click.echo()


# Add commands to CLI group
cli.add_command(login)
cli.add_command(logout)
cli.add_command(convert)
cli.add_command(extract)
cli.add_command(segment)
cli.add_command(custom_pipeline)
cli.add_command(track_changes)
cli.add_command(fill)
cli.add_command(form_fields)
cli.add_command(agent)
cli.add_command(create_document)
cli.add_command(create_workflow)
cli.add_command(get_workflow)
cli.add_command(get_step_types)
cli.add_command(list_workflows)
cli.add_command(execute_workflow)
cli.add_command(get_execution_status)
cli.add_command(visualize_workflow)

if __name__ == "__main__":
    cli()
