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

from datalab_sdk.client import AsyncDatalabClient
from datalab_sdk.mimetypes import SUPPORTED_EXTENSIONS
from datalab_sdk.models import OCROptions, ConvertOptions, ProcessingOptions
from datalab_sdk.exceptions import DatalabError
from datalab_sdk.settings import settings


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
    func = click.option("--skip_cache", help="Skip the cache when running inference")(
        func
    )
    func = click.option(
        "--max_polls", default=300, type=int, help="Maximum number of polling attempts"
    )(func)
    func = click.option(
        "--poll_interval", default=1, type=int, help="Polling interval in seconds"
    )(func)
    return func


def marker_options(func):
    """Options specific to marker/convert command"""
    func = click.option(
        "--format",
        "output_format",
        default="markdown",
        type=click.Choice(["markdown", "html", "json"]),
        help="Output format",
    )(func)
    func = click.option("--force_ocr", is_flag=True, help="Force OCR on every page")(
        func
    )
    func = click.option(
        "--format_lines", is_flag=True, help="Partially OCR lines for better formatting"
    )(func)
    func = click.option(
        "--paginate", is_flag=True, help="Add page delimiters to output"
    )(func)
    func = click.option("--use_llm", is_flag=True, help="Use LLM to enhance accuracy")(
        func
    )
    func = click.option(
        "--strip_existing_ocr",
        is_flag=True,
        help="Remove existing OCR text and redo OCR",
    )(func)
    func = click.option(
        "--disable_image_extraction", is_flag=True, help="Disable extraction of images"
    )(func)
    func = click.option(
        "--block_correction_prompt", help="Custom prompt for block correction"
    )(func)
    func = click.option(
        "--page_schema", help="Schema to set to do structured extraction"
    )(func)
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
                    if method == "convert":
                        result = await client.convert(
                            file_path, options=options, save_output=output_path,
                            max_polls=max_polls, poll_interval=poll_interval
                        )
                    else:  # method == 'ocr'
                        result = await client.ocr(
                            file_path, options=options, save_output=output_path,
                            max_polls=max_polls, poll_interval=poll_interval
                        )

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

    # Process all files concurrently
    tasks = [process_single_file(file_path) for file_path in files]
    results = await asyncio.gather(*tasks)

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
            click.echo(f"❌ Skipping {path}: unsupported file type", err=True)
            sys.exit(1)
        return [path]
    else:
        # Directory processing
        return find_files_in_directory(path, file_extensions)


def show_results(results: List[dict], operation: str, output_dir: Path):
    """Display processing results"""
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    click.echo(f"\n📊 {operation} Summary:")
    click.echo(f"   ✅ Successfully processed: {successful} files")
    if failed > 0:
        click.echo(f"   ❌ Failed: {failed} files")

        # Show failed files
        click.echo("\n   Failed files:")
        for result in results:
            if not result["success"]:
                click.echo(f"      - {result['file_path']}: {result['error']}")

    click.echo(f"\n📁 Output saved to: {output_dir}")


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
    force_ocr: bool = False,
    format_lines: bool = False,
    paginate: bool = False,
    use_llm: bool = False,
    strip_existing_ocr: bool = False,
    disable_image_extraction: bool = False,
    block_correction_prompt: Optional[str] = None,
    page_schema: Optional[str] = None,
):
    """Unified document processing function"""
    try:
        # Validate inputs
        if api_key is None:
            api_key = settings.DATALAB_API_KEY

        if api_key is None:
            raise DatalabError(
                "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable."
            )

        if base_url is None:
            base_url = settings.DATALAB_HOST

        output_dir = setup_output_directory(output_dir)
        file_extensions = parse_extensions(extensions)

        # Get files to process
        path = Path(path)
        to_process = get_files_to_process(path, file_extensions)

        if not to_process:
            click.echo(f"❌ No supported files found in {path}", err=True)
            sys.exit(1)

        click.echo(f"📂 Found {len(to_process)} files to process")

        # Create processing options based on method
        if method == "convert":
            options = ConvertOptions(
                output_format=output_format,
                max_pages=max_pages,
                force_ocr=force_ocr,
                format_lines=format_lines,
                paginate=paginate,
                use_llm=use_llm,
                strip_existing_ocr=strip_existing_ocr,
                disable_image_extraction=disable_image_extraction,
                page_range=page_range,
                block_correction_prompt=block_correction_prompt,
                skip_cache=skip_cache,
                page_schema=page_schema,
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
                output_dir,
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
        operation = "Conversion" if method == "convert" else "OCR"
        show_results(results, operation, output_dir)

    except DatalabError as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@click.group()
@click.version_option(version=settings.VERSION)
def cli():
    pass


@click.command()
@click.argument("path", type=click.Path(exists=True))
@common_options
@marker_options
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
    force_ocr: bool,
    format_lines: bool,
    paginate: bool,
    use_llm: bool,
    strip_existing_ocr: bool,
    disable_image_extraction: bool,
    block_correction_prompt: Optional[str],
    page_schema: Optional[str],
):
    """Convert documents to markdown, HTML, or JSON"""
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
        output_format=output_format,
        force_ocr=force_ocr,
        format_lines=format_lines,
        paginate=paginate,
        use_llm=use_llm,
        strip_existing_ocr=strip_existing_ocr,
        disable_image_extraction=disable_image_extraction,
        block_correction_prompt=block_correction_prompt,
        page_schema=page_schema,
    )


@click.command()
@click.argument("path", type=click.Path(exists=True))
@common_options
def ocr(
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
):
    """Perform OCR on documents"""
    process_documents(
        path=path,
        method="ocr",
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
    )


# Add commands to CLI group
cli.add_command(convert)
cli.add_command(ocr)

if __name__ == "__main__":
    cli()
