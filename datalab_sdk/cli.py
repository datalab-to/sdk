#!/usr/bin/env python3
"""
Datalab SDK Command Line Interface
"""

import sys
from pathlib import Path
from typing import Optional
import click

from datalab_sdk.client import DatalabClient
from datalab_sdk.models import ProcessingOptions
from datalab_sdk.exceptions import DatalabError
from datalab_sdk.settings import settings


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Datalab SDK - Command line interface for document processing"""
    pass


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--api-key", required=False, help="Datalab API key")
@click.option(
    "--output-dir", "-o", required=False, type=click.Path(), help="Output directory"
)
@click.option(
    "--format",
    "output_format",
    default="markdown",
    type=click.Choice(["markdown", "html", "json"]),
    help="Output format",
)
@click.option("--max-pages", type=int, help="Maximum number of pages to process")
@click.option("--force-ocr", is_flag=True, help="Force OCR on every page")
@click.option(
    "--format-lines", is_flag=True, help="Partially OCR lines for better formatting"
)
@click.option("--paginate", is_flag=True, help="Add page delimiters to output")
@click.option("--use-llm", is_flag=True, help="Use LLM to enhance accuracy")
@click.option("--page-range", help='Page range to process (e.g., "0-2" or "0,1,2")')
@click.option(
    "--extensions", help="Comma-separated list of file extensions (for directories)"
)
@click.option(
    "--max-concurrent", default=5, type=int, help="Maximum concurrent requests"
)
@click.option("--base-url", default=settings.DATALAB_API_KEY, help="API base URL")
def convert(
    path: str,
    api_key: str,
    output_dir: str,
    output_format: str,
    max_pages: Optional[int],
    force_ocr: bool,
    format_lines: bool,
    paginate: bool,
    use_llm: bool,
    page_range: Optional[str],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
):
    """Convert documents to markdown, HTML, or JSON"""

    if api_key is None:
        api_key = settings.DATALAB_API_KEY
    if api_key is None:
        raise DatalabError(
            "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable."
        )

    path = Path(path)

    if output_dir is None:
        output_dir = path.parent

    output_dir = Path(output_dir)

    # Parse extensions
    file_extensions = None
    if extensions:
        file_extensions = [ext.strip() for ext in extensions.split(",")]
        file_extensions = [
            ext if ext.startswith(".") else f".{ext}" for ext in file_extensions
        ]

    # Create processing options
    options = ProcessingOptions(
        output_format=output_format,
        max_pages=max_pages,
        force_ocr=force_ocr,
        format_lines=format_lines,
        paginate=paginate,
        use_llm=use_llm,
        page_range=page_range,
    )

    try:
        client = DatalabClient(api_key, base_url)
        results = client.process(
            path,
            output_dir,
            "/api/v1/marker",
            options,
            max_pages,
            file_extensions,
            max_concurrent,
        )

        # Handle results
        if isinstance(results, dict):
            # Single file
            if results["success"]:
                click.echo(f"✅ Successfully converted {results['file_path']}")
            else:
                click.echo(
                    f"❌ Failed to convert {results['file_path']}: {results['error']}",
                    err=True,
                )
                sys.exit(1)
        else:
            # Multiple files
            successful = sum(1 for r in results if r["success"])
            failed = len(results) - successful

            click.echo("\n📊 Conversion Summary:")
            click.echo(f"   ✅ Successfully converted: {successful} files")
            if failed > 0:
                click.echo(f"   ❌ Failed: {failed} files")

                # Show failed files
                click.echo("\n   Failed files:")
                for result in results:
                    if not result["success"]:
                        click.echo(f"      - {result['file_path']}: {result['error']}")

            click.echo(f"\n📁 Output saved to: {output_dir}")

    except DatalabError as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--api-key", required=False, help="Datalab API key")
@click.option(
    "--output-dir", "-o", required=False, type=click.Path(), help="Output directory"
)
@click.option("--max-pages", type=int, help="Maximum number of pages to process")
@click.option(
    "--extensions", help="Comma-separated list of file extensions (for directories)"
)
@click.option(
    "--max-concurrent", default=5, type=int, help="Maximum concurrent requests"
)
@click.option("--base-url", default=settings.DATALAB_API_KEY, help="API base URL")
def ocr(
    path: str,
    api_key: str,
    output_dir: str,
    max_pages: Optional[int],
    extensions: Optional[str],
    max_concurrent: int,
    base_url: str,
):
    """Perform OCR on documents"""

    if api_key is None:
        api_key = settings.DATALAB_API_KEY
    if api_key is None:
        raise DatalabError(
            "You must either pass in an api key via --api_key or set the DATALAB_API_KEY env variable."
        )

    path = Path(path)

    if output_dir is None:
        output_dir = path.parent

    output_dir = Path(output_dir)

    # Parse extensions
    file_extensions = None
    if extensions:
        file_extensions = [ext.strip() for ext in extensions.split(",")]
        file_extensions = [
            ext if ext.startswith(".") else f".{ext}" for ext in file_extensions
        ]

    try:
        client = DatalabClient(api_key, base_url)
        results = client.process(
            path,
            output_dir,
            "/api/v1/ocr",
            None,
            max_pages,
            file_extensions,
            max_concurrent,
        )

        # Handle results (same pattern as convert)
        if isinstance(results, dict):
            if results["success"]:
                click.echo(f"✅ Successfully performed OCR on {results['file_path']}")
            else:
                click.echo(
                    f"❌ Failed OCR on {results['file_path']}: {results['error']}",
                    err=True,
                )
                sys.exit(1)
        else:
            successful = sum(1 for r in results if r["success"])
            failed = len(results) - successful

            click.echo("\n📊 OCR Summary:")
            click.echo(f"   ✅ Successfully processed: {successful} files")
            if failed > 0:
                click.echo(f"   ❌ Failed: {failed} files")

    except DatalabError as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


# Add commands to CLI group
cli.add_command(convert)
cli.add_command(ocr)


if __name__ == "__main__":
    cli()
