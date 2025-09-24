"""
Datalab SDK Collections - Batch document processing
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import asyncio
from urllib.parse import urlparse


@dataclass
class CollectionResult:
    """Result from batch processing a collection"""
    collection_name: str
    total_files: int
    successful: int
    failed: int
    results: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]


@dataclass
class Collection:
    """A collection of documents for batch processing"""
    name: str
    sources: List[str]  # Mix of local paths, URLs, S3 URIs

    @classmethod
    def from_local_directory(
        cls,
        name: str,
        directory: Union[str, Path],
        extensions: Optional[List[str]] = None
    ) -> "Collection":
        """Create collection from local directory"""
        from datalab_sdk.mimetypes import SUPPORTED_EXTENSIONS

        directory = Path(directory)
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"Directory not found: {directory}")

        if extensions is None:
            extensions = SUPPORTED_EXTENSIONS

        # Ensure extensions start with dot
        extensions = [ext if ext.startswith('.') else f'.{ext}' for ext in extensions]

        files = []
        for file_path in directory.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in extensions:
                files.append(str(file_path))

        return cls(name=name, sources=files)

    @classmethod
    def from_s3_prefix(
        cls,
        name: str,
        s3_uri: str,
        extensions: Optional[List[str]] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: Optional[str] = None
    ) -> "Collection":
        """Create collection from S3 prefix like 's3://bucket/invoices/'"""
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
        except ImportError:
            raise ImportError("boto3 is required for S3 support. Install with: pip install boto3")

        # Parse S3 URI
        parsed = urlparse(s3_uri)
        if parsed.scheme != 's3':
            raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")

        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')

        # Setup S3 client
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        s3_client = session.client('s3')

        try:
            # List objects with prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']

                        # Filter by extensions if provided
                        if extensions:
                            file_ext = Path(key).suffix.lower()
                            extensions_normalized = [ext if ext.startswith('.') else f'.{ext}' for ext in extensions]
                            if file_ext not in extensions_normalized:
                                continue

                        files.append(f"s3://{bucket}/{key}")

        except (ClientError, NoCredentialsError) as e:
            raise ValueError(f"Failed to access S3: {e}")

        return cls(name=name, sources=files)

    @classmethod
    def from_urls(cls, name: str, file_urls: List[str]) -> "Collection":
        """Create collection from list of URLs"""
        return cls(name=name, sources=file_urls)

    @classmethod
    def from_mixed_sources(cls, name: str, sources: List[Union[str, Path]]) -> "Collection":
        """Create collection from mixed sources (local files, URLs, S3 URIs)"""
        sources_str = [str(source) for source in sources]
        return cls(name=name, sources=sources_str)

    async def convert_all(
        self,
        client,
        options: Optional["ProcessingOptions"] = None,
        output_dir: Optional[Union[str, Path]] = None,
        max_concurrent: int = 5,
        max_polls: int = 300,
        poll_interval: int = 1
    ) -> CollectionResult:
        """Convert all documents in the collection"""
        from datalab_sdk.models import ConvertOptions

        if options is None:
            options = ConvertOptions()

        return await self._process_all(
            client=client,
            method="convert",
            options=options,
            output_dir=output_dir,
            max_concurrent=max_concurrent,
            max_polls=max_polls,
            poll_interval=poll_interval
        )

    async def ocr_all(
        self,
        client,
        options: Optional["ProcessingOptions"] = None,
        output_dir: Optional[Union[str, Path]] = None,
        max_concurrent: int = 5,
        max_polls: int = 300,
        poll_interval: int = 1
    ) -> CollectionResult:
        """OCR all documents in the collection"""
        from datalab_sdk.models import OCROptions

        if options is None:
            options = OCROptions()

        return await self._process_all(
            client=client,
            method="ocr",
            options=options,
            output_dir=output_dir,
            max_concurrent=max_concurrent,
            max_polls=max_polls,
            poll_interval=poll_interval
        )

    async def _process_all(
        self,
        client,
        method: str,
        options: "ProcessingOptions",
        output_dir: Optional[Union[str, Path]] = None,
        max_concurrent: int = 5,
        max_polls: int = 300,
        poll_interval: int = 1
    ) -> CollectionResult:
        """Internal method to process all sources"""
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_single_source(source: str) -> Dict[str, Any]:
            async with semaphore:
                try:
                    # Determine if source is local file or URL
                    if source.startswith(('http://', 'https://', 's3://')):
                        file_path = None
                        file_url = source
                        # Generate output filename from URL
                        source_name = Path(urlparse(source).path).stem or "output"
                    else:
                        file_path = source
                        file_url = None
                        source_name = Path(source).stem

                    # Create output path if specified
                    save_output = None
                    if output_dir:
                        save_output = output_dir / self.name / source_name / source_name

                    # Process based on method
                    if method == "convert":
                        result = await client.convert(
                            file_path=file_path,
                            file_url=file_url,
                            options=options,
                            save_output=save_output,
                            max_polls=max_polls,
                            poll_interval=poll_interval
                        )
                    else:  # method == "ocr"
                        if file_url:
                            raise ValueError("OCR method only supports local files, not URLs")
                        result = await client.ocr(
                            file_path=file_path,
                            options=options,
                            save_output=save_output,
                            max_polls=max_polls,
                            poll_interval=poll_interval
                        )

                    return {
                        "source": source,
                        "output_path": str(save_output) if save_output else None,
                        "success": result.success,
                        "error": result.error,
                        "page_count": result.page_count,
                        "status": result.status
                    }

                except Exception as e:
                    return {
                        "source": source,
                        "output_path": None,
                        "success": False,
                        "error": str(e),
                        "page_count": None,
                        "status": "failed"
                    }

        # Process all sources concurrently
        tasks = [process_single_source(source) for source in self.sources]
        results = await asyncio.gather(*tasks)

        # Aggregate results
        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]

        return CollectionResult(
            collection_name=self.name,
            total_files=len(self.sources),
            successful=len(successful_results),
            failed=len(failed_results),
            results=results,
            errors=[{"source": r["source"], "error": r["error"]} for r in failed_results]
        )