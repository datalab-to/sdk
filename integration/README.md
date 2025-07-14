# Integration Tests

This directory contains integration tests that run against the live Datalab API.

## Setup

1. **Set your API key** as an environment variable:
   ```bash
   export DATALAB_API_KEY="your_api_key_here"
   ```

2. **Optional: Set custom base URL** if testing against a different server:
   ```bash
   export DATALAB_BASE_URL="https://custom.datalab.to"
   ```

## Running the Tests

Run all integration tests:
```bash
pytest integration/ -v
```

Run specific test classes:
```bash
pytest integration/test_live_api.py::TestMarkerIntegration -v
pytest integration/test_live_api.py::TestOCRIntegration -v
```

Run individual tests:
```bash
pytest integration/test_live_api.py::TestMarkerIntegration::test_convert_pdf_basic -v
```

## Test Coverage

### Marker/Convert Tests
- **test_convert_pdf_basic**: Basic PDF to markdown conversion
- **test_convert_office_document**: Word document to HTML conversion  
- **test_convert_async_with_json**: Async PowerPoint to JSON conversion

### OCR Tests
- **test_ocr_pdf_basic**: Basic PDF OCR with text extraction
- **test_ocr_image_file**: OCR on PNG image file
- **test_ocr_async_multiple_pages**: Async OCR with multiple pages

### Error Handling Tests
- **test_invalid_api_key**: Invalid API key handling
- **test_nonexistent_file**: Nonexistent file handling
- **test_unsupported_file_type**: Unsupported file type handling

### Save Output Tests
- **test_convert_with_save_output**: Automatic file saving for conversion
- **test_ocr_with_save_output**: Automatic file saving for OCR

## Test Data Files Used

The tests use sample files from the `data/` directory:
- `adversarial.pdf` - PDF document
- `bid_evaluation.docx` - Word document
- `08-Lambda-Calculus.pptx` - PowerPoint presentation
- `thinkpython.pdf` - PDF book
- `chi_hind.png` - Image file

## Notes

- Tests use `max_pages=1` or `max_pages=2` to keep API usage minimal
- LLM mode is disabled to avoid extra costs
- All tests require a valid API key and will be skipped if not provided
- Tests make actual API calls and will consume API credits
- Some tests may take time to complete due to processing delays