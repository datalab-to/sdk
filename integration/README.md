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

Set `-n 4` to run 4 parallel test workers.