# Workflow Definitions

This directory contains JSON workflow definitions that can be loaded and executed by the example scripts.

## Structure

Each workflow definition is a JSON file with the following structure:

```json
{
  "name": "Workflow Name",
  "description": "Optional description",
  "steps": [
    {
      "step_key": "step_type",
      "unique_name": "unique_identifier",
      "settings": {
        // Step-specific configuration
      },
      "depends_on": ["other_step_name"]
    }
  ]
}
```

For a full list of `settings` to use for `marker` related steps, visit our [API reference](https://documentation.datalab.to/api-reference/list-step-types).

## Existing Sample Workflows

### compare_segmentation.json

Compares Marker and Reducto segmentation in parallel:
- **Marker branch**: Parse → Segment
- **Reducto branch**: Upload → Parse → Split

**Required tokens:**
- `YOUR_REDUCTO_API_KEY` - Replace with your Reducto API key

**Usage:**
```bash
python recipes/workflows/examples/eval_workflow_compare_segmentation.py \
    --file_url https://example.com/doc.pdf
```

## Creating New Workflows

1. Create a new JSON file in this directory
2. Define your workflow steps with appropriate `step_key`, `unique_name`, and `settings`
3. Use `depends_on` to specify dependencies between steps
4. Create a corresponding execution script in `../examples/`

## Token Replacement

Workflow definitions can include placeholder tokens (e.g., `YOUR_API_KEY`) that get replaced at runtime by the execution script. This allows you to:
- Keep sensitive data out of version control
- Share workflow definitions without exposing credentials
- Configure the same workflow for different environments
