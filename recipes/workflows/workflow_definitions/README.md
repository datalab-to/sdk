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

## Available Workflow Definitions

### Eval Segmentation

**What it does:**
Lets you pass in one or more documents into two parallel flows, one that does `marker_parse` -> `marker_segment`, and another that uses our `api_request` step to make authenticated API calls to an external vendor you might be evaluating (Reducto, etc.) to do something similar.

Once you get results, you can process them to run your own custom evaluations.

**Structure:**
- **Marker branch**: Parse → Segment
- **Reducto branch**: Upload → Parse → Split (runs in parallel)

**Visualize:**
```bash
datalab visualize-workflow --definition workflow_definitions/eval_segmentation.json
```

**Execute:**
```bash
# Using end-to-end runner
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/eval_segmentation.json \
    --file_url https://example.com/doc.pdf \
    --replace YOUR_REDUCTO_API_KEY your_key_here \
    --save results.json

# Or step-by-step
python recipes/workflows/individual_examples/create_workflow.py \
    --definition workflow_definitions/eval_segmentation.json \
    --replace YOUR_REDUCTO_API_KEY your_key_here
```

**Required tokens:**
- `YOUR_REDUCTO_API_KEY` - Your Reducto API key

---

### Slack Alert Workflow

**What it does:**
Complete pipeline that parses documents, segments into sections, extracts structured data from multiple segments in parallel and then fires off a Slack alert. You can modify this to trigger review based alerts in Slack, or job completion notifications, depending on your use case.

**Structure:**
1. **Parse** - Parse document with Marker
2. **Segment** - Segment into Item 4, Item 5, and Item 16E sections
3. **Extract (parallel)** - Extract data from each segment:
   - `extract_item4` - Key products with sales data
   - `extract_item5` - Phase 3 compounds
   - `extract_item16e` - Share repurchase info
5. **Post to Slack** - Send notification with results

**Visualize:**
```bash
datalab visualize-workflow --definition workflow_definitions/slack_alert.json
```

**Execute:**
```bash
# Using end-to-end runner with multiple files
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/slack_alert.json \
    --file_url https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-form-20-f-2023.pdf \
    --replace YOUR_SLACK_BOT_TOKEN xoxb-your-token \
    --replace YOUR_SLACK_CHANNEL_ID <YOUR_CHANNEL_ID> \
    --save results.json

# Or step-by-step
python recipes/workflows/individual_examples/create_workflow.py \
    --definition workflow_definitions/slack_alert.json \
    --replace YOUR_SLACK_BOT_TOKEN xoxb-your-token \
    --replace YOUR_SLACK_CHANNEL_ID <YOUR_CHANNEL_ID>
```

**Required tokens:**
- `YOUR_SLACK_BOT_TOKEN` - Your Slack bot token (starts with `xoxb-`)
- `YOUR_SLACK_CHANNEL_ID` - Slack channel ID (e.g., `<YOUR_CHANNEL_ID>`)

**Note:** This workflow processes multiple documents in batch. You can pass multiple `--file_url` arguments or use bucket enumeration.

## Creating Your Own Workflows

1. Create a new JSON file in this directory
2. Define your workflow steps with appropriate `step_key`, `unique_name`, and `settings`
3. Use `depends_on` to specify dependencies between steps
4. Create a corresponding execution script in `../examples/`

## Token Replacement

Workflow definitions can include placeholder tokens (e.g., `YOUR_API_KEY`) that get replaced at runtime by the execution script. This allows you to:
- Keep sensitive data out of version control
- Share workflow definitions without exposing credentials
- Configure the same workflow for different environments
