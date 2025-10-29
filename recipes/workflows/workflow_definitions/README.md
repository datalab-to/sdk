# Workflow Definitions

This directory contains JSON workflow definitions that can be loaded and executed by the example scripts.

## Available Workflows

- [Parse and Segment (Simple)](#parse-and-segment-simple) - Do a straightforward parse -> segment to run one or more files through
- [Eval Segmentation Across Providers](#eval-segmentation-across-providers) - Compare Marker vs Reducto segmentation in parallel
- [Parallel Extract Large SEC Filings](#parallel-extract-large-sec-filings) - Parse → Segment → Extract from multiple sections in parallel
- [Slack Alert Workflow](#slack-alert-workflow) - Full pipeline with parallel extraction, aggregation, and Slack notification

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

### Parse and Segment (Simple)

**What it does:**
Simple workflow that does `marker_parse` -> `marker_segment`. You can pass in one or more `file_urls` when triggering your workflow.

Once you get results, you can process them to run your own custom evaluations.

**Structure:**
- **Marker branch**: Parse → Segment

**Visualize:**
```bash
datalab visualize-workflow --definition recipes/workflows/workflow_definitions/parse_segment.json
```

**Execute:**
```bash
# Using end-to-end runner
python recipes/workflows/end_to_end_workflow.py \
    --definition recipes/workflows/workflow_definitions/parse_segment.json \
    --file_url https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-form-20-f-2023.pdf

# Or step-by-step
python recipes/workflows/workflow_api_tutorial/create_workflow.py \
    --definition recipes/workflows/workflow_definitions/parse_segment.json
```

---

### Eval Segmentation Across Providers

**What it does:**
Lets you pass in one or more documents into two parallel flows, one that does `marker_parse` -> `marker_segment`, and another that uses our `api_request` step to make authenticated API calls to an external vendor you might be evaluating (Reducto, etc.) to do something similar.

Once you get results, you can process them to run your own custom evaluations.

**Structure:**
- **Marker branch**: Parse → Segment
- **Reducto branch**: Upload → Parse → Split (runs in parallel)

**Visualize:**
```bash
datalab visualize-workflow --definition recipes/workflows/workflow_definitions/eval_segmentation.json
```

**Execute:**
```bash
# Using end-to-end runner
python recipes/workflows/end_to_end_workflow.py \
    --definition recipes/workflows/workflow_definitions/eval_segmentation.json \
    --file_url https://example.com/doc.pdf \
    --replace YOUR_REDUCTO_API_KEY your_key_here \
    --save results.json

# Or step-by-step
python recipes/workflows/workflow_api_tutorial/create_workflow.py \
    --definition recipes/workflows/workflow_definitions/eval_segmentation.json \
    --replace YOUR_REDUCTO_API_KEY your_key_here
```

**Required tokens:**
- `YOUR_REDUCTO_API_KEY` - Your Reducto API key

---

### Parallel Extract Large SEC Filings

**What it does:**
Takes in 20-F filings, uses segmentation to segment the document by sections provided in the table of contents, and then for a few specific segments of interest, does parallel extractions that are specific to each segment.

Without this, you might have a long, dense schema that applies on the entire document, which is slow and error prone. This lets you optimize your extraction schemas for accuracy and speed in a scalable way.

**Structure:**
1. **Parse** - Parse document with Marker
2. **Segment** - Segment into Item 4, Item 5, and Item 16E sections
3. **Extract (parallel)** - Extract data from each segment:
   - `extract_item4` - Key products with sales data
   - `extract_item5` - Phase 3 compounds
   - `extract_item16e` - Share repurchase info

**Visualize:**
```bash
datalab visualize-workflow --definition recipes/workflows/workflow_definitions/segment_parallel_extract.json
```

**Execute:**
```bash
# Using end-to-end runner
python recipes/workflows/end_to_end_workflow.py \
    --definition recipes/workflows/workflow_definitions/segment_parallel_extract.json \
    --file_url https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-form-20-f-2023.pdf

# Or step-by-step
python recipes/workflows/workflow_api_tutorial/create_workflow.py \
    --definition recipes/workflows/workflow_definitions/segment_parallel_extract.json
```

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
4. **Post to Slack** - Send notification with results

**Visualize:**
```bash
datalab visualize-workflow --definition recipes/workflows/workflow_definitions/slack_alert.json
```

**Execute:**
```bash
# Using end-to-end runner with multiple files
python recipes/workflows/end_to_end_workflow.py \
    --definition recipes/workflows/workflow_definitions/slack_alert.json \
    --file_url https://www.novonordisk.com/content/dam/nncorp/global/en/investors/irmaterial/annual_report/2024/novo-nordisk-form-20-f-2023.pdf \
    --replace YOUR_SLACK_BOT_TOKEN xoxb-your-token \
    --replace YOUR_SLACK_CHANNEL_ID <YOUR_CHANNEL_ID> \
    --save results.json

# Or step-by-step
python recipes/workflows/workflow_api_tutorial/create_workflow.py \
    --definition recipes/workflows/workflow_definitions/slack_alert.json \
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
