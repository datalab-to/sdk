# Workflow Recipes & Examples

This directory contains example Workflow definitions and code samples to help you make the most of Datalab's [Workflow functionality](https://documentation.datalab.to/docs/recipes/workflows/workflow-concepts).

The purpose is to give you an intuition around integrating Workflows into your own code, with realistic examples on Workflow definitions you might run into. For any requests, feedback, or questions, reach out to us anytime at support@datalab.to

## Prerequisites

```bash
# Install the SDK
pip install -e .

# Set your API key
export DATALAB_API_KEY="your_key_here"
```

Get your API key from: https://www.datalab.to/app/keys

## Directory Structure

- **`workflow_definitions/`** - JSON workflow definitions (reusable recipes). The [`README file within`](./workflow_Definitions/README.md) explains what each definition is to give you ideas on making your own.
- **`end_to_end_workflow.py`** - Generic runner for any workflow definition that handles the full cycle of creating a workflow, executing it, and polling for completion.
- **`workflow_api_tutorial/`** - Individual operation scripts (create, execute, poll, etc.) so you can see how they work individually.

## Visualizing Workflows

Before running a workflow, you can visualize its structure:

```bash
datalab visualize-workflow --definition recipes/workflows/workflow_definitions/segment_parallel_extract.json
```

**Output:**
```
======================================================================
Workflow: Parse, Segment, and Parallel Extract
======================================================================

Layer 0 (start):
  • parse (marker_parse)
  |
  v

Layer 1:
  • segment (marker_segment)
    ← depends on: parse
  |
  v

Layer 2 (end):
  • extract_item4 (marker_extract)
    ← depends on: segment
  • extract_item5 (marker_extract)
    ← depends on: segment
  • extract_item16e (marker_extract)
    ← depends on: segment
```

This helps you understand the workflow and dependencies before execution. If you're making a custom workflow, this can come in handy to sanity check since those JSON files can quickly get complicated!

## Workflow Definitions

We've provided pre-built workflows in `workflow_definitions/` with more detailed instructions on what each one does, and how you can make your own, in the [README](./workflow_definitions/README.md).

### Running Workflow Samples

You can pick any workflow and run it end to end like this:

```bash
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/my_workflow.json \
    --file_url https://example.com/doc.pdf
```
This script:
1. Loads the workflow definition
2. Creates the workflow
3. Executes it with your file
4. Polls until completion
5. Displays and saves results

You can optionally create just the workflow (without executing it) using the example script (which shows you how to integrate it into your own python code) like this:

```bash
python recipes/workflows/workflow_api_tutorial/3_create_workflow.py \
    --definition workflow_definitions/my_workflow.json
```

Or, just use the CLI directly:

```bash
datalab create-workflow \
    --name "My Workflow" \
    --steps workflow_definitions/my_workflow.json
```

The scripts in `workflow_api_tutorial/` are code samples that will help you integrate each operation into your own python code.

## Getting Help

Reach out at support@datalab.to or create a github issue if there are any questions or bugs!
