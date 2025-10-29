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
- **`individual_examples/`** - Individual operation scripts (create, execute, poll, etc.) so you can see how they work individually.

## Quick Start: End-to-End

The fastest way to run a complete workflow from definition to results:

```bash
# Run with any workflow definition
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/compare_segmentation.json \
    --file_url https://example.com/document.pdf \
    --save results.json

```

This single command:
1. Loads the workflow definition
2. Creates the workflow
3. Executes it with your file
4. Polls until completion
5. Displays and saves results

## Visualizing Workflows

Before running a workflow, visualize its DAG structure:

```bash
datalab visualize-workflow --definition workflow_definitions/compare_segmentation.json
```

**Output:**
```
============================================================
Workflow: Parallel Marker vs Reducto Comparison
============================================================

Total steps: 5

├── marker_parse
│       └─ step_key: marker_parse
│   └── marker_segment
│           └─ step_key: marker_segment

└── reducto_upload
        └─ step_key: api_request
    └── reducto_parse
            └─ step_key: api_request
        └── reducto_split
                └─ step_key: api_request
```

This helps you understand the workflow structure and dependencies before execution.

## Workflow Definitions

There are pre-built workflows provided in `workflow_definitions/` with more detailed instructions on what each one does, and how you can make your own, in the [README](./workflow_definitions/README.md).

### Running Workflow Samples

You can pick any defined workflow and run it end to end like this:

```bash
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/my_workflow.json \
    --file_url https://example.com/doc.pdf
```

You can optionally create just the workflow (without executing it) using the example like this:

```bash
python recipes/workflows/individual_examples/create_workflow.py \
    --definition workflow_definitions/my_workflow.json
```

Or, using the CLI directly:

```bash
datalab create-workflow \
    --name "My Workflow" \
    --steps workflow_definitions/my_workflow.json
```

## Getting Help

Reach out at support@datalab.to or create a github issue if there are any questions or bugs!
