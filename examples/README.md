# Workflow Examples

This directory contains focused examples for working with Datalab workflows.

## Prerequisites

```bash
# Install the SDK
pip install -e .

# Set your API key
export DATALAB_API_KEY="your_key_here"
```

Get your API key from: https://www.datalab.to/app/keys

## Examples

### 1. List Workflows

Get all workflows for your team.

```bash
python examples/get_workflows.py
```

### 2. Get Available Step Types

See what step types are available for creating workflows.

```bash
python examples/get_step_types.py
```

This shows available step types like:
- `marker_parse` - Parse documents into structured format
- `marker_extract` - Extract structured data using schemas

### 3. Create a Workflow

Create a new workflow with multiple steps.

```bash
python examples/create_workflow.py
```

**What it does:**
- Creates a workflow with 2 steps
- Step 1: Parse document with marker
- Step 2: Extract metadata (depends on step 1)

### 4. Execute a Workflow

Trigger a workflow execution (returns immediately).

```bash
python examples/execute_workflow.py \
  --workflow_id 1 \
  --file_url https://example.com/document.pdf
```

### 5. Poll Workflow Status

Check the status of a workflow execution, with optional polling until complete.

**Single status check:**
```bash
python examples/poll_workflow.py --execution_id 123 --single
```

**Poll until complete:**
```bash
python examples/poll_workflow.py --execution_id 123
```

**Custom polling (every 5 seconds, max 2 minutes):**
```bash
python examples/poll_workflow.py \
  --execution_id 123 \
  --max_polls 24 \
  --poll_interval 5
```

## Complete Workflow

Here's a typical workflow sequence:

```bash
# 1. List existing workflows
python examples/get_workflows.py

# 2. Create a new workflow
python examples/create_workflow.py
# Note the workflow ID from output

# 3. Execute the workflow
python examples/execute_workflow.py \
  --workflow_id 1 \
  --file_url https://example.com/doc.pdf
# Note the execution ID from output

# 4. Poll for completion
python examples/poll_workflow.py \
  --execution_id 123 \
  --save results.json
```

## Using the CLI

You can also use the CLI for these operations:

```bash
# List workflows
datalab list-workflows

# Create workflow
datalab create-workflow \
  --name "My Workflow" \
  --steps steps.json

# Execute workflow
datalab execute-workflow \
  --workflow_id 1 \
  --input_config input.json

# Check status
datalab get-execution-status --execution_id 123

# Poll until complete
datalab get-execution-status \
  --execution_id 123 \
  --max_polls 60 \
  --poll_interval 2
```

## Notes

- **Team ID**: Automatically determined from your API key
- **unique_name**: Each step must have a unique `unique_name` within the workflow
- **depends_on**: Reference other steps by their `unique_name`
- **Execution**: `execute_workflow` returns immediately - use `poll_workflow.py` to wait for completion
- **Status values**: `processing`, `complete`, or `failed`

## Getting Help

Reach out at support@datalab.to or create a github issue if there are any questions or bugs!