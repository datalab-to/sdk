# Individual Examples - Python Integration Reference

These scripts demonstrate how to integrate Datalab workflows into your Python applications.

**When to use these examples:**
- Building Python applications that use workflows
- Automating workflow operations in scripts  
- Understanding SDK API patterns and return types

**When to use CLI:**
- Quick one-off operations
- Testing workflows manually
- Shell scripting

## Examples

### 1. Get Available Step Types

See what step types you can use in workflows:

**Python SDK:**
```bash
python recipes/workflows/individual_examples/get_step_types.py
```

**CLI equivalent:**
```bash
# Note: CLI doesn't have a dedicated command for this
# Use the Python example or check the API documentation
```

### 2. List Existing Workflows

View all workflows for your team:

**Python SDK:**
```bash
python recipes/workflows/individual_examples/get_workflows.py
```

Shows how to iterate through workflows and access their properties programmatically.

**CLI equivalent:**
```bash
datalab list-workflows
```

### 3. Create a Workflow

Create from a JSON definition or use a simple hardcoded example:

**Python SDK:**
```bash
# From JSON definition with token replacement
python recipes/workflows/individual_examples/create_workflow.py \
    --definition workflow_definitions/compare_segmentation.json

# Or use built-in simple workflow
python recipes/workflows/individual_examples/create_workflow.py
```

Shows how to load JSON, create WorkflowStep objects, and handle the workflow creation response.

**CLI equivalent:**
```bash
datalab create-workflow \
    --name "My Workflow" \
    --steps workflow_definitions/compare_segmentation.json
```

### 4. Execute a Workflow

Trigger a workflow execution (returns immediately):

**Python SDK:**
```bash
python recipes/workflows/individual_examples/execute_workflow.py \
    --workflow_id 1 \
    --file_url https://example.com/document.pdf
```

Shows how to create InputConfig and handle the execution response.

**CLI equivalent:**
```bash
datalab execute-workflow \
    --workflow_id 1 \
    --input_config '{"file_urls": ["https://example.com/document.pdf"]}'
```

### 5. Poll for Results

Check execution status with optional polling:

**Python SDK:**
```bash
# Single status check
python recipes/workflows/individual_examples/poll_workflow.py \
    --execution_id 123 \
    --single

# Poll until complete with result download
python recipes/workflows/individual_examples/poll_workflow.py \
    --execution_id 123 \
    --download

# Custom polling interval
python recipes/workflows/individual_examples/poll_workflow.py \
    --execution_id 123 \
    --max_polls 60 \
    --poll_interval 5
```

Shows how to access execution status, results, and step-level data programmatically.

**CLI equivalent:**
```bash
# Single status check
datalab get-execution-status --execution_id 123

# Poll until complete
datalab get-execution-status \
    --execution_id 123 \
    --max_polls 60 \
    --poll_interval 2

# Download actual results from URLs
datalab get-execution-status \
    --execution_id 123 \
    --download
```

## Complete Workflow Example

For a complete end-to-end workflow that chains all these operations together, see:

```bash
python recipes/workflows/end_to_end_workflow.py \
    --definition workflow_definitions/compare_segmentation.json \
    --file_url https://example.com/document.pdf \
```

This demonstrates the full integration pattern in a single script.
