#!/usr/bin/env python3
"""
Example: Create a new workflow

This example shows how to create a workflow with multiple steps.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    python examples/create_workflow.py
"""

from datalab_sdk import DatalabClient, WorkflowStep


def main():
    # Initialize client (uses DATALAB_API_KEY environment variable)
    client = DatalabClient()

    print("🔨 Creating a new workflow...\n")

    # Define workflow steps
    # IMPORTANT: unique_name must be unique within the workflow
    steps = [
        WorkflowStep(
            step_key="marker_parse",
            unique_name="parse_document",  # Must be unique!
            settings={
                "max_pages": 10,
                "output_format": "json",
                "force_ocr": False
            },
            depends_on=[]  # No dependencies - this runs first
        ),
        WorkflowStep(
            step_key="marker_extract",
            unique_name="extract_metadata",  # Must be unique!
            settings={
                "page_schema": {
                    "title": {"type": "string"},
                    "author": {"type": "string"},
                    "date": {"type": "string"},
                    "summary": {"type": "string"}
                }
            },
            depends_on=["parse_document"]  # Depends on first step
        )
    ]

    # Create the workflow
    workflow = client.create_workflow(
        name="Document Parser with Metadata Extraction",
        steps=steps
    )

    print("✅ Workflow created successfully!\n")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"ID:         {workflow.id}")
    print(f"Name:       {workflow.name}")
    print(f"Team ID:    {workflow.team_id}")
    print(f"Steps:      {len(workflow.steps)}")
    print()

    print("Steps configured:")
    for i, step in enumerate(workflow.steps, 1):
        print(f"  {i}. {step.unique_name}")
        if step.depends_on:
            print(f"     Depends on: {', '.join(step.depends_on)}")
        print()

    print(f"💡 To execute this workflow, run:")
    print(f"   python examples/execute_workflow.py --workflow_id {workflow.id}")
    print()
    print(f"💡 Or use the CLI:")
    print(f"   datalab execute-workflow --workflow_id {workflow.id} --input_config input.json")


if __name__ == "__main__":
    main()
