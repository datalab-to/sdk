#!/usr/bin/env python3
"""
Tutorial: Create a new workflow

This example shows how to create a workflow from a JSON definition file.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    # Use a workflow definition file
    python recipes/workflows/tutorial/create_workflow.py \
        --definition ../workflow_definitions/compare_segmentation.json

    # Or create a simple hardcoded workflow
    python recipes/workflows/tutorial/create_workflow.py
"""

import argparse
import json
import sys
from pathlib import Path

# Add SDK to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from datalab_sdk import DatalabClient, WorkflowStep


def load_workflow_definition(definition_path: str) -> dict:
    """Load workflow definition from JSON file"""
    with open(definition_path, 'r') as f:
        return json.load(f)


def create_workflow_from_definition(client: DatalabClient, workflow_def: dict):
    """Create workflow from definition dictionary"""
    steps = [
        WorkflowStep(
            step_key=step["step_key"],
            unique_name=step["unique_name"],
            settings=step["settings"],
            depends_on=step.get("depends_on", [])
        )
        for step in workflow_def["steps"]
    ]

    return client.create_workflow(
        name=workflow_def["name"],
        steps=steps
    )


def create_simple_workflow(client: DatalabClient):
    """Create a simple hardcoded workflow for demo purposes"""
    steps = [
        WorkflowStep(
            step_key="marker_parse",
            unique_name="parse_document",
            settings={
                "max_pages": 10,
                "output_format": "json",
                "force_ocr": False
            },
            depends_on=[]
        ),
        WorkflowStep(
            step_key="marker_extract",
            unique_name="extract_metadata",
            settings={
                "page_schema": {
                    "title": {"type": "string"},
                    "author": {"type": "string"},
                    "date": {"type": "string"},
                    "summary": {"type": "string"}
                }
            },
            depends_on=["parse_document"]
        )
    ]

    return client.create_workflow(
        name="Document Parser with Metadata Extraction",
        steps=steps
    )


def main():
    parser = argparse.ArgumentParser(
        description="Create a workflow from JSON definition or hardcoded example"
    )
    parser.add_argument(
        "--definition",
        help="Path to workflow definition JSON file"
    )
    parser.add_argument(
        "--replace",
        action="append",
        nargs=2,
        metavar=("TOKEN", "VALUE"),
        help="Replace tokens in definition (e.g., --replace YOUR_API_KEY abc123)"
    )
    args = parser.parse_args()

    # Initialize client
    client = DatalabClient()

    print("🔨 Creating workflow...\n")

    # Create workflow from definition or use simple example
    if args.definition:
        workflow_def = load_workflow_definition(args.definition)

        # Apply token replacements if provided
        if args.replace:
            workflow_json = json.dumps(workflow_def)
            for token, value in args.replace:
                workflow_json = workflow_json.replace(token, value)
            workflow_def = json.loads(workflow_json)

        print(f"📄 Using definition: {args.definition}")
        print(f"   Name: {workflow_def['name']}")
        print(f"   Steps: {len(workflow_def['steps'])}")
        print()

        workflow = create_workflow_from_definition(client, workflow_def)
    else:
        print("📄 Using hardcoded simple workflow")
        print("   (Use --definition to load from JSON file)")
        print()
        workflow = create_simple_workflow(client)

    # Display results
    print("✅ Workflow created successfully!\n")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"ID:         {workflow.id}")
    print(f"Name:       {workflow.name}")
    print(f"Team ID:    {workflow.team_id}")
    print(f"Steps:      {len(workflow.steps)}")
    if workflow.created:
        print(f"Created:    {workflow.created}")
    print()

    print("Steps configured:")
    for i, step in enumerate(workflow.steps, 1):
        print(f"  {i}. {step.unique_name} ({step.step_key})")
        if step.depends_on:
            print(f"     Depends on: {', '.join(step.depends_on)}")
    print()

    print(f"💡 Next steps:")
    print(f"   1. Execute this workflow:")
    print(f"      python recipes/workflows/tutorial/execute_workflow.py \\")
    print(f"          --workflow_id {workflow.id} \\")
    print(f"          --file_url https://example.com/doc.pdf")
    print()
    print(f"   2. Or use the CLI:")
    print(f"      datalab execute-workflow --workflow_id {workflow.id} \\")
    json_example = '{"file_urls": ["https://example.com/doc.pdf"]}'
    print(f"          --input_config '{json_example}'")


if __name__ == "__main__":
    main()
