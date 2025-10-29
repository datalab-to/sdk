#!/usr/bin/env python3
"""
End-to-end workflow runner

This demonstrates a complete workflow from definition to results.
Can be used with any workflow definition file.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    # Run with a workflow definition
    python recipes/workflows/end_to_end_workflow.py \
        --definition workflow_definitions/compare_segmentation.json \
        --file_url https://example.com/document.pdf
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add SDK to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datalab_sdk import DatalabClient, WorkflowStep, InputConfig


def load_workflow_definition(definition_path: str, replacements: dict = None) -> dict:
    """Load workflow definition from JSON file with optional token replacement"""
    with open(definition_path, 'r') as f:
        workflow_def = json.load(f)

    # Apply token replacements if provided
    if replacements:
        workflow_json = json.dumps(workflow_def)
        for token, value in replacements.items():
            workflow_json = workflow_json.replace(token, value)
        workflow_def = json.loads(workflow_json)

    return workflow_def


def create_simple_workflow_definition() -> dict:
    """Create a simple default workflow definition"""
    return {
        "name": "Simple Parse and Extract",
        "steps": [
            {
                "step_key": "marker_parse",
                "unique_name": "parse_document",
                "settings": {"max_pages": 10, "output_format": "json"},
                "depends_on": []
            },
            {
                "step_key": "marker_extract",
                "unique_name": "extract_metadata",
                "settings": {
                    "page_schema": {
                        "title": {"type": "string"},
                        "author": {"type": "string"},
                        "summary": {"type": "string"}
                    }
                },
                "depends_on": ["parse_document"]
            }
        ]
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run any workflow end-to-end from definition to results"
    )
    parser.add_argument(
        "--definition",
        help="Path to workflow definition JSON file (optional - uses simple default if not provided)"
    )
    parser.add_argument(
        "--file_url",
        required=True,
        help="URL of the document to process"
    )
    parser.add_argument(
        "--replace",
        action="append",
        nargs=2,
        metavar=("TOKEN", "VALUE"),
        help="Replace tokens in definition (e.g., --replace YOUR_API_KEY abc123)"
    )
    parser.add_argument(
        "--max_polls",
        type=int,
        default=120,
        help="Maximum number of polling attempts (default: 120)"
    )
    parser.add_argument(
        "--poll_interval",
        type=int,
        default=5,
        help="Seconds between polls (default: 5)"
    )
    parser.add_argument(
        "--save",
        help="Save results to file (e.g., results.json)"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download actual results from presigned URLs when workflow execution finishes (default: just show URLs)"
    )
    args = parser.parse_args()

    # Check API key
    if not os.getenv("DATALAB_API_KEY"):
        print("❌ Error: DATALAB_API_KEY environment variable not set")
        sys.exit(1)

    client = DatalabClient()

    print(f"{'='*60}")
    print(f"End-to-End Workflow Execution")
    print(f"{'='*60}\n")

    # Step 1: Load or create workflow definition
    print("📄 Loading workflow definition...")

    if args.definition:
        # Build replacements dict
        replacements = dict(args.replace) if args.replace else None
        workflow_def = load_workflow_definition(args.definition, replacements)
        print(f"   Source: {args.definition}")
    else:
        workflow_def = create_simple_workflow_definition()
        print(f"   Source: Built-in simple workflow")

    print(f"   Name: {workflow_def['name']}")
    print(f"   Steps: {len(workflow_def['steps'])}\n")

    # Step 2: Create workflow
    print("🔨 Creating workflow...")
    steps = [
        WorkflowStep(
            step_key=step["step_key"],
            unique_name=step["unique_name"],
            settings=step["settings"],
            depends_on=step.get("depends_on", [])
        )
        for step in workflow_def["steps"]
    ]

    workflow = client.create_workflow(
        name=workflow_def["name"],
        steps=steps
    )
    print(f"✅ Workflow created (ID: {workflow.id})")
    for i, step in enumerate(workflow.steps, 1):
        deps = f" (depends on: {', '.join(step.depends_on)})" if step.depends_on else ""
        print(f"   {i}. {step.unique_name}{deps}")
    print()

    # Step 3: Execute workflow
    print(f"🚀 Executing workflow")
    print(f"   File: {args.file_url}")
    input_config = InputConfig(file_urls=[args.file_url])
    execution = client.execute_workflow(
        workflow_id=workflow.id,
        input_config=input_config
    )
    print(f"✅ Execution started (ID: {execution.id})\n")

    # Step 4: Poll for completion
    total_time = args.max_polls * args.poll_interval
    print(f"⏳ Polling for completion (max {total_time} seconds)...")

    final_execution = client.get_execution_status(
        execution_id=execution.id,
        max_polls=args.max_polls,
        poll_interval=args.poll_interval,
        download_results=args.download
    )

    # Step 5: Display results
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"Status:   {final_execution.status}")
    print(f"Success:  {final_execution.success}")
    if final_execution.created:
        print(f"Created:  {final_execution.created}")
    if final_execution.updated:
        print(f"Updated:  {final_execution.updated}")
    print()

    if final_execution.status == "COMPLETED":
        print("✅ Workflow completed successfully!\n")

        if final_execution.results:
            print("📊 Step Results:")
            for step_name, step_data in final_execution.results.items():
                status = step_data.get('status', 'N/A')
                print(f"\n  {step_name}: {status}")

                # Show key metrics
                if isinstance(step_data, dict):
                    if 'output_url' in step_data and not args.download:
                        print(f"    Output URL: {step_data['output_url']}")
                        print(f"    💡 Use --download to fetch actual results")

                    # Show common result fields
                    for key in ['segments', 'chunks', 'pages', 'document_id']:
                        if key in step_data:
                            if isinstance(step_data[key], list):
                                print(f"    {key.title()}: {len(step_data[key])} items")
                            else:
                                print(f"    {key.title()}: {step_data[key]}")

            if args.save:
                final_execution.save_output(args.save)
                print(f"\n💾 Full results saved to: {args.save}")

        print(f"\n🎉 Workflow execution complete!")

    elif final_execution.status == "FAILED":
        print("❌ Workflow failed!")
        if final_execution.error:
            print(f"\nError: {final_execution.error}")

        if final_execution.results:
            print(f"\n⚠️  Partial results available:")
            print(json.dumps(final_execution.results, indent=2))

    elif final_execution.status == "IN_PROGRESS":
        print(f"⏱️  Workflow still running (timed out after {total_time}s)")
        print(f"\nContinue polling:")
        print(f"  python recipes/workflows/workflow_api_tutorial/poll_workflow.py \\")
        print(f"      --execution_id {final_execution.id}")

    else:
        print(f"⚠️  Unknown status: {final_execution.status}")


if __name__ == "__main__":
    main()
