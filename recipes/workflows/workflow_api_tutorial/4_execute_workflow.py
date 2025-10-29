#!/usr/bin/env python3
"""
Example: Execute a workflow

This triggers a workflow execution and returns immediately with an execution ID.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    python examples/execute_workflow.py --workflow_id 1 --file_url https://example.com/doc.pdf
"""

import argparse
from datalab_sdk import DatalabClient, InputConfig


def main():
    parser = argparse.ArgumentParser(description="Execute a workflow")
    parser.add_argument(
        "--workflow_id",
        type=int,
        required=True,
        help="ID of the workflow to execute"
    )
    parser.add_argument(
        "--file_url",
        required=True,
        help="URL of the file to process"
    )
    args = parser.parse_args()

    # Initialize client (uses DATALAB_API_KEY environment variable)
    client = DatalabClient()

    print(f"🚀 Executing workflow {args.workflow_id}...\n")

    # Create input configuration
    input_config = InputConfig(
        file_urls=[args.file_url]
    )

    # Execute the workflow
    # Note: This returns immediately - it does NOT wait for completion
    execution = client.execute_workflow(
        workflow_id=args.workflow_id,
        input_config=input_config
    )

    print("✅ Workflow execution triggered successfully!\n")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"Execution ID:  {execution.id}")
    print(f"Workflow ID:   {execution.workflow_id}")
    print(f"Status:        {execution.status}")
    print(f"Created:       {execution.created}")
    print()

    print("💡 Next steps:")
    print()
    print("   1. Check execution status (single check):")
    print(f"      python examples/poll_workflow.py --execution_id {execution.id} --single")
    print()
    print("   2. Poll until complete:")
    print(f"      python examples/poll_workflow.py --execution_id {execution.id}")
    print()
    print("   3. Or use the CLI:")
    print(f"      datalab get-execution-status --execution_id {execution.id}")


if __name__ == "__main__":
    main()
