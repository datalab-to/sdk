#!/usr/bin/env python3
"""
Example: Poll workflow execution status

This checks the status of a workflow execution and optionally polls until complete.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    # Single status check:
    python examples/poll_workflow.py --execution_id 123 --single

    # Poll until complete (default):
    python examples/poll_workflow.py --execution_id 123

    # Custom polling (check every 5 seconds for up to 2 minutes):
    python examples/poll_workflow.py --execution_id 123 --max_polls 24 --poll_interval 5
"""

import argparse
import json
from datalab_sdk import DatalabClient


def main():
    parser = argparse.ArgumentParser(description="Check workflow execution status")
    parser.add_argument(
        "--execution_id",
        type=int,
        required=True,
        help="ID of the execution to check"
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Single status check (don't poll)"
    )
    parser.add_argument(
        "--max_polls",
        type=int,
        default=60,
        help="Maximum number of polling attempts (default: 60)"
    )
    parser.add_argument(
        "--poll_interval",
        type=int,
        default=2,
        help="Seconds between polls (default: 2)"
    )
    parser.add_argument(
        "--save",
        help="Save results to file (e.g., results.json)"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download actual results from presigned URLs (default: just show URLs)"
    )
    args = parser.parse_args()

    # Initialize client (uses DATALAB_API_KEY environment variable)
    client = DatalabClient()

    if args.single:
        print(f"📊 Checking execution {args.execution_id} status...\n")
        max_polls = 1
    else:
        total_time = args.max_polls * args.poll_interval
        print(f"⏳ Polling execution {args.execution_id} status...")
        print(f"   Max time: {total_time} seconds ({args.max_polls} polls × {args.poll_interval}s)\n")
        max_polls = args.max_polls

    # Get execution status (with optional polling and download)
    execution = client.get_execution_status(
        execution_id=args.execution_id,
        max_polls=max_polls,
        poll_interval=args.poll_interval,
        download_results=args.download
    )

    # Display status
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"Execution ID:  {execution.id}")
    print(f"Workflow ID:   {execution.workflow_id}")
    print(f"Status:        {execution.status}")
    print(f"Success:       {execution.success}")
    print(f"Created:       {execution.created}")
    if execution.updated:
        print(f"Updated:       {execution.updated}")
    print()

    # Handle different statuses
    if execution.status == "COMPLETED":
        print("✅ Workflow completed successfully!")
        if execution.results:
            print("\n📋 Results:")
            print(json.dumps(execution.results, indent=2))

            # Save if requested
            if args.save:
                execution.save_output(args.save)
                print(f"\n💾 Results saved to: {args.save}")

    elif execution.status == "FAILED":
        print("❌ Workflow failed!")
        if execution.error:
            print(f"\nError: {execution.error}")

    elif execution.status == "IN_PROGRESS":
        print("⏱️  Workflow is still processing...")
        if not args.single:
            print(f"\n   Timed out after {args.max_polls} polls.")
            print(f"   Run again to continue checking:")
            print(f"   python examples/poll_workflow.py --execution_id {execution.id}")

    else:
        print(f"⚠️  Unknown status: {execution.status}")

    # Show next steps
    if execution.status == "IN_PROGRESS":
        print(f"\n💡 To continue polling:")
        print(f"   python examples/poll_workflow.py --execution_id {execution.id}")
        print(f"\n💡 Or use the CLI:")
        print(f"   datalab get-execution-status --execution_id {execution.id} --max_polls 60")


if __name__ == "__main__":
    main()
