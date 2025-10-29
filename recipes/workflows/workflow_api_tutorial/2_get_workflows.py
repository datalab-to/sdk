#!/usr/bin/env python3
"""
Example: List all workflows for your team

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    python examples/get_workflows.py
"""

from datalab_sdk import DatalabClient


def main():
    # Initialize client (uses DATALAB_API_KEY environment variable)
    client = DatalabClient()

    # List all workflows for your team
    print("📋 Fetching workflows...\n")
    workflows = client.list_workflows()

    if not workflows:
        print("No workflows found.")
        return

    print(f"Found {len(workflows)} workflow(s):\n")

    for workflow in workflows:
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"ID:         {workflow.id}")
        print(f"Name:       {workflow.name}")
        print(f"Team ID:    {workflow.team_id}")
        print(f"Created:    {workflow.created}")
        print(f"Steps:      {len(workflow.steps)}")

        if workflow.steps:
            print("\nSteps:")
            for i, step in enumerate(workflow.steps, 1):
                print(f"  {i}. {step.unique_name} ({step.step_key})")
                if step.depends_on:
                    print(f"     → Depends on: {', '.join(step.depends_on)}")
        print()


if __name__ == "__main__":
    main()
