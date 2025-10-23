#!/usr/bin/env python3
"""
Example script to test workflow functionality locally.

Before running:
1. Install SDK: pip install -e .
2. Set API key: export DATALAB_API_KEY="your_key"
"""

import os
import sys
from datalab_sdk import DatalabClient, WorkflowStep, InputConfig


FILE_URL = "https://example.com/sample.pdf"  # Replace with a real file URL


def main():
    # Check API key is set
    api_key = os.getenv("DATALAB_API_KEY")
    if not api_key:
        print("❌ Error: DATALAB_API_KEY environment variable not set")
        print("Set it with: export DATALAB_API_KEY='your_key'")
        sys.exit(1)

    print("🔧 Initializing Datalab client...")
    client = DatalabClient()

    # Step 1: List existing workflows
    print("\n" + "=" * 50)
    print("📋 STEP 1: Listing existing workflows")
    print("=" * 50)
    try:
        workflows = client.list_workflows()
        print(f"✅ Found {len(workflows)} workflow(s)")
        for w in workflows[:3]:  # Show first 3
            print(f"   - ID: {w.id}, Name: {w.name}, Steps: {len(w.steps)}")
    except Exception as e:
        print(f"❌ Error listing workflows: {e}")
        return

    # Step 2: Create a new workflow
    print("\n" + "=" * 50)
    print("🔨 STEP 2: Creating a new workflow")
    print("=" * 50)
    try:
        steps = [
            WorkflowStep(
                step_key="marker_parse",
                unique_name="parse_step",
                settings={
                    "max_pages": 5,
                    "output_format": "json"
                }
            ),
            WorkflowStep(
                step_key="marker_extract",
                unique_name="extract_step",
                settings={
                    "page_schema": {
                        "title": {"type": "string"},
                        "author": {"type": "string"}
                    }
                },
                depends_on=["parse_step"]  # Depends on first step
            )
        ]

        workflow = client.create_workflow(
            name="SDK Test Workflow",
            steps=steps
        )
        print(f"✅ Created workflow!")
        print(f"   ID: {workflow.id}")
        print(f"   Name: {workflow.name}")
        print(f"   Steps: {len(workflow.steps)}")

        workflow_id = workflow.id
    except Exception as e:
        print(f"❌ Error creating workflow: {e}")
        return

    # Step 3: Get workflow details
    print("\n" + "=" * 50)
    print("🔍 STEP 3: Getting workflow details")
    print("=" * 50)
    try:
        workflow_details = client.get_workflow(workflow_id)
        print(f"✅ Retrieved workflow {workflow_id}")
        print(f"   Name: {workflow_details.name}")
        print(f"   Created: {workflow_details.created_at}")
        print(f"   Steps:")
        for i, step in enumerate(workflow_details.steps, 1):
            print(f"      {i}. {step.unique_name} ({step.step_key})")
            if step.depends_on:
                print(f"         Depends on: {', '.join(step.depends_on)}")
    except Exception as e:
        print(f"❌ Error getting workflow: {e}")
        return

    # Step 4: Execute the workflow
    print("\n" + "=" * 50)
    print("🚀 STEP 4: Executing workflow")
    print("=" * 50)
    try:
        input_config = InputConfig(
            type="single_file",
            file_url=FILE_URL
        )

        execution = client.execute_workflow(
            workflow_id=workflow_id,
            input_config=input_config
        )
        print(f"✅ Successfully triggered execution!")
        print(f"   Execution ID: {execution.id}")
        print(f"   Status: {execution.status}")
        print(f"   Workflow ID: {execution.workflow_id}")

        execution_id = execution.id
    except Exception as e:
        print(f"❌ Error executing workflow: {e}")
        return

    # Step 5: Check execution status (single check)
    print("\n" + "=" * 50)
    print("📊 STEP 5: Checking execution status")
    print("=" * 50)
    try:
        status = client.get_execution_status(execution_id)
        print(f"✅ Retrieved status")
        print(f"   Status: {status.status}")
        print(f"   Success: {status.success}")
        if status.error:
            print(f"   Error: {status.error}")
        if status.results:
            print(f"   Results available: Yes")
    except Exception as e:
        print(f"❌ Error checking status: {e}")
        return

    # Step 6: Poll until complete (optional)
    print("\n" + "=" * 50)
    print("⏳ STEP 6: Polling for completion (max 30 seconds)")
    print("=" * 50)
    try:
        print("Polling every 2 seconds...")
        final_status = client.get_execution_status(
            execution_id=execution_id,
            max_polls=15,  # 15 polls * 2 seconds = 30 seconds max
            poll_interval=2
        )
        print(f"✅ Final status: {final_status.status}")
        print(f"   Success: {final_status.success}")
        if final_status.status == "complete":
            print(f"   🎉 Workflow completed successfully!")
            if final_status.results:
                print(f"   Results keys: {list(final_status.results.keys())}")
        elif final_status.status == "failed":
            print(f"   ❌ Workflow failed: {final_status.error}")
        else:
            print(f"   ⏱️  Still processing...")
    except Exception as e:
        print(f"❌ Error polling status: {e}")

    # Summary
    print("\n" + "=" * 50)
    print("✅ TEST COMPLETE!")
    print("=" * 50)
    print(f"Created workflow ID: {workflow_id}")
    print(f"Execution ID: {execution_id}")
    print("\nTo check status later, run:")
    print(f"  datalab get-execution-status --execution_id {execution_id}")


if __name__ == "__main__":
    main()
