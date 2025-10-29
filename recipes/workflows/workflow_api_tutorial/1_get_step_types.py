#!/usr/bin/env python3
"""
Example: Get available workflow step types

This fetches and displays all available step types from the API.

Before running:
    export DATALAB_API_KEY="your_key"

Usage:
    python examples/get_step_types.py
"""

import asyncio
import json
from datalab_sdk import AsyncDatalabClient


async def fetch_step_types():
    """Fetch step types from the API"""
    async with AsyncDatalabClient() as client:
        response = await client._make_request("GET", "/api/v1/workflows/step-types")
        return response


def main():
    print("🔍 Fetching available step types from API...\n")

    try:
        response = asyncio.run(fetch_step_types())

        if "step_types" in response:
            step_types = response["step_types"]
            print(f"Found {len(step_types)} step type(s):\n")

            for step_type in step_types:
                print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                print(f"Key:         {step_type.get('type')}")
                print(f"Version:         {step_type.get('version')}")
                print(f"Name:        {step_type.get('name')}")
                if step_type.get('description'):
                    print(f"Description: {step_type['description']}")

                if step_type.get('settings_schema'):
                    print(f"\nSettings Schema:")
                    print(json.dumps(step_type['settings_schema'], indent=2))
                print()
        else:
            print("⚠️  No step types returned from API")
            print(f"Response: {json.dumps(response, indent=2)}")

    except Exception as e:
        print(f"❌ Error fetching step types: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
