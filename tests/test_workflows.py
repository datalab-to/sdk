"""
Tests for workflow functionality
"""

import pytest
from unittest.mock import patch, AsyncMock

from datalab_sdk import DatalabClient, AsyncDatalabClient
from datalab_sdk.models import Workflow, WorkflowStep, WorkflowExecution, InputConfig
from datalab_sdk.exceptions import DatalabAPIError


class TestWorkflowMethods:
    """Test workflow-related methods"""

    @pytest.mark.asyncio
    async def test_create_workflow_success(self):
        """Test successful workflow creation"""
        # Create workflow steps
        steps = [
            WorkflowStep(
                step_key="marker_parse",
                unique_name="marker_parse",
                settings={"max_pages": 10},
            ),
            WorkflowStep(
                step_key="marker_extract",
                unique_name="marker_extract",
                settings={
                    "page_schema": {"title": {"type": "string"}, "summary": {"type": "string"}}
                },
                depends_on=["marker_parse"],
            ),
        ]

        # Mock API response
        mock_response = {
            "id": 1,
            "name": "Test Workflow",
            "team_id": 12,
            "steps": [
                {
                    "step_key": "marker_parse",
                    "unique_name": "marker_parse",
                    "settings": {"max_pages": 10},
                    "depends_on": [],
                },
                {
                    "step_key": "marker_extract",
                    "unique_name": "marker_extract",
                    "settings": {
                        "page_schema": {
                            "title": {"type": "string"},
                            "summary": {"type": "string"},
                        }
                    },
                    "depends_on": ["marker_parse"],
                },
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_response

                # Create workflow
                workflow = await client.create_workflow(
                    name="Test Workflow", steps=steps
                )

                # Verify result
                assert isinstance(workflow, Workflow)
                assert workflow.id == 1
                assert workflow.name == "Test Workflow"
                assert workflow.team_id == 12
                assert len(workflow.steps) == 2
                assert workflow.steps[0].step_key == "marker_parse"
                assert workflow.steps[1].depends_on == ["marker_parse"]

    @pytest.mark.asyncio
    async def test_get_workflow_success(self):
        """Test retrieving a workflow by ID"""
        mock_response = {
            "id": 1,
            "name": "Test Workflow",
            "team_id": 12,
            "steps": [
                {
                    "step_key": "marker_parse",
                    "unique_name": "marker_parse",
                    "settings": {"max_pages": 10},
                    "depends_on": [],
                },
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_response

                workflow = await client.get_workflow(1)

                # Verify result
                assert isinstance(workflow, Workflow)
                assert workflow.id == 1
                assert workflow.name == "Test Workflow"
                assert len(workflow.steps) == 1

    @pytest.mark.asyncio
    async def test_list_workflows_success(self):
        """Test listing workflows"""
        mock_response = {
            "workflows": [
                {
                    "id": 1,
                    "name": "Test Workflow 1",
                    "team_id": 12,
                    "steps": [
                        {
                            "step_key": "marker_parse",
                            "unique_name": "marker_parse",
                            "settings": {},
                            "depends_on": [],
                        }
                    ],
                    "created_at": "2024-01-01T00:00:00Z",
                },
                {
                    "id": 2,
                    "name": "Test Workflow 2",
                    "team_id": 12,
                    "steps": [
                        {
                            "step_key": "marker_extract",
                            "unique_name": "marker_extract",
                            "settings": {},
                            "depends_on": [],
                        }
                    ],
                    "created_at": "2024-01-02T00:00:00Z",
                },
            ]
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_response

                workflows = await client.list_workflows()

                # Verify result
                assert len(workflows) == 2
                assert all(isinstance(w, Workflow) for w in workflows)
                assert workflows[0].id == 1
                assert workflows[1].id == 2

                # Verify the API was called without team_id parameter
                mock_request.assert_called_once_with("GET", "/api/v1/workflows/workflows")

    @pytest.mark.asyncio
    async def test_execute_workflow_success(self):
        """Test executing a workflow"""
        # Mock execution response (returns immediately, no polling)
        mock_execute_response = {
            "execution_id": 1,
            "status": "processing",
            "success": True,
        }

        input_config = InputConfig(
            type="single_file", file_url="https://example.com/test.pdf"
        )

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_execute_response

                execution = await client.execute_workflow(
                    workflow_id=1, input_config=input_config
                )

                # Verify result - should return immediately with initial status
                assert isinstance(execution, WorkflowExecution)
                assert execution.id == 1
                assert execution.workflow_id == 1
                assert execution.status == "processing"
                assert execution.success is True
                assert execution.results is None  # No results yet

    @pytest.mark.asyncio
    async def test_get_execution_status_success(self):
        """Test checking execution status"""
        mock_response = {
            "execution_id": 1,
            "workflow_id": 1,
            "status": "COMPLETED",
            "steps": {
                "step1": {
                    "status": "COMPLETED",
                    "output_url": "https://example.com/output.json"
                }
            },
            "created": "2024-01-01T00:00:00Z",
            "updated": "2024-01-01T00:01:00Z",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_response

                execution = await client.get_execution_status(1)

                # Verify result
                assert isinstance(execution, WorkflowExecution)
                assert execution.id == 1
                assert execution.status == "COMPLETED"
                assert execution.success is True

    @pytest.mark.asyncio
    async def test_get_execution_status_with_polling(self):
        """Test polling execution status until completion"""
        # First status check: still processing
        mock_status_processing = {
            "execution_id": 1,
            "workflow_id": 1,
            "status": "IN_PROGRESS",
            "steps": {},
            "created": "2024-01-01T00:00:00Z",
        }

        # Second status check: complete
        mock_status_complete = {
            "execution_id": 1,
            "workflow_id": 1,
            "status": "COMPLETED",
            "steps": {
                "step1": {
                    "status": "COMPLETED",
                    "output_url": "https://example.com/output.json"
                }
            },
            "created": "2024-01-01T00:00:00Z",
            "updated": "2024-01-01T00:01:00Z",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                # Two status checks: processing, then complete
                mock_request.side_effect = [
                    mock_status_processing,
                    mock_status_complete,
                ]

                execution = await client.get_execution_status(
                    execution_id=1, max_polls=5, poll_interval=0.1
                )

                # Verify result
                assert execution.status == "COMPLETED"
                assert execution.success is True
                assert "step1" in execution.results
                assert mock_request.call_count == 2  # 2 status checks

    @pytest.mark.asyncio
    async def test_get_execution_status_failed(self):
        """Test checking execution status when workflow fails"""
        mock_status_failed = {
            "execution_id": 1,
            "workflow_id": 1,
            "status": "FAILED",
            "steps": {
                "step1": {
                    "status": "FAILED",
                    "error": "Step processing failed"
                }
            },
            "error": "Processing error occurred",
            "created": "2024-01-01T00:00:00Z",
            "updated": "2024-01-01T00:01:00Z",
        }

        async with AsyncDatalabClient(api_key="test-key") as client:
            with patch.object(client, "_make_request", new_callable=AsyncMock) as mock_request:
                mock_request.return_value = mock_status_failed

                execution = await client.get_execution_status(execution_id=1)

                # Verify result shows failure
                assert execution.status == "FAILED"
                assert execution.success is False
                assert execution.error == "Processing error occurred"


class TestSyncWorkflowClient:
    """Test sync wrapper for workflow methods"""

    def test_sync_create_workflow(self):
        """Test sync create_workflow"""
        steps = [
            WorkflowStep(
                step_key="marker_parse",
                unique_name="marker_parse",
                settings={"max_pages": 10},
            ),
        ]

        mock_response = {
            "id": 1,
            "name": "Test Workflow",
            "team_id": 12,
            "steps": [
                {
                    "step_key": "marker_parse",
                    "unique_name": "marker_parse",
                    "settings": {"max_pages": 10},
                    "depends_on": [],
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
        }

        client = DatalabClient(api_key="test-key")
        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            mock_request.return_value = mock_response

            workflow = client.create_workflow(name="Test Workflow", steps=steps)

            assert isinstance(workflow, Workflow)
            assert workflow.id == 1
            assert workflow.name == "Test Workflow"

    def test_sync_execute_workflow(self):
        """Test sync execute_workflow"""
        mock_execute_response = {
            "execution_id": 1,
            "status": "processing",
            "success": True,
        }

        input_config = InputConfig(
            type="single_file", file_url="https://example.com/test.pdf"
        )

        client = DatalabClient(api_key="test-key")
        with patch.object(
            client._async_client, "_make_request", new_callable=AsyncMock
        ) as mock_request:
            mock_request.return_value = mock_execute_response

            execution = client.execute_workflow(workflow_id=1, input_config=input_config)

            assert isinstance(execution, WorkflowExecution)
            assert execution.id == 1
            assert execution.status == "processing"
            assert execution.success is True


class TestWorkflowModels:
    """Test workflow model classes"""

    def test_workflow_step_to_dict(self):
        """Test WorkflowStep to_dict method"""
        step = WorkflowStep(
            step_key="marker_parse",
            unique_name="marker_parse",
            settings={"max_pages": 10},
            depends_on=["previous_step"],
        )

        result = step.to_dict()

        assert result["step_key"] == "marker_parse"
        assert result["unique_name"] == "marker_parse"
        assert result["settings"] == {"max_pages": 10}
        assert result["depends_on"] == ["previous_step"]

    def test_workflow_to_dict(self):
        """Test Workflow to_dict method"""
        steps = [
            WorkflowStep(
                step_key="marker_parse",
                unique_name="marker_parse",
                settings={"max_pages": 10},
            ),
        ]

        workflow = Workflow(id=1, name="Test Workflow", team_id=12, steps=steps)

        result = workflow.to_dict()

        assert result["id"] == 1
        assert result["name"] == "Test Workflow"
        assert result["team_id"] == 12
        assert len(result["steps"]) == 1
        assert result["steps"][0]["step_key"] == "marker_parse"

    def test_input_config_to_dict(self):
        """Test InputConfig to_dict method"""
        config = InputConfig(
            type="single_file",
            file_url="https://example.com/test.pdf",
            additional_config={"custom_field": "value"},
        )

        result = config.to_dict()

        assert result["type"] == "single_file"
        assert result["file_url"] == "https://example.com/test.pdf"
        assert result["custom_field"] == "value"

    def test_workflow_execution_save_output(self, temp_dir):
        """Test saving workflow execution results"""
        execution = WorkflowExecution(
            id=1,
            workflow_id=1,
            status="complete",
            success=True,
            input_config={"type": "single_file", "file_url": "https://example.com/test.pdf"},
            results={"output": "test result"},
        )

        output_path = temp_dir / "execution_result"
        execution.save_output(output_path)

        # Verify file was created
        result_file = temp_dir / "execution_result.json"
        assert result_file.exists()

        # Verify content
        import json

        with open(result_file, "r") as f:
            data = json.load(f)

        assert data["id"] == 1
        assert data["status"] == "complete"
        assert data["results"] == {"output": "test result"}
