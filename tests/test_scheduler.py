# tests/test_scheduler.py
import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from tasks_scheduler.scheduler import start_scheduler
from tasks_scheduler.db_client import get_pending_tasks
from tasks_scheduler.task_runner import run_task
import logging

@pytest.mark.asyncio
async def test_start_scheduler():
    """
    Test the start_scheduler function to ensure it retrieves tasks and runs them.
    """
    # Mock the get_pending_tasks to return a predefined list of tasks
    mock_tasks = [
        {
            'task_name': 'test_task_scheduler',
            'task_command': 'echo "Scheduler Task"',
            'task_due_date': '2024-01-01'
        }
    ]

    with patch('tasks_scheduler.scheduler.get_pending_tasks', new=AsyncMock(return_value=mock_tasks)):
        with patch('tasks_scheduler.scheduler.run_task', new=AsyncMock()) as mock_run_task:
            # Run the scheduler for a short period and then cancel
            scheduler_task = asyncio.create_task(start_scheduler())
            await asyncio.sleep(1)  # Let the scheduler run once
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass

            # Assert that run_task was called with the mock task
            mock_run_task.assert_called_with(mock_tasks[0])
            print("test_start_scheduler passed: Scheduler retrieved tasks and initiated run_task.")
