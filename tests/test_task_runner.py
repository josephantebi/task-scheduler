# tests/test_task_runner.py
import asyncio
import pytest
from unittest.mock import patch, AsyncMock, ANY
from tasks_scheduler.task_runner import run_task
from tasks_scheduler.db_client import insert_task_log, update_task_state, update_task_log

@pytest.mark.asyncio
async def test_run_task_local_success():
    """
    Test running a task locally that succeeds.
    """
    task = {
        'task_name': 'test_local_success',
        'task_command': 'echo "Local Task Success"',
        'task_due_date': '2024-01-01'
    }

    with patch('tasks_scheduler.task_runner.execute_remote_command', new=AsyncMock()) as mock_execute_remote_command:
        with patch('tasks_scheduler.task_runner.update_task_state', new=AsyncMock()) as mock_update_task_state:
            # Run the task
            await run_task(task)

            # Assertions
            mock_update_task_state.assert_any_call('test_local_success', '2024-01-01', 'running')
            mock_update_task_state.assert_any_call('test_local_success', '2024-01-01', 'completed')
            mock_execute_remote_command.assert_not_called()
            print("test_run_task_local_success passed: Local task ran and completed successfully.")

@pytest.mark.asyncio
async def test_run_task_local_failure():
    """
    Test running a task locally that fails.
    """
    task = {
        'task_name': 'test_local_failure',
        'task_command': 'invalid_command',  # This should fail
        'task_due_date': '2024-01-01'
    }

    with patch('tasks_scheduler.task_runner.execute_remote_command', new=AsyncMock()) as mock_execute_remote_command:
        with patch('tasks_scheduler.task_runner.update_task_state', new=AsyncMock()) as mock_update_task_state:
            # Run the task
            await run_task(task)

            # Assertions
            mock_update_task_state.assert_any_call('test_local_failure', '2024-01-01', 'running')
            mock_update_task_state.assert_any_call('test_local_failure', '2024-01-01', 'failed')
            mock_execute_remote_command.assert_not_called()
            print("test_run_task_local_failure passed: Local task failed as expected.")

@pytest.mark.asyncio
async def test_run_task_remote_success():
    """
    Test running a task remotely via SSH that succeeds.
    """
    task = {
        'task_name': 'test_remote_success',
        'task_command': 'echo "Remote Task Success"',
        'task_due_date': '2024-01-01'
    }

    with patch('tasks_scheduler.task_runner.execute_remote_command', new=AsyncMock(return_value=0)) as mock_execute_remote_command:
        with patch('tasks_scheduler.task_runner.update_task_state', new=AsyncMock()) as mock_update_task_state:
            # Set LOCAL_MODE to False
            with patch('tasks_scheduler.task_runner.LOCAL_MODE', False):
                await run_task(task)

                # Assertions
                mock_update_task_state.assert_any_call('test_remote_success', '2024-01-01', 'running')
                mock_execute_remote_command.assert_called_with('echo "Remote Task Success"', ANY)
                mock_update_task_state.assert_any_call('test_remote_success', '2024-01-01', 'completed')
                print("test_run_task_remote_success passed: Remote task ran and completed successfully.")

@pytest.mark.asyncio
async def test_run_task_remote_failure():
    """
    Test running a task remotely via SSH that fails.
    """
    task = {
        'task_name': 'test_remote_failure',
        'task_command': 'invalid_command',  # This should fail
        'task_due_date': '2024-01-01'
    }

    with patch('tasks_scheduler.task_runner.execute_remote_command', new=AsyncMock(return_value=1)) as mock_execute_remote_command:
        with patch('tasks_scheduler.task_runner.update_task_state', new=AsyncMock()) as mock_update_task_state:
            # Set LOCAL_MODE to False
            with patch('tasks_scheduler.task_runner.LOCAL_MODE', False):
                await run_task(task)

                # Assertions
                mock_update_task_state.assert_any_call('test_remote_failure', '2024-01-01', 'running')
                mock_execute_remote_command.assert_called_with('invalid_command', ANY)
                mock_update_task_state.assert_any_call('test_remote_failure', '2024-01-01', 'failed')
                print("test_run_task_remote_failure passed: Remote task failed as expected.")
