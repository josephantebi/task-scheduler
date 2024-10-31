# tests/test_task_runner.py
import datetime
import pytest
from tasks_scheduler.task_runner import run_task
from tasks_scheduler.db_client import get_task_by_name_and_due_date, insert_task

@pytest.mark.asyncio
async def test_run_task_local_success(monkeypatch):
    """
    Test running a task locally that succeeds.
    """
    monkeypatch.setenv('LOCAL_MODE', 'True')

    task = {
        'task_name': 'test_local_success',
        'task_command': 'echo "Local Task Success"',
        'task_due_date': datetime.datetime(2024, 1, 1)
    }

    await insert_task(task['task_name'], task['task_command'], task['task_due_date'])

    await run_task(task)

    task_record = await get_task_by_name_and_due_date(task['task_name'], task['task_due_date'])
    assert task_record is not None, "Task not found."
    assert task_record['task_state'] == 'completed', "Task was not marked as completed."


@pytest.mark.asyncio
async def test_run_task_local_failure(monkeypatch):
    """
    Test running a task locally that fails.
    """
    monkeypatch.setenv('LOCAL_MODE', 'True')

    task = {
        'task_name': 'test_local_failure',
        'task_command': 'invalid_command',
        'task_due_date': datetime.datetime(2024, 1, 1)
    }

    await insert_task(task['task_name'], task['task_command'], task['task_due_date'])

    await run_task(task)

    task_record = await get_task_by_name_and_due_date(task['task_name'], task['task_due_date'])
    assert task_record is not None, "Task not found."
    assert task_record['task_state'] == 'failed', "Task was not marked as failed."
