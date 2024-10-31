# tests/test_db_client.py
import datetime
import pytest
from tasks_scheduler.db_client import (
    insert_task,
    update_task_state,
    get_task_by_name_and_due_date
)

@pytest.mark.asyncio
async def test_update_task_state():
    """
    Test updating the state of a task and verifying the update.
    """
    task_name = "test_task_update_state"
    task_due_date = datetime.datetime(2024, 12, 31)
    new_state = "running"

    # Insert a task for testing update
    await insert_task(task_name, "echo 'Test Task'", task_due_date)

    # Update the task state
    await update_task_state(task_name, task_due_date, new_state)

    # Fetch the task directly to verify the update
    task = await get_task_by_name_and_due_date(task_name, task_due_date)

    # Verify that the task exists and its state was updated
    assert task is not None, "Task not found."
    assert task['task_state'] == new_state, "Task state was not updated correctly."
