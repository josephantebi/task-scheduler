# tests/test_db_client.py
import asyncio
import pytest
from tasks_scheduler.db_client import (
    get_db_connection,
    insert_task_log,
    update_task_state,
    get_pending_tasks,
    update_task_log
)
import uuid

@pytest.mark.asyncio
async def test_get_db_connection():
    """
    Test establishing a connection to the PostgreSQL database.
    """
    conn = await get_db_connection()
    assert conn is not None, "Failed to establish database connection."
    await conn.close()
    print("test_get_db_connection passed: Successfully connected to the database.")

@pytest.mark.asyncio
async def test_insert_task_log():
    """
    Test inserting a new task log entry and verifying the run_id.
    """
    task_name = "test_task_insert"
    run_id = await insert_task_log(task_name)
    assert isinstance(run_id, str), "run_id should be a string."
    assert uuid.UUID(run_id), "run_id should be a valid UUID."
    print(f"test_insert_task_log passed: Inserted task log with run_id {run_id}.")

@pytest.mark.asyncio
async def test_update_task_state():
    """
    Test updating the state of a task and verifying the update.
    """
    task_name = "test_task_update_state"
    task_due_date = "2024-12-31"
    new_state = "running"

    # Insert a task to update
    run_id = await insert_task_log(task_name)
    await update_task_state(task_name, task_due_date, new_state)

    # Retrieve pending tasks to verify update
    tasks = await get_pending_tasks()
    updated_tasks = [task for task in tasks if task['task_name'] == task_name and task['task_due_date'].strftime('%Y-%m-%d') == task_due_date]
    assert len(updated_tasks) == 0, "Task state was not updated correctly."
    print(f"test_update_task_state passed: Updated task '{task_name}' to state '{new_state}'.")

@pytest.mark.asyncio
async def test_get_pending_tasks():
    """
    Test retrieving pending tasks that are due to run.
    """
    # Insert a task that is due to run
    task_name = "test_task_pending"
    task_command = "echo 'Pending Task'"
    task_due_date = "2024-01-01"  # Past date to ensure it's due
    await update_task_state(task_name, task_due_date, "waiting_to_run")
    run_id = await insert_task_log(task_name)

    tasks = await get_pending_tasks()
    matching_tasks = [task for task in tasks if task['task_name'] == task_name]
    assert len(matching_tasks) > 0, "Failed to retrieve pending tasks."
    print(f"test_get_pending_tasks passed: Retrieved {len(matching_tasks)} pending task(s).")

@pytest.mark.asyncio
async def test_update_task_log():
    """
    Test updating the task log with output, error, and completion status.
    """
    task_name = "test_task_log_update"
    run_id = await insert_task_log(task_name)

    # Update with output and error
    output = "Task completed successfully."
    error = ""
    await update_task_log(run_id, output=output, error=error)

    # Update with completion status
    return_code = 0
    complete = True
    await update_task_log(run_id, return_code=return_code, complete=complete)

    # Retrieve and verify the log
    conn = await get_db_connection()
    try:
        log = await conn.fetchrow("""
            SELECT * FROM task_log WHERE run_id = $1;
        """, run_id)
        assert log['output'] == output, "Output was not updated correctly."
        assert log['error'] == error, "Error was not updated correctly."
        assert log['return_code'] == return_code, "Return code was not updated correctly."
        assert log['complete_date'] is not None, "Complete date was not set."
    finally:
        await conn.close()
    
    print(f"test_update_task_log passed: Updated task log for run_id {run_id}.")
