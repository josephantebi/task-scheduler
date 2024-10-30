import asyncio
import uuid
from tasks_scheduler.db_client import update_task_state, insert_task_log, update_task_log
from tasks_scheduler.ssh_client import execute_remote_command

async def run_task(task):
    task_name = task['task_name']
    task_command = task['task_command']
    task_due_date = task['task_due_date']

    await update_task_state(task_name, task_due_date, 'running')
    run_id = await insert_task_log(task_name)

    # Execute the command asynchronously
    await execute_remote_command(task_command, run_id)

    # After execution, update task state to 'completed'
    await update_task_state(task_name, task_due_date, 'completed')
