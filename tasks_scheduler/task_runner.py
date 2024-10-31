# tasks_scheduler/task_runner.py
import asyncio
from tasks_scheduler.db_client import update_task_state, insert_task_log, update_task_log
from tasks_scheduler.ssh_client import execute_remote_command
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

async def run_task(task):
    """
    Runs a given task by executing its command and updating its state.
    """
    task_name = task['task_name']
    task_command = task['task_command']
    task_due_date = task['task_due_date']

    await update_task_state(task_name, task_due_date, 'running')
    run_id = await insert_task_log(task_name)

    LOCAL_MODE = os.getenv('LOCAL_MODE', 'False').lower() in ['true', '1', 'yes']

    if not LOCAL_MODE:
        try:
            await execute_remote_command(task_command, run_id)
            await update_task_state(task_name, task_due_date, 'completed')
        except Exception as e:
            logger.error(f"Remote command execution failed: {e}")
            await update_task_log(run_id, error=str(e), complete=True)
            await update_task_state(task_name, task_due_date, 'failed')
    else:
        logger.info(f"Executing local command: {task_command} with run_id: {run_id}")
        try:
            proc = await asyncio.create_subprocess_shell(
                task_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            return_code = proc.returncode

            # Update the log
            await update_task_log(run_id, output=stdout.decode(), error=stderr.decode(), return_code=str(return_code), complete=True)

            if return_code == 0:
                await update_task_state(task_name, task_due_date, 'completed')
                logger.info(f"Local command executed successfully with return code: {return_code}")
            else:
                await update_task_state(task_name, task_due_date, 'failed')
                logger.error(f"Local command failed with return code: {return_code}")
        except Exception as e:
            logger.error(f"Local command execution failed: {e}")
            await update_task_log(run_id, error=str(e), complete=True)
            await update_task_state(task_name, task_due_date, 'failed')
