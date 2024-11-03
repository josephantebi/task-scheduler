# tasks_scheduler/task_runner.py
import asyncio
from tasks_scheduler.db_client import update_task_state, insert_task_log, update_task_log
from tasks_scheduler.ssh_client import execute_remote_command
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

async def follow_process_output(proc, run_id):
    """
    Follows the stdout and stderr of a running process and updates the task log every 5 seconds.
    """
    output_buffer = ''
    error_buffer = ''
    update_interval = 5  # seconds

    async def read_stream(stream, buffer, name):
        while True:
            line = await stream.readline()
            if not line:
                break
            buffer.append(line.decode())
            logger.info(f"{name}: {line.decode().strip()}")

    stdout_buffer = []
    stderr_buffer = []

    stdout_task = asyncio.create_task(read_stream(proc.stdout, stdout_buffer, 'STDOUT'))
    stderr_task = asyncio.create_task(read_stream(proc.stderr, stderr_buffer, 'STDERR'))

    while not proc.stdout.at_eof() or not proc.stderr.at_eof():
        await asyncio.sleep(update_interval)
        output = ''.join(stdout_buffer)
        error = ''.join(stderr_buffer)
        stdout_buffer.clear()
        stderr_buffer.clear()
        if output or error:
            await update_task_log(run_id, output=output, error=error)
            logger.info(f"Updated task log '{run_id}' with new output and error.")

    await asyncio.gather(stdout_task, stderr_task)
    await proc.wait()
    return_code = proc.returncode

    # Update the log with the final return code and completion date
    await update_task_log(run_id, return_code=str(return_code), complete=True)


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

            await follow_process_output(proc, run_id)

            return_code = proc.returncode

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
