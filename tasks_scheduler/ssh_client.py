import asyncio
import asyncssh
import os
from dotenv import load_dotenv
from tasks_scheduler.db_client import update_task_log

load_dotenv()

SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_USER = os.getenv('SSH_USER')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')

async def execute_remote_command(command, run_id):
    """
    Executes a remote command via SSH and updates the task log with output and errors.
    """
    async with asyncssh.connect(
        SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASSWORD
    ) as conn:
        proc = await conn.create_process(command)
        stdout, stderr = '', ''
        while not proc.stdout.at_eof() or not proc.stderr.at_eof():
            output = await proc.stdout.read(1024)
            error = await proc.stderr.read(1024)
            if output:
                stdout += output
            if error:
                stderr += error
            # Update task log
            await update_task_log(run_id, output=output, error=error)
            await asyncio.sleep(5)  # Sleep for 5 seconds before next update
        await proc.wait()
        return_code = proc.exit_status
        # Update task log with final output and return code
        await update_task_log(run_id, return_code=return_code, complete=True)
