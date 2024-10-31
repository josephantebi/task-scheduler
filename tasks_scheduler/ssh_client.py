# tasks_scheduler/ssh_client.py
import asyncio
import asyncssh
import os
from dotenv import load_dotenv
from tasks_scheduler.db_client import update_task_log
import logging

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SSH_HOST = os.getenv('SSH_HOST') 
SSH_PORT = int(os.getenv('SSH_PORT', 22)) 
SSH_USER = os.getenv('SSH_USER') 
SSH_PASSWORD = os.getenv('SSH_PASSWORD') 

async def execute_remote_command(command, run_id):
    """
    Executes a remote command via SSH and updates the task log with output and errors.
    """
    logger.info(f"Executing remote command: {command} with run_id: {run_id}")
    try:
        # Connect to the remote SSH server
        async with asyncssh.connect(
            SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASSWORD
        ) as conn:
            proc = await conn.create_process(command)
            logger.info("Process created. Starting to capture output and errors.")

            async def read_stream(stream, callback):
                while not stream.at_eof():
                    data = await stream.read(1024)
                    if data:
                        await callback(data)

            async def handle_stdout(data):
                logger.info(f"STDOUT: {data.strip()}")
                await update_task_log(run_id, output=data)

            async def handle_stderr(data):
                logger.error(f"STDERR: {data.strip()}")
                await update_task_log(run_id, error=data)

            # Capture stdout and stderr in real-time
            await asyncio.gather(
                read_stream(proc.stdout, handle_stdout),
                read_stream(proc.stderr, handle_stderr)
            )

            await proc.wait()
            return_code = str(proc.exit_status)  # Convert to string
            logger.info(f"Command executed with return code: {return_code}")
            await update_task_log(run_id, return_code=return_code, complete=True)

    except asyncssh.Error as e:
        logger.error(f"SSH connection failed: {e}")
        await update_task_log(run_id, error=str(e), complete=True)
