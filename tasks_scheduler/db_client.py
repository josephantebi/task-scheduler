# db_client.py
import asyncpg
import os
from dotenv import load_dotenv
import uuid
import logging

load_dotenv()

logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


async def get_db_connection():
    """
    Establishes and returns a connection to the PostgreSQL database.
    """
    try:
        conn = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info("Connected to the database.")
        return conn
    except asyncpg.PostgresError as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise


async def get_pending_tasks():
    """
    Retrieves tasks that are due to run and are in 'waiting_to_run' state.
    """
    conn = await get_db_connection()
    try:
        tasks = await conn.fetch("""
            SELECT task_name, task_command, task_due_date
            FROM tasks
            WHERE task_due_date <= NOW() AND task_state = 'waiting_to_run';
        """)
        logger.info(f"Retrieved {len(tasks)} pending tasks.")
        return tasks
    finally:
        await conn.close()


async def update_task_state(task_name, task_due_date, state):
    """
    Updates the state of a task.
    """
    conn = await get_db_connection()
    try:
        await conn.execute("""
            UPDATE tasks
            SET task_state = $1
            WHERE task_name = $2 AND task_due_date = $3;
        """, state, task_name, task_due_date)
        logger.info(f"Updated task '{task_name}' to state '{state}'.")
    finally:
        await conn.close()


async def insert_task_log(task_name):
    """
    Inserts a new log entry for a task and returns the run_id.
    """
    conn = await get_db_connection()
    try:
        run_id = str(uuid.uuid4())
        await conn.execute("""
            INSERT INTO task_log (run_id, task_name, run_date)
            VALUES ($1, $2, NOW());
        """, run_id, task_name)
        logger.info(f"Inserted log for task '{task_name}' with run_id '{run_id}'.")
        return run_id
    finally:
        await conn.close()


async def update_task_log(run_id, output=None, error=None, return_code=None, complete=False):
    """
    Updates the task log with output, error, return code, and completion status.
    """
    conn = await get_db_connection()
    try:
        if complete:
            await conn.execute("""
                UPDATE task_log
                SET complete_date = NOW(), return_code = $1
                WHERE run_id = $2;
            """, return_code, run_id)
            logger.info(f"Task log '{run_id}' marked as complete with return code '{return_code}'.")
        else:
            await conn.execute("""
                UPDATE task_log
                SET output = COALESCE(output, '') || $1,
                    error = COALESCE(error, '') || $2
                WHERE run_id = $3;
            """, output, error, run_id)
            logger.info(f"Updated task log '{run_id}' with output and error.")
    finally:
        await conn.close()
