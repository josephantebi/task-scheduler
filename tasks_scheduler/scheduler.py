# tasks_scheduler/scheduler.py
import asyncio
from tasks_scheduler.db_client import get_pending_tasks
from tasks_scheduler.task_runner import run_task
import logging

logger = logging.getLogger(__name__)


async def start_scheduler():
    """
    Starts the scheduler that periodically checks for pending tasks and runs them.
    """
    logger.info("Scheduler started.")
    while True:
        tasks = await get_pending_tasks()
        if tasks:
            logger.info(f"Found {len(tasks)} tasks to run.")
            for task in tasks:
                asyncio.create_task(run_task(task))
        else:
            logger.info("No pending tasks found.")
        await asyncio.sleep(5)
