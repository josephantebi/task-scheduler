import asyncio
from tasks_scheduler.db_client import get_pending_tasks
from tasks_scheduler.task_runner import run_task

async def start_scheduler():
    while True:
        tasks = await get_pending_tasks()
        for task in tasks:
            asyncio.create_task(run_task(task))
        await asyncio.sleep(5)
