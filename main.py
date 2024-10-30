import asyncio
from tasks_scheduler.scheduler import start_scheduler

if __name__ == "__main__":
    asyncio.run(start_scheduler())
