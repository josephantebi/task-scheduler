# main.py
import asyncio
from tasks_scheduler.scheduler import start_scheduler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        logger.info("Starting scheduler...")
        asyncio.run(start_scheduler())
    except KeyboardInterrupt:
        logger.info("Scheduler stopped manually.")

