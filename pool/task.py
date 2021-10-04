import asyncio
import logging

logger = logging.getLogger('task')


def task_exception(coro):
    async def wrapper(*args, **kwargs):
        try:
            await coro(*args, **kwargs)
        except Exception:
            logger.error('Task %r failed', coro, exc_info=True)
    return wrapper
