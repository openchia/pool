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


@task_exception
async def common_loop(method, init_coro=None, sleep=None, log=None):
    if log is None:
        log = logger

    if init_coro:
        await init_coro

    while True:
        try:
            await method()
            if sleep:
                await asyncio.sleep(sleep)
        except asyncio.CancelledError:
            log.info(f'Cancelled {method.__name__}, closing')
            return
        except Exception:
            log.error(f'Unexpected error in {method.__name__}', exc_info=True)
            await asyncio.sleep(5)
