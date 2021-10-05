import asyncio
import logging
import time

from chia.types.blockchain_format.sized_bytes import bytes32

from .singleton import LastSpendCoinNotFound
from .task import task_exception

logger = logging.getLogger('launchers')


class LaunchersSingleton(object):

    def __init__(self, pool):
        self.pool = pool
        self.store = pool.store
        self.time_target = pool.time_target
        self.pending_launchers: asyncio.Queue = asyncio.Queue()

    async def add_launcher(self, launcher_id: bytes32):
        await self.pending_launchers.put(launcher_id)

    @task_exception
    async def loop(self):

        for lid in await self.store.get_launchers_without_recent_partials(
            int(time.time() - self.time_target)
        ):
            await self.add_launcher(lid)

        while True:
            try:
                launcher_id: bytes32 = await self.pending_launchers.get()

                try:
                    singleton_state_tuple = await self.pool.get_and_validate_singleton_state(
                        launcher_id, raise_exc=True,
                    )
                except LastSpendCoinNotFound as e:
                    is_member = False
                    for wallet in self.pool.wallets:
                        if wallet['puzzle_hash'] == e.last_not_none_state.target_puzzle_hash:
                            is_member = True
                            break
                else:
                    if singleton_state_tuple is None:
                        continue

                    is_member = singleton_state_tuple[2]

                if not is_member:
                    logger.info('Launcher %s is no longer a pool member', launcher_id.hex())
                    await self.pool.partials.remove_launcher(launcher_id)
                    await self.store.update_farmer(launcher_id, ['is_pool_member'], [False])
                else:
                    logger.info('Launcher %s is still a pool member', launcher_id.hex())

            except asyncio.CancelledError:
                logger.info("Cancelled launchers loop, closing")
                return
            except Exception as e:
                logger.error(f"Unexpected error in launchers loop: {e}", exc_info=True)
                await asyncio.sleep(5)

            # Sleep 3s between every check to not potentially overload the node
            await asyncio.sleep(3)
