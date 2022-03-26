import asyncio
import logging
import time

from chia.types.blockchain_format.sized_bytes import bytes32

from .record import FarmerRecord
from .singleton import LastSpendCoinNotFound, find_last_reward_from_launcher
from .task import task_exception

logger = logging.getLogger('launchers')


class Launchers(object):

    def __init__(self, pool):
        self.pool = pool
        self.store = pool.store
        self.time_target = pool.time_target
        self.pending_singleton: asyncio.Queue = asyncio.Queue()
        self.pending_last_reward: asyncio.Queue = asyncio.Queue()

    async def add_singleton(self, launcher_id: bytes32):
        await self.pending_singleton.put(launcher_id)

    @task_exception
    async def singleton_loop(self):

        for lid in await self.store.get_launchers_without_recent_partials(
            int(time.time() - self.time_target)
        ):
            await self.add_singleton(lid)

        while True:
            try:
                launcher_id: bytes32 = await self.pending_singleton.get()

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

    async def add_last_reward(self, farmer_record: FarmerRecord):
        await self.pending_last_reward.put(farmer_record)

    @task_exception
    async def last_reward_loop(self):
        for farmer in (await self.store.get_farmer_records([
            ('last_block_timestamp', 'IS NULL', None),
            ('is_pool_member', '=', True),
        ])).values():
            await self.add_last_reward(farmer)

        while True:
            farmer_record: FarmerRecord = await self.pending_last_reward.get()
            lr = await self.last_reward_update(farmer_record)
            if lr:
                logger.info('Last reward timestamp for %r: %r', farmer_record.launcher_id.hex(), lr)
            await asyncio.sleep(2)

    async def last_reward_update(self, farmer_record: FarmerRecord):
        try:
            if farmer_record.last_block_timestamp:
                return farmer_record.last_block_timestamp

            coin = await find_last_reward_from_launcher(
                self.pool.node_rpc_client,
                farmer_record,
                self.pool.blockchain_state['peak'].height,
            )

            if not coin:
                # If the launcher never farmed a block get the birth of NFT
                coin = await self.pool.node_rpc_client.get_coin_record_by_name(
                    farmer_record.launcher_id,
                )

            await self.store.update_farmer(
                farmer_record.launcher_id,
                ['last_block_timestamp'],
                [int(coin.timestamp)],
            )
            return int(coin.timestamp)

        except Exception:
            logger.error(
                'Failed to get last reward for %r',
                farmer_record.launcher_id.hex(),
                exc_info=True,
            )

    async def start(self):
        self.singleton_loop_task = asyncio.create_task(self.singleton_loop())
        self.last_reward_loop_task = asyncio.create_task(self.last_reward_loop())

    async def stop(self):
        self.singleton_loop_task.cancel()
        self.last_reward_loop_task.cancel()
