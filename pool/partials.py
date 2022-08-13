import asyncio
import bisect
import itertools
import logging
import time

from chia.protocols.pool_protocol import PostPartialPayload
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.ints import uint64
from decimal import Decimal
from typing import Optional

from .util import RequestMetadata

logger = logging.getLogger('partials')


class PartialsInterval(object):

    def __init__(self, keep_interval):
        self.keep_interval = keep_interval
        self.clear()

    def __repr__(self):
        return f'<PartialsInterval[{self.points}]>'

    def add(self, timestamp, difficulty, remove=True):
        bisect.insort(self.partials, (timestamp, difficulty))
        self.points += difficulty
        self.last_update = int(time.time())

        if remove:
            self.scrub(self.last_update)

        return next(self.additions)

    def add_partials_list(self, partials_list):
        for pi in partials_list:
            self.partials += pi.partials
            self.points += pi.points
        self.last_update = int(time.time())
        self.partials.sort()

    def changed_recently(self, time):
        if self.last_update > time - 60 * 10:
            return True
        return False

    def clear(self):
        self.partials = []
        self.points = 0
        self.additions = itertools.count()
        self.last_update = 0

    def scrub(self, now=None):
        drop_time = (now or int(time.time())) - self.keep_interval
        while self.partials:
            timestamp, difficulty = self.partials[0]
            if timestamp < drop_time:
                del self.partials[0]
                self.points -= difficulty
            else:
                # We assume `partials` is a list in chronological order
                break


class PartialsCache(dict):

    def __init__(
            self, *args,
            partials=None, pool=None, store=None, store_ts=None, config=None, pool_config=None,
            keep_interval: int = 86400,
            **kwargs):
        self.partials = partials
        self.pool = pool
        self.store = store
        self.store_ts = store_ts
        self.config = config
        self.pool_config = pool_config
        self.keep_interval = keep_interval
        self.all = PartialsInterval(keep_interval)
        self._lock = asyncio.Lock()
        super().__init__(*args, **kwargs)

    def __missing__(self, launcher_id):
        pi = PartialsInterval(self.keep_interval)
        self[launcher_id] = pi
        return pi

    async def __aenter__(self, *args, **kwargs):
        await self._lock.acquire()

    async def __aexit__(self, *args, **kwargs):
        self._lock.release()

    async def add(self, launcher_id, timestamp, difficulty):
        if launcher_id not in self or self[launcher_id].points == 0:
            pi = self[launcher_id] = PartialsInterval(self.keep_interval)
            start_time = int(time.time()) - self.keep_interval
            recent_partials = await self.store.get_recent_partials(start_time, launcher_id)
            logger.info(
                'Launcher %r not in cache, adding %d from database',
                launcher_id, len(recent_partials),
            )
            for lid, t, d in recent_partials:
                pi.add(t, d, remove=False)
                self.all.add(t, d, remove=False)

        async with self._lock:
            additions = self[launcher_id].add(timestamp, difficulty)
            self.all.add(timestamp, difficulty)
        # Update estimated size and PPLNS every 5 partials
        if additions % 5 == 0:
            await self.update_db(launcher_id, timestamp)

    async def update_db(self, launcher_id, timestamp):
        if self[launcher_id].keep_interval == self.pool_config['time_target']:
            points = self[launcher_id].points
        else:
            last_time_target = timestamp - self.pool_config['time_target']
            points = sum(map(
                lambda x: x[1],
                filter(lambda x: x[0] >= last_time_target, self[launcher_id].partials),
            ))

        time_target_8h = 60 * 60 * 8
        last_8h = timestamp - time_target_8h
        points_8h = sum(map(
            lambda x: x[1],
            filter(lambda x: x[0] >= last_8h, self[launcher_id].partials),
        ))

        estimated_size_24h = self.partials.calculate_estimated_size(points)
        estimated_size_8h = self.partials.calculate_estimated_size(points_8h, time_target_8h)
        estimated_size = int((estimated_size_24h + estimated_size_8h) / 2)

        share_pplns = Decimal(points) / Decimal(self.all.points)
        logger.info(
            'Updating %r with points of %d (%.3f GiB), PPLNS %.5f',
            launcher_id,
            points,
            estimated_size / 1073741824,  # 1024 ^ 3
            share_pplns,
        )

        current_etw = await self.pool.get_etw(estimated_size)

        await self.store.update_farmer(
            launcher_id,
            ['estimated_size', 'points_pplns', 'share_pplns', 'current_etw'],
            [estimated_size, points, share_pplns, current_etw],
        )

        asyncio.create_task(self.store_ts.add_launcher_size(launcher_id, estimated_size_24h, estimated_size_8h))


class Partials(object):

    def __init__(self, pool):
        self.pool = pool
        self.store = pool.store
        self.store_ts = pool.store_ts
        self.config = pool.config
        self.pool_config = pool.pool_config
        # By default keep partials for the last day
        self.keep_interval = pool.pool_config.get('pplns_interval', 86400)

        self.cache = PartialsCache(
            partials=self,
            pool=pool,
            store=self.store,
            store_ts=self.store_ts,
            config=self.config,
            pool_config=self.pool_config,
            keep_interval=self.keep_interval,
        )

        self.scrub_lock = asyncio.Lock()

        self.remove_old_partials_loop_task: Optional[asyncio.Task] = None
        self.pool_estimated_size_loop_task: Optional[asyncio.Task] = None
        self.missing_partials_loop_task: Optional[asyncio.Task] = None
        self.scrub_loop_task: Optional[asyncio.Task] = None

    async def load_from_store(self):
        """
        Fill in the cache from database when initializing.
        """
        start_time = int(time.time()) - self.keep_interval
        for lid, t, d in await self.store.get_recent_partials(start_time):
            self.cache[lid].add(t, d, remove=False)
            self.cache.all.add(t, d, remove=False)
        await self.store.scrub_pplns(start_time)

    async def start(self, launchers):
        self.launchers = launchers
        self.remove_old_partials_loop_task = asyncio.create_task(
            self.remove_old_partials_loop()
        )
        self.pool_estimated_size_loop_task = asyncio.create_task(
            self.pool_estimated_size_loop()
        )
        self.missing_partials_loop_task = asyncio.create_task(
            self.missing_partials_loop()
        )
        self.scrub_loop_task = asyncio.create_task(self.scrub_loop())

    async def stop(self):
        if self.remove_old_partials_loop_task is not None:
            self.remove_old_partials_loop_task.cancel()
        if self.pool_estimated_size_loop_task is not None:
            self.pool_estimated_size_loop_task.cancel()
        if self.missing_partials_loop_task is not None:
            self.missing_partials_loop_task.cancel()
        if self.scrub_loop_task is not None:
            self.scrub_loop_task.cancel()

    def calculate_estimated_size(self, points, time_target=None):
        if time_target is None:
            time_target = self.pool_config['time_target']
        estimated_size = int(points / (time_target * 1.0881482400062102e-15))
        if self.config['full_node']['selected_network'] in ('testnet7', 'testnet10'):
            # DIFFICULTY_CONSTANT_FACTOR
            # mainnet = 2 ** 67
            # testnet = 10052721566054
            estimated_size = int(estimated_size / 14680000)
        return estimated_size

    async def scrub(self):
        async with self.scrub_lock:
            async with self.cache:
                now = int(time.time())
                to_update = []

                self.cache.all.clear()

                for launcher_id, points_interval in list(self.cache.items()):
                    if not points_interval.changed_recently(now):
                        before = points_interval.points
                        if points_interval.scrub() == 0:
                            del self.cache[launcher_id]
                            # Check if NFT is still assigned to pool
                            await self.launchers.add_singleton(
                                bytes32(bytes.fromhex(launcher_id))
                            )
                        if points_interval.points != before:
                            to_update.append(launcher_id)

                self.cache.all.add_partials_list(list(self.cache.values()))

            now = int(time.time())
            for i in to_update:
                await self.cache.update_db(i, now)

    async def scrub_loop(self):
        while True:
            try:
                await asyncio.sleep(60 * 2)
                await self.scrub()
            except asyncio.CancelledError:
                logger.info('Cancelled scrub_loop')
                break
            except Exception:
                logger.error('Unexpected error in scrub_loop', exc_info=True)

    async def get_pool_size(self):
        return self.calculate_estimated_size(self.cache.all.points)

    async def remove_old_partials_loop(self):
        while True:
            try:
                # Get timestamp of the first block before one week ago.
                # We will keep partials for about one week.
                old_timestamp = await self.store.get_block_timestamp(
                    int(time.time() - 7 * 24 * 60 * 60)
                )
                rowcount = await self.store.remove_partials(old_timestamp)
                logger.info('%d old partials deleted', rowcount)
            except asyncio.CancelledError:
                logger.info('Cancelled remove_old_partials_loop')
                break
            except Exception:
                logger.error('Unexpected error in remove_old_partials_loop', exc_info=True)
            # Run once a day
            await asyncio.sleep(60 * 60 * 24)

    async def pool_estimated_size_loop(self):
        while True:
            try:
                start_time = int(time.time()) - self.keep_interval
                points_per_pool_host = await self.store.get_points_per_pool_host(start_time)

                points_per_pool_host['global'] = sum(points_per_pool_host.values())

                size_per_pool_host = {}
                for k, v in list(points_per_pool_host.items()):
                    if k is None:
                        logger.warning('None pool host with size %d', v)
                        continue
                    size_per_pool_host[k] = self.calculate_estimated_size(v)

                await self.store_ts.add_pool_size(size_per_pool_host)
            except asyncio.CancelledError:
                logger.info('Cancelled pool_estimated_size_loop')
                break
            except Exception:
                logger.error('Unexpected error in pool_estimated_size_loop', exc_info=True)
            await asyncio.sleep(60 * 30)

    async def missing_partials_loop(self):
        """
        Loop to check for launchers that suddenly stopped sending partials
        """
        seen = {}
        while True:
            try:
                new = {}
                one_hour_ago = time.time() - 3600
                for launcher_id, pi in self.cache.items():
                    if pi.partials and pi.partials[-1][0] < one_hour_ago:
                        if launcher_id not in seen:
                            new[launcher_id] = pi.partials[-1][0]
                        seen[launcher_id] = pi.partials[-1][0]
                    else:
                        seen.pop(launcher_id, None)

                if new:
                    farmer_records = {}
                    time_now = time.time()
                    two_hours_ago = time_now - 3600 * 2
                    three_hours_ago = time_now - 3600 * 3
                    six_hours_ago = time_now - 3600 * 6
                    for launcher_id, rec in (await self.store.get_farmer_records([
                        ('email', 'IS NOT NULL', None),
                        ('notify_missing_partials_hours', 'IS NOT NULL', None),
                        ('notify_missing_partials_hours', '>', 0),
                    ])).items():
                        last_seen = new.get(launcher_id)
                        if not last_seen:
                            continue
                        if self.config['full_node']['selected_network'] == 'mainnet':
                            # Farmers with low space can take up more hours without partials
                            if rec.estimated_size < 429496729600:  # 400GiB
                                if last_seen > six_hours_ago:
                                    continue
                            if rec.estimated_size < 966367641600:  # 900GiB
                                if last_seen > three_hours_ago:
                                    continue
                            if rec.estimated_size < 1932735283200:  # 1800GiB
                                if last_seen > two_hours_ago:
                                    continue
                        farmer_records[launcher_id] = rec

                        if rec.is_pool_member:
                            # Check if farmer is still a pool member
                            await self.launchers.add_singleton(rec.launcher_id)
                    if farmer_records:
                        logger.debug('%d launchers stopped sending partials.', len(farmer_records))
                        await self.pool.run_hook('missing_partials', farmer_records)
                else:
                    logger.debug('No launchers stopped sending partials.')
            except asyncio.CancelledError:
                logger.info('Cancelled missing_partials_loop')
                break
            except Exception:
                logger.error('Unexpected error in missing_partials_loop', exc_info=True)
            await asyncio.sleep(3600)

    async def remove_launcher(self, launcher_id: bytes32):
        lid = launcher_id.hex()
        if lid not in self.cache:
            return
        async with self.cache:
            self.cache.pop(lid, None)
        await self.scrub()

    async def add_partial(
        self,
        partial_payload: PostPartialPayload,
        req_metadata: Optional[RequestMetadata],
        timestamp: uint64,
        difficulty: uint64,
        error: Optional[str] = None,
    ) -> None:

        # Add to database
        await self.store.add_partial(partial_payload, req_metadata, timestamp, difficulty, error)
        asyncio.create_task(
            self.store_ts.add_partial(partial_payload, timestamp, difficulty, error)
        )

        # Add to the cache and compute the estimated farm size if a successful partial
        if error is None:
            await self.cache.add(partial_payload.launcher_id.hex(), timestamp, difficulty)

    async def get_recent_partials(self, launcher_id: bytes32, number_of_partials: int):
        """
        Difficulty function expects descendent order.
        """
        return [
            (uint64(x[0]), uint64(x[1]))
            for x in reversed(self.cache[launcher_id.hex()].partials[-number_of_partials:])
        ]

    async def get_farmer_points_data(self):
        launcher_payout_data = await self.store.get_launcher_id_payout_data(
            self.pool_config.get('reward_system')
        )
        points_data = []
        await self.scrub()
        async with self.cache:
            total_points = self.cache.all.points
            for launcher_id, points_interval in self.cache.items():
                if points_interval.points == 0:
                    continue
                payout_data = launcher_payout_data.get(launcher_id)
                if payout_data is None:
                    logger.error(
                        'Did not find payout instructions for %r, points %d.',
                        launcher_id, points_interval.points,
                    )
                    total_points -= points_interval.points
                    continue
                payout_data['points'] = uint64(points_interval.points)
                payout_data['launcher_id'] = launcher_id
                points_data.append(payout_data)
        return points_data, total_points
