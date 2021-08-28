import asyncio
import bisect
import itertools
import logging
import time

from chia.cmds.farm_funcs import get_average_block_time
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.ints import uint64
from decimal import Decimal
from typing import Optional

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

    def add_partials(self, pi):
        self.partials += pi.partials
        self.points += pi.points
        self.partials.sort()
        self.last_update = int(time.time())

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
            partials=None, store=None, config=None, pool_config=None, keep_interval: int = 86400,
            **kwargs):
        self.partials = partials
        self.store = store
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

        estimated_size = self.partials.calculate_estimated_size(points)

        share_pplns = Decimal(points) / Decimal(self.all.points)
        logger.info(
            'Updating %r with points of %d (%.3f GiB), PPLNS %.5f',
            launcher_id,
            points,
            estimated_size / 1073741824,  # 1024 ^ 3
            share_pplns,
        )
        await self.store.update_estimated_size_and_pplns(
            launcher_id, estimated_size, points, share_pplns
        )


class Partials(object):

    def __init__(self, pool):
        self.pool = pool
        self.store = pool.store
        self.config = pool.config
        self.pool_config = pool.pool_config
        # By default keep partials for the last day
        self.keep_interval = pool.pool_config.get('pplns_interval', 86400)

        self.cache = PartialsCache(
            partials=self,
            store=self.store,
            config=self.config,
            pool_config=self.pool_config,
            keep_interval=self.keep_interval,
        )

        self.scrub_lock = asyncio.Lock()
        self.additions = itertools.count()

    async def load_from_store(self):
        """
        Fill in the cache from database when initializing.
        """
        start_time = int(time.time()) - self.keep_interval
        for lid, t, d in await self.store.get_recent_partials(start_time):
            self.cache[lid].add(t, d, remove=False)
            self.cache.all.add(t, d, remove=False)
        await self.store.scrub_pplns(start_time)

    def calculate_estimated_size(self, points):
        estimated_size = int(points / (self.pool_config['time_target'] * 1.0881482400062102e-15))
        if self.config['full_node']['selected_network'] == 'testnet7':
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
                        if points_interval.points != before:
                            to_update.append(launcher_id)
                    self.cache.all.add_partials(points_interval)

            now = int(time.time())
            for i in to_update:
                await self.cache.update_db(i, now)

    async def get_pool_size_and_etw(self):
        pool_size = self.calculate_estimated_size(self.cache.all.points)
        blockchain_space = self.pool.blockchain_state['space']
        proportion = pool_size / blockchain_space if blockchain_space else -1
        etw = int(await get_average_block_time(None) / proportion) if proportion else -1
        return pool_size, etw

    async def pool_estimated_size_loop(self):
        while True:
            try:
                estimated_size = self.calculate_estimated_size(self.cache.all.points)
                await self.store.set_pool_size(estimated_size)
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
                    six_hours_ago = time.time() - 3600 * 6
                    for launcher_id, rec in (await self.store.get_farmer_records([
                        ('email', 'IS NOT NULL', None),
                        ('notify_missing_partials_hours', 'IS NOT NULL', None),
                        ('notify_missing_partials_hours', '>', 0),
                    ])).items():
                        last_seen = new.get(launcher_id)
                        if not last_seen:
                            continue
                        # Farmers with low space (less than 300GiB) can take up to six hours
                        # to send partials
                        if rec.estimated_size < 322122547200:  # 300GiB
                            if last_seen > six_hours_ago:
                                continue
                        farmer_records[launcher_id] = rec
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

    async def add_partial(self, launcher_id: bytes32, timestamp: uint64, difficulty: uint64, error: Optional[str] = None):

        # Add to database
        await self.store.add_partial(launcher_id, timestamp, difficulty, error)

        # Add to the cache and compute the estimated farm size if a successful partial
        if error is None:
            await self.cache.add(launcher_id.hex(), timestamp, difficulty)

        if next(self.additions) % 10 == 0:
            await self.scrub()

    async def get_recent_partials(self, launcher_id: bytes32, number_of_partials: int):
        """
        Difficulty function expects descendent order.
        """
        return [
            (uint64(x[0]), uint64(x[1]))
            for x in reversed(self.cache[launcher_id.hex()].partials[-number_of_partials:])
        ]

    async def get_farmer_points_and_payout_instructions(self):
        launcher_id_and_ph = await self.store.get_launcher_id_and_payout_instructions(
            self.pool_config.get('reward_system')
        )
        points_and_ph = []
        await self.scrub()
        async with self.cache:
            for launcher_id, points_interval in self.cache.items():
                if points_interval.points == 0:
                    continue
                ph = launcher_id_and_ph.get(launcher_id)
                if ph is None:
                    logger.error(
                        'Did not find payout instructions for %r, points %d.',
                        launcher_id, points_interval.points,
                    )
                    continue
                points_and_ph.append((uint64(points_interval.points), ph))
        return points_and_ph, self.cache.all.points
