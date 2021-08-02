import cachetools
import logging

from .store.abstract import AbstractPoolStore

from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.ints import uint64
from typing import Optional

logger = logging.getLogger('partials')


class PartialsCache(cachetools.LRUCache):

    def __init__(self, *args, maxsize_partials: int = 0, **kwargs):
        self.maxsize_partials = maxsize_partials
        super().__init__(*args, **kwargs)

    def __missing__(self, key: str):
        partials = {
            'additions': 0,
            'fifo': cachetools.FIFOCache(self.maxsize_partials),
        }
        self[key] = partials
        return partials


class Partials(object):

    def __init__(self, store: AbstractPoolStore, pool_config):
        self.store = store
        self.pool_config = pool_config

        # Up to 10 thousand launchers sending partials
        self.cache = PartialsCache(
            maxsize=10000,
            # Store up to two times of the expected number of partials
            maxsize_partials=pool_config['number_of_partials_target'] * 2,
        )

    async def add_partial(self, launcher_id: bytes32, timestamp: uint64, difficulty: uint64, error: Optional[str] = None):
        # If its a valid partial make sure we grab recents from database if its empty
        if error is None:
            partials = self.cache[launcher_id.hex()]
            if len(partials['fifo']) == 0:
                for ptime, difficulty in await self.store.get_recent_partials(
                    launcher_id, self.pool_config['number_of_partials_target'],
                ):
                    partials['fifo'][int(ptime)] = int(difficulty)

        # Add to database
        await self.store.add_partial(launcher_id, timestamp, difficulty, error)

        # Add to the cache and compute the estimated farm size if a successful partial
        if error is None:
            partials['additions'] += 1
            partials['fifo'][int(timestamp)] = int(difficulty)
            # Update every 10 partials
            if partials['additions'] == 10:
                partials['additions'] = 0
                last_time_target = int(timestamp) - self.pool_config['time_target']
                points = sum(map(
                    lambda x: x[1],
                    filter(lambda x: x[0] >= last_time_target, partials['fifo'].items()),
                ))
                estimated_size = int(points / (self.pool_config['time_target'] * 1.088e-15))
                logger.debug(
                    'Updating %r with last time_target points of %d (%.3f GiB)',
                    launcher_id.hex(),
                    points,
                    estimated_size / 1073741824,  # 1024 ^ 3
                )
                await self.store.update_estimated_size(launcher_id, estimated_size)
