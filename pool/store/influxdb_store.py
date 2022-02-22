import asyncio
import concurrent.futures
import functools
import logging

from typing import Dict

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from ..task import task_exception

logger = logging.getLogger('influxdb_store')


class InfluxdbStore(object):
    def __init__(self, pool_config: Dict):
        self.pool_config = pool_config
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self._loop = asyncio.get_event_loop()
        self.bucket = self.pool_config['influxdb'].get('bucket', 'openchia')

    async def connect(self):
        self.client = InfluxDBClient(
            url=self.pool_config['influxdb']['url'],
            token=self.pool_config['influxdb']['token'],
            org=self.pool_config['influxdb']['org'],
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    async def _write(self, *args, **kwargs):
        return await self._loop.run_in_executor(
            self._executor, functools.partial(self.write_api.write, *args, **kwargs)
        )

    @task_exception
    async def add_launcher_size(self, launcher_id: str, size: int, size_8h: int):
        p = Point('launcher_size').tag('launcher', launcher_id).field(
            'size', size).field('size_8h', size_8h)
        return await self._write(bucket=self.bucket, record=p)

    async def add_pool_size(self, sizes: Dict[str, int]):
        p = Point('pool_size')
        for k, v in sizes.items():
            p = p.field(k, v)
        return await self._write(bucket=self.bucket, record=p)

    async def add_netspace(self, size: int):
        p = Point('netspace').field('size', size / 1024 / 1024)
        return await self._write(bucket=self.bucket, record=p)

    async def add_xchprice(self, xch_price: Dict):
        p = Point('xchprice').field('usd', xch_price['usd']).field('eur', xch_price['eur']).field(
            'gbp', xch_price['gbp']).field('btc', xch_price['btc']).field('eth', xch_price['eth'])
        return await self._write(bucket=self.bucket, record=p)
