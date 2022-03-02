import asyncio
import concurrent.futures
import functools
import logging
import textwrap

from typing import Dict, Optional

from chia.protocols.pool_protocol import PostPartialPayload
from chia.util.ints import uint64

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

from ..task import task_exception

logger = logging.getLogger('influxdb_store')


class InfluxdbStore(object):
    def __init__(self, pool_config: Dict):
        self.pool_config = pool_config
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self._loop = asyncio.get_event_loop()
        self.bucket = self.pool_config['influxdb'].get('bucket', 'openchia')
        self.bucket_partial = self.pool_config['influxdb'].get('bucket_partial', 'openchia_partial')

    async def connect(self):
        self.client = InfluxDBClient(
            url=self.pool_config['influxdb']['url'],
            token=self.pool_config['influxdb']['token'],
            org=self.pool_config['influxdb']['org'],
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    async def _query(self, *args, **kwargs):
        return await self._loop.run_in_executor(
            self._executor, functools.partial(self.query_api.query, *args, **kwargs)
        )

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

    async def add_partial(
        self,
        partial_payload: PostPartialPayload,
        timestamp: uint64,
        difficulty: uint64,
        error: Optional[str] = None,
    ) -> None:
        p = Point('partial').time(int(timestamp), WritePrecision.S).tag(
            'launcher', partial_payload.launcher_id.hex()).tag(
            'harvester', partial_payload.harvester_id.hex()).tag(
            'error', error).field(
            'difficulty', int(difficulty))
        return await self._write(bucket=self.bucket_partial, record=p)

    async def add_xchprice(self, xch_price: Dict):
        p = Point('xchprice').field('usd', xch_price['usd']).field('eur', xch_price['eur']).field(
            'gbp', xch_price['gbp']).field('btc', xch_price['btc']).field('eth', xch_price['eth'])
        return await self._write(bucket=self.bucket, record=p)

    async def get_launcher_sizes(self, launcher_id: str, start: str):
        q = await self._query(
            textwrap.dedent('''from(bucket: "openchia")
              |> range(start: duration(v: _start), stop: now())
              |> filter(fn: (r) => r["_measurement"] == "launcher_size")
              |> filter(fn: (r) => r["_field"] == "size_8h")
              |> filter(fn: (r) => r["launcher"] == _launcher)'''),
            params={
                '_start': start,
                '_launcher': launcher_id,
            },
        )
        rv = []
        for table in q:
            for r in table.records:
                rv.append((r.get_time(), r.get_value()))
        return rv
