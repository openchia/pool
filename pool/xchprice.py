import aiohttp
import asyncio
import json
import logging

logger = logging.getLogger('xchprice')


class XCHPrice(object):

    def __init__(self, store, store_ts):
        self.store = store
        self.store_ts = store_ts
        self.current_price = None

    async def loop(self):
        while True:
            try:
                async with aiohttp.request(
                    'GET',
                    'https://api.coingecko.com/api/v3/coins/chia',
                    params={
                        'localization': 'false',
                        'tickers': 'false',
                        'market_data': 'true',
                        'community_data': 'false',
                        'developer_data': 'false',
                        'sparkline': 'false',
                    },
                ) as r:
                    r.raise_for_status()
                    data = await r.json()
                    self.current_price = data['market_data']['current_price']
                    await self.store.set_globalinfo({'xch_current_price': json.dumps(self.current_price)})
                    await self.store_ts.add_xchprice(self.current_price)
            except Exception:
                logger.error('Failed to get XCH price', exc_info=True)
            await asyncio.sleep(120)
