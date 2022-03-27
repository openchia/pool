import asyncio
import yaml

from typing import List

from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.bech32m import decode_puzzle_hash
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.ints import uint16

from pool.store.pgsql_store import PgsqlPoolStore
from pool.singleton import find_reward_from_coinrecord

"""
Tool to check the blockchain for all rewards sent to the pool wallet.
It will print coins that were spent and not registered in the database.
"""


async def main():
    with open('/data/config.yaml') as f:
        pool_config = yaml.safe_load(f)
    config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
    node_rpc_client = await FullNodeRpcClient.create(
        pool_config['nodes'][0]['hostname'], uint16(8555), DEFAULT_ROOT_PATH, config
    )

    store = PgsqlPoolStore(pool_config)
    await store.connect()

    coin_records: List = await node_rpc_client.get_coin_records_by_puzzle_hash(
        bytes32(decode_puzzle_hash(pool_config['wallets'][0]['address'])),
        include_spent_coins=True,
        start_height=500000,
    )

    rewards = {i[0] for i in await store._execute("SELECT name from coin_reward")}

    for cr in sorted(coin_records, key=lambda x: int(x.confirmed_block_index)):
        if cr.coin.amount != 1750000000000:
            continue
        if cr.name.hex() in rewards:
            continue

        if await store.block_exists(cr.coin.parent_coin_info.hex()):
            continue

        result = await find_reward_from_coinrecord(
            node_rpc_client,
            store,
            cr,
        )
        if result:
            print(cr.name.hex())

    node_rpc_client.close()
    await node_rpc_client.await_closed()
    await store.close()


if __name__ == '__main__':
    asyncio.run(main())
