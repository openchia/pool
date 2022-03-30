import asyncio
import sys
import yaml

from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.ints import uint16
from chia.consensus.default_constants import DEFAULT_CONSTANTS

from pool.store.pgsql_store import PgsqlPoolStore
from pool.singleton import get_singleton_state

"""
Tool to check the singleton of a launcher
"""


async def main():
    with open('/data/config.yaml') as f:
        pool_config = yaml.safe_load(f)
    config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
    node_rpc_client = await FullNodeRpcClient.create(
        pool_config['nodes'][0]['hostname'], uint16(8555), DEFAULT_ROOT_PATH, config
    )

    overrides = config["network_overrides"]["constants"][
        config["selected_network"]
    ]
    constants = DEFAULT_CONSTANTS.replace_str_to_bytes(**overrides)

    store = PgsqlPoolStore(pool_config)
    await store.connect()

    lid = bytes32(bytes.fromhex(sys.argv[1]))
    blockchain_state = await node_rpc_client.get_blockchain_state()
    singleton = await get_singleton_state(
        node_rpc_client,
        lid,
        None,
        blockchain_state['peak'].height,
        32,
        constants.GENESIS_CHALLENGE,
    )
    print(singleton)

    node_rpc_client.close()
    await node_rpc_client.await_closed()
    await store.close()


if __name__ == '__main__':
    asyncio.run(main())
