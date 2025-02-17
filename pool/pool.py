import aiohttp
import asyncio
import dataclasses
import json
import itertools
import logging
import os
import pathlib
import shlex
import subprocess
import time
from asyncio import Task
from collections import defaultdict
from decimal import Decimal as D
from typing import Dict, Optional, Set, List, Tuple
from packaging.version import Version

from chia_rs import AugSchemeMPL, G1Element
from chia.pools.pool_wallet_info import PoolState, PoolSingletonState
from chia.protocols.pool_protocol import (
    PoolErrorCode,
    PostPartialRequest,
    PostPartialResponse,
    PostFarmerRequest,
    PostFarmerResponse,
    PutFarmerRequest,
    POOL_PROTOCOL_VERSION,
)
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.proof_of_space import verify_and_get_quality_string
from chia.types.coin_record import CoinRecord
from chia.types.coin_spend import CoinSpend
from chia.util.bech32m import decode_puzzle_hash
from chia.consensus.constants import ConsensusConstants
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.util.ints import uint8, uint16, uint32, uint64
from chia.util.byte_types import hexstr_to_bytes
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.streamable import Streamable
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.full_node.signage_point import SignagePoint
from chia.types.end_of_slot_bundle import EndOfSubSlotBundle
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.spend_bundle import estimate_fees
from chia.consensus.pot_iterations import calculate_iterations_quality
from chia.util.lru_cache import LRUCache
from chia.wallet.transaction_record import TransactionRecord
from chia.pools.pool_puzzles import (
    get_most_recent_singleton_coin_from_coin_spend,
    get_delayed_puz_info_from_launcher_spend,
    launcher_id_to_p2_puzzle_hash,
)

from .absorb_spend import NoCoinForFee
from .difficulty_adjustment import get_new_difficulty
from .fee import get_cost
from .launchers import Launchers
from .notifications import Notifications
from .partials import Partials
from .payment import subtract_fees, create_share
from .singleton import (
    create_absorb_transaction,
    get_singleton_state,
    get_coin_spend,
    find_reward_from_coinrecord,
)
from .store.influxdb_store import InfluxdbStore
from .store.pgsql_store import PgsqlPoolStore
from .record import FarmerRecord
from .task import task_exception
from .types import AbsorbFee, PaymentFee
from .util import (
    RequestMetadata,
    calculate_effort,
    create_transaction,
    error_dict,
    payment_targets_to_additions,
)
from .xchprice import XCHPrice

SECONDS_PER_BLOCK = (24 * 3600) / 4608
logger = logging.getLogger('pool')
plogger = logging.getLogger('partials')


class Pool:
    def __init__(self, pool_config: Dict):
        self.follow_singleton_tasks: Dict[bytes32, asyncio.Task] = {}
        self.log = logging.getLogger('pool')

        self.pool_config = pool_config

        # Set our pool info here
        self.info_default_res = pool_config["pool_info"]["default_res"]
        self.info_name = pool_config["pool_info"]["name"]
        self.info_logo_url = pool_config["pool_info"]["logo_url"]
        self.info_description = pool_config["pool_info"]["description"]
        self.welcome_message = pool_config["welcome_message"]

        self.config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
        overrides = self.config["network_overrides"]["constants"][
            self.config["selected_network"]
        ]
        self.constants: ConsensusConstants = DEFAULT_CONSTANTS.replace_str_to_bytes(**overrides)

        self.store = PgsqlPoolStore(pool_config)
        self.store_ts = InfluxdbStore(pool_config)
        self.notifications = Notifications(self)
        self.partials = Partials(self)

        fee: Dict = pool_config.get('fee') or {}

        self.pool_fee: float = fee['pool']
        self.mojos_per_cost: int = fee.get('mojos_per_cost') or 5
        self.stay_fee_discount: int = fee.get('stay_discount') or 0
        self.stay_fee_length: float = fee.get('stay_length') or 0.0
        self.size_fee_discount: Dict[int, float] = fee.get('size_discount') or {}
        self.max_fee_discount: D = D(fee.get('max_discount') or 0.0)

        # The pool fees will be sent to this address.
        # This MUST be on a different key than the target_puzzle_hash.
        self.pool_fee_puzzle_hash: bytes32 = bytes32(decode_puzzle_hash(fee["address"]))
        self.testnet: bool = fee["address"].startswith('txch')

        # may be False, True or "auto"
        payment_fee = fee.get('payment')
        payment_fee_t = str(payment_fee).upper()
        try:
            self.payment_fee: PaymentFee = PaymentFee.__members__[payment_fee_t]
        except KeyError:
            raise RuntimeError(
                f'Invalid payment_fee: {payment_fee}. Valid values: '
                ', '.join(list(PaymentFee.__members__.keys()))
            )
        self.payment_fee_absolute: int = fee.get('payment_absolute') or 0

        # may be False, True or "auto"
        absorb_fee = fee.get('absorb')
        absorb_fee_t = str(absorb_fee).upper()
        try:
            self.absorb_fee: AbsorbFee = AbsorbFee.__members__[absorb_fee_t]
        except KeyError:
            raise RuntimeError(
                f'Invalid absorb_fee: {absorb_fee}. Valid values: '
                ', '.join(list(AbsorbFee.__members__.keys()))
            )

        self.absorb_fee_absolute: Optional[int] = fee.get('absorb_absolute')

        # This number should be held constant and be consistent for every pool in the network. DO NOT CHANGE
        self.iters_limit = self.constants.POOL_SUB_SLOT_ITERS // 64

        # This number should not be changed, since users will put this into their singletons
        self.relative_lock_height = uint32(pool_config["relative_lock_height"])

        # This is what the user enters into the input field.
        # This exact value will be stored on the blockchain.
        self.pool_url = pool_config["pool_url"]
        # 10 difficulty is about 1 proof a day per plot
        self.min_difficulty = uint64(pool_config["min_difficulty"])
        self.default_difficulty: uint64 = uint64(pool_config["default_difficulty"])

        self.pending_point_partials: asyncio.Queue = asyncio.Queue()
        self.recent_points_added: LRUCache = LRUCache(20000)

        self.recent_signage_point: LRUCache = LRUCache(1000)
        self.recent_eos: LRUCache = LRUCache(1000)

        # The time in minutes for an authentication token to be valid.
        self.authentication_token_timeout: uint8 = pool_config["authentication_token_timeout"]

        # This is where the block rewards will get paid out to. The pool needs to support this address forever,
        # since the farmers will encode it into their singleton on the blockchain.

        self.default_target_puzzle_hashes: List[bytes32] = []
        self.wallets = []
        for wallet in pool_config["wallets"]:
            wallet['puzzle_hash'] = bytes32(decode_puzzle_hash(wallet['address']))
            self.default_target_puzzle_hashes.append(wallet['puzzle_hash'])
            wallet['hostname'] = wallet.get("hostname") or self.config["self_hostname"]
            wallet['ssl_dir'] = wallet.get("ssl_dir")
            wallet['synced'] = False
            wallet['balance'] = None
            self.wallets.append(wallet)

        self.min_payment: int = pool_config.get('min_payment', 0)

        # Workaround for adding a block for a coin that was already spent from
        # the wallet puzzle hash
        self.absorbed_extra_coins: List[bytes32] = [
            bytes32(bytes.fromhex(i))
            for i in (pool_config.get('absorbed_extra_coins') or [])
        ]

        # We need to check for slow farmers. If farmers cannot submit proofs in time, they won't be able to win
        # any rewards either. This number can be tweaked to be more or less strict. More strict ensures everyone
        # gets high rewards, but it might cause some of the slower farmers to not be able to participate in the pool.
        self.partial_time_limit: int = pool_config["partial_time_limit"]

        # There is always a risk of a reorg, in which case we cannot reward farmers that submitted partials in that
        # reorg. That is why we have a time delay before changing any account points.
        self.partial_confirmation_delay: int = pool_config["partial_confirmation_delay"]

        # Only allow PUT /farmer per launcher_id every n seconds to prevent difficulty change attacks.
        self.farmer_update_blocked: set = set()
        self.farmer_update_cooldown_seconds: int = 600

        # These are the phs that we want to look for on chain, that we can claim to our pool
        self.scan_p2_singleton_puzzle_hashes: Set[bytes32] = set()

        # Don't scan anything before this height, for efficiency (for example pool start date)
        self.scan_start_height: uint32 = uint32(pool_config["scan_start_height"])
        self.scan_current_height: uint32 = self.scan_start_height
        self.scan_move_collect_pending: bool = True
        self.scan_move_payment_pending: bool = True

        # Interval for scanning and collecting the pool rewards
        self.collect_pool_rewards_interval = pool_config["collect_pool_rewards_interval"]

        # After this many confirmations, a transaction is considered final and irreversible
        self.confirmation_security_threshold = pool_config["confirmation_security_threshold"]

        # Interval for making payout transactions to farmers
        self.payment_interval = pool_config["payment_interval"]

        # We will not make transactions with more targets than this, to ensure our transaction gets into the blockchain
        # faster.
        self.max_additions_per_transaction = pool_config["max_additions_per_transaction"]

        # We target these many partials for this number of seconds. We adjust after receiving this many partials.
        self.number_of_partials_target: int = pool_config["number_of_partials_target"]
        self.time_target: int = pool_config["time_target"]

        self.launchers_banned: Dict[str, str] = pool_config.get('launchers_banned') or {}

        self.launcher_lock: Dict[bytes32, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Tasks (infinite While loops) for different purposes
        self.confirm_partials_loop_task: Optional[asyncio.Task] = None
        self.collect_pool_rewards_loop_task: Optional[asyncio.Task] = None
        self.create_payment_loop_tasks: List[asyncio.Task] = []
        self.submit_payment_loop_tasks: List[asyncio.Task] = []
        self.get_peak_loop_task: Optional[asyncio.Task] = None
        self.xchprice_loop_task: Optional[asyncio.Task] = None

        self.node_rpc_client: Optional[FullNodeRpcClient] = None
        self.nodes: List[Dict] = []
        for node in pool_config['nodes']:
            self.nodes.append({
                'hostname': node['hostname'],
                'rpc_port': node.get('rpc_port') or 8555,
                'ssl_dir': node.get('ssl_dir'),
                'rpc_client': None,
                # Keeps track of the latest state of our node
                'blockchain_state': {'peak': None},
                'blockchain_mempool_full_pct': 0,

            })

        # Keeps track of the latest state of our node
        self.blockchain_state: Dict = {"peak": None}
        self.blockchain_mempool_full_pct: int = 0

    async def start(self):
        await self.store.connect()
        await self.store_ts.connect()
        await self.partials.load_from_store()

        working_node = False
        for node in list(self.nodes):
            args = [node['hostname'], uint16(node['rpc_port'])]
            if node['ssl_dir']:
                args += [
                    pathlib.Path(node['ssl_dir']),
                    {
                        'private_ssl_ca': {
                            'crt': 'ca/private_ca.crt',
                            'key': 'ca/private_ca.key',
                        },
                        'daemon_ssl': {
                            'private_crt': 'daemon/private_daemon.crt',
                            'private_key': 'daemon/private_daemon.key',
                        },
                    },
                ]
            else:
                args += [DEFAULT_ROOT_PATH, self.config]

            try:
                node['rpc_client'] = await FullNodeRpcClient.create(*args)
            except Exception:
                self.log.error(
                    'Failed to create connection to %s. Removing it.',
                    node['hostname'],
                    exc_info=True,
                )
                self.nodes.remove(node)
            else:
                working_node = True

        if not working_node:
            raise RuntimeError('Unable to create node client, exiting.')

        for wallet in self.wallets:
            if wallet['ssl_dir']:
                wallet['rpc_client'] = await WalletRpcClient.create(
                    wallet['hostname'],
                    uint16(wallet['rpc_port']),
                    pathlib.Path(wallet['ssl_dir']),
                    {
                        'private_ssl_ca': {
                            'crt': 'private_ca.crt',
                            'key': 'private_ca.key',
                        },
                        'daemon_ssl': {
                            'private_crt': 'private_daemon.crt',
                            'private_key': 'private_daemon.key',
                        },
                    },
                )
            else:
                wallet['rpc_client'] = await WalletRpcClient.create(
                    wallet['hostname'], uint16(wallet['rpc_port']), DEFAULT_ROOT_PATH, self.config
                )
            try:
                wallet['synced'] = await wallet['rpc_client'].get_synced()
            except Exception:
                wallet['synced'] = False

        working_node = None
        while not working_node:
            for node in self.nodes:
                try:
                    node['blockchain_state'] = await node['rpc_client'].get_blockchain_state()
                except aiohttp.client_exceptions.ClientConnectorError as e:
                    self.log.error(
                        'Failing to connect to node %r, retrying in 2 seconds: %s',
                        node['hostname'],
                        e,
                    )
                else:
                    # Select the first by default
                    if not working_node:
                        working_node = node
            if not working_node:
                await asyncio.sleep(2)
        self.node_rpc_client = working_node['rpc_client']
        self.blockchain_state = working_node['blockchain_state']

        try:
            for wallet in self.wallets:
                res = await wallet['rpc_client'].log_in(
                    fingerprint=wallet['fingerprint']
                )
                if not res["success"]:
                    raise ValueError(f"Error logging in: {res['error']}. Make sure your config fingerprint is correct.")
                self.log.info(f"Logging in: {res}")
                res = await wallet['rpc_client'].get_wallet_balance(wallet['id'])
                self.log.info(f"Obtaining balance: {res}")
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.log.error('Failed to connect to the wallet %s: %s', wallet['fingerprint'], e)

        self.scan_p2_singleton_puzzle_hashes = await self.store.get_pay_to_singleton_phs()

        self.confirm_partials_loop_task = asyncio.create_task(self.confirm_partials_loop())
        self.collect_pool_rewards_loop_task = asyncio.create_task(self.collect_pool_rewards_loop())
        for wallet in self.wallets:
            self.create_payment_loop_tasks.append(
                asyncio.create_task(self.create_payment_loop(wallet))
            )
            self.submit_payment_loop_tasks.append(
                asyncio.create_task(self.submit_payment_loop(wallet))
            )
        self.get_peak_loop_task = asyncio.create_task(self.get_peak_loop())

        self.launchers = Launchers(self)
        await self.partials.start(self.launchers)
        await self.launchers.start()

        self.xchprice_loop_task = asyncio.create_task(XCHPrice(self.store, self.store_ts).loop())

        await self.notifications.start()

    async def stop(self):
        if self.confirm_partials_loop_task is not None:
            self.confirm_partials_loop_task.cancel()
        if self.collect_pool_rewards_loop_task is not None:
            self.collect_pool_rewards_loop_task.cancel()
        for create_payment_loop_task in self.create_payment_loop_tasks:
            create_payment_loop_task.cancel()
        for submit_payment_loop_task in self.submit_payment_loop_tasks:
            submit_payment_loop_task.cancel()
        if self.get_peak_loop_task is not None:
            self.get_peak_loop_task.cancel()

        await self.partials.stop()
        await self.launchers.stop()

        if self.xchprice_loop_task is not None:
            self.xchprice_loop_task.cancel()

        # Await task that can use database connection
        await self.confirm_partials_loop_task

        for wallet in self.wallets:
            wallet['rpc_client'].close()
            await wallet['rpc_client'].await_closed()

        for node in self.nodes:
            if node['rpc_client']:
                node['rpc_client'].close()
                await node['rpc_client'].await_closed()

        await self.store.close()

    async def run_hook(self, name, *args):
        hooks = self.pool_config.get('hooks', {}).get(name)
        if not hooks:
            logger.debug('Hook %r not configured', name)
            return

        def dump(item):
            if isinstance(item, Streamable):
                return item.to_json_dict()
                return [dump(i) for i in item]
            elif isinstance(item, (list, tuple)):
                return [dump(i) for i in item]
            elif isinstance(item, dict):
                return {dump(k): dump(v) for k, v in item.items()}
            else:
                return item

        if isinstance(hooks, str):
            hooks = [hooks]

        for hook in hooks:

            hook = shlex.split(hook)
            if not os.path.exists(hook[0]):
                logger.error('Hook %r does not exist', hook)
                continue

            final_args = tuple(
                hook + [name.upper()] + [json.dumps(dump(i)) for i in args]
            )

            logger.debug('Running hook %r with args %r', hook, final_args)
            asyncio.ensure_future(self._run_hook_proc(hook, final_args))

    async def _run_hook_proc(self, hook, final_args):
        env = os.environ.copy()
        env['CONFIG_PATH'] = self.pool_config['__path__']
        env['PYTHONPATH'] = '.'
        proc = await asyncio.create_subprocess_exec(
            *final_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 30)
        except asyncio.TimeoutError:
            await proc.kill()
            stdout, stderr = await proc.communicate()
            logger.warning('Hook %r killed after 30 seconds: %r', hook, stdout)
        if proc.returncode != 0:
            logger.warning('Hook %r returned %d: %r', hook, proc.returncode, stdout)
        else:
            logger.debug('Hook %r returned %d: %r', hook, proc.returncode, stdout)

    def set_healthy_node(self, switch=False):
        higher_peak = None
        cur_node = None
        for node in self.nodes:
            if not (node['blockchain_state'].get('sync') or {}).get('synced'):
                logger.warning('Node %r not synced', node['hostname'])
                continue
            if switch is True:
                # If we are switching always choose a different one
                if node['rpc_client'] == self.node_rpc_client:
                    continue
            if higher_peak is None:
                higher_peak = node['blockchain_state']['peak'].height
                cur_node = node
            elif node['blockchain_state']['peak'].height > higher_peak:
                higher_peak = node['blockchain_state']['peak'].height
                cur_node = node
        if cur_node is None:
            raise RuntimeError('No healthy node available')

        if self.node_rpc_client != cur_node['rpc_client']:
            self.log.warning('Switching to node %r', cur_node['hostname'])
            self.node_rpc_client = cur_node['rpc_client']
        self.blockchain_state = cur_node['blockchain_state']
        self.blockchain_mempool_full_pct = cur_node['blockchain_mempool_full_pct']

    @task_exception
    async def get_peak_loop(self):
        """
        Periodically contacts the full node to get the latest state of the blockchain
        """
        while True:
            try:
                working_node = False
                for node in self.nodes:
                    try:
                        node['blockchain_state'] = await node['rpc_client'].get_blockchain_state()
                        node['blockchain_mempool_full_pct'] = int((
                            node['blockchain_state']['mempool_cost'] /
                            node['blockchain_state']['mempool_max_total_cost']
                        ) * 100)
                    except Exception:
                        self.log.warning(
                            'Failed to get blockchain state for node %r',
                            node['hostname'],
                            exc_info=True,
                        )
                    else:
                        working_node = True

                if not working_node:
                    self.log.error('Unable to get blockchain state from any node.')
                    await asyncio.sleep(15)
                    continue

                self.set_healthy_node()

                if not self.scan_move_collect_pending and not self.scan_move_payment_pending and self.blockchain_state['peak'].height > self.scan_current_height:
                    new_scan_height = self.blockchain_state['peak'].height - 500
                    if new_scan_height > self.scan_current_height:
                        self.scan_current_height = uint32(new_scan_height)

                # Get the wallets as last since its not absolutely critical for pool operation
                for wallet in self.wallets:
                    try:
                        wallet['synced'] = await wallet['rpc_client'].get_synced()
                        wallet['balance'] = await wallet['rpc_client'].get_wallet_balance(
                            str(wallet['id'])
                        )
                    except aiohttp.client_exceptions.ClientConnectorError as e:
                        self.log.error(
                            'Failed to connect to wallet %s: %s', wallet['fingerprint'], e
                        )

                asyncio.create_task(self.store.set_globalinfo({
                    'blockchain_height': self.blockchain_state['peak'].height,
                    'blockchain_space': self.blockchain_state['space'],
                    'blockchain_avg_block_time': await self.get_average_block_time(),
                    'wallets': json.dumps([
                        {'address': i['address'], 'balance': i['balance'], 'synced': i['synced']}
                        for i in self.wallets
                    ]),
                }))

                asyncio.create_task(
                    self.store_ts.add_netspace(int(self.blockchain_state['space']))
                )
                asyncio.create_task(
                    self.store_ts.add_mempool(
                        int(self.blockchain_state['mempool_size']),
                        int(self.blockchain_state['mempool_cost']),
                        int(self.blockchain_state['mempool_max_total_cost']),
                    )
                )

                # Sleep less than 30 so there is time to switch nodes for checking partial
                await asyncio.sleep(25)
            except asyncio.CancelledError:
                self.log.info("Cancelled get_peak_loop, closing")
                return
            except Exception as e:
                self.log.error(f"Unexpected error in get_peak_loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def get_average_block_time(self):
        blocks_to_compare = 500
        curr = self.blockchain_state["peak"]
        if curr is None or curr.height < (blocks_to_compare + 100):
            return SECONDS_PER_BLOCK
        while curr is not None and curr.height > 0 and not curr.is_transaction_block:
            curr = await self.node_rpc_client.get_block_record(curr.prev_hash)
        if curr is None:
            return SECONDS_PER_BLOCK

        past_curr = await self.node_rpc_client.get_block_record_by_height(
            curr.height - blocks_to_compare
        )
        while past_curr is not None and past_curr.height > 0 and not past_curr.is_transaction_block:
            past_curr = await self.node_rpc_client.get_block_record(past_curr.prev_hash)
        if past_curr is None:
            return SECONDS_PER_BLOCK

        return (curr.timestamp - past_curr.timestamp) / (curr.height - past_curr.height)

    async def get_etw(self, size):
        blockchain_space = self.blockchain_state['space']
        proportion = size / blockchain_space if blockchain_space else -1
        etw = int(await self.get_average_block_time() / proportion) if proportion else -1
        return etw

    @task_exception
    async def collect_pool_rewards_loop(self):
        """
        Iterates through the blockchain, looking for pool rewards, and claims them, creating a transaction to the
        pool's puzzle_hash.
        """

        while True:
            try:
                if not self.blockchain_state["sync"]["synced"]:
                    await asyncio.sleep(60)
                    continue

                scan_phs: List[bytes32] = list(self.scan_p2_singleton_puzzle_hashes)
                peak_height = self.blockchain_state["peak"].height

                coin_records: List[CoinRecord] = []
                scan_per_round = 200
                for i in range(0, len(scan_phs), scan_per_round):
                    coin_records += await self.node_rpc_client.get_coin_records_by_puzzle_hashes(
                        scan_phs[i:i + scan_per_round],
                        include_spent_coins=False,
                        start_height=self.scan_current_height,
                    )
                    # Sleep a little to possibly not overload the node
                    await asyncio.sleep(2)

                self.log.info(
                    f"Scanning for block rewards from {self.scan_current_height} to {peak_height}. "
                    f"Found: {len(coin_records)}"
                )
                ph_to_amounts: Dict[bytes32, int] = {}
                valid_coin_records: List[CoinRecord] = []
                for cr in coin_records:
                    if not cr.coinbase:
                        self.log.info(f"Non coinbase coin: {cr.coin}, ignoring")
                        continue

                    if cr.coin.puzzle_hash not in ph_to_amounts:
                        ph_to_amounts[cr.coin.puzzle_hash] = 0
                    ph_to_amounts[cr.coin.puzzle_hash] += cr.coin.amount
                    valid_coin_records.append(cr)

                # For each p2sph, get the FarmerRecords
                ph_to_farmer = {
                    i.p2_singleton_puzzle_hash: i
                    for i in await self.store.get_farmer_records_for_p2_singleton_phs(
                        set(ph_to_amounts.keys())
                    )
                }

                # For each singleton, create, submit, and save a claim transaction
                claimable_amounts = 0
                not_claimable_amounts = 0
                for rec in ph_to_farmer.values():
                    if rec.is_pool_member:
                        claimable_amounts += ph_to_amounts[rec.p2_singleton_puzzle_hash]
                    else:
                        not_claimable_amounts += ph_to_amounts[rec.p2_singleton_puzzle_hash]
                    self.log.info(
                        '%s from %s: %s',
                        'Claimable' if rec.is_pool_member else 'Not claimable',
                        rec.launcher_id.hex(),
                        ph_to_amounts[rec.p2_singleton_puzzle_hash] / (10 ** 12),
                    )

                if len(coin_records) > 0:
                    self.log.info(f"Claimable amount: {claimable_amounts / (10**12)}")
                    self.log.info(f"Not claimable amount: {not_claimable_amounts / (10**12)}")

                if claimable_amounts == 0:
                    self.scan_move_collect_pending = False
                    await asyncio.sleep(self.collect_pool_rewards_interval)
                    continue
                else:
                    self.scan_move_collect_pending = True

                farmers_seen = set()
                used_fee_coins = []

                # Absorb coins in chronological order (farmed block)
                for cr in sorted(
                    valid_coin_records,
                    key=lambda x: int.from_bytes(
                        bytes(x.coin.parent_coin_info)[16:], 'big'
                    ),
                ):

                    rec = ph_to_farmer.get(cr.coin.puzzle_hash)
                    if rec is None:
                        self.log.error('Could not find farmer for %r', cr.coin.puzzle_hash)
                        continue

                    # Absorb only one coin at a time per farmer.
                    # That is because the singleton will change for each absorbeb coin.
                    # Also We cannot exceed max block cost
                    # (number of absorbeb coins per transaction).
                    if rec.launcher_id in farmers_seen:
                        continue

                    if not rec.is_pool_member:
                        # TODO: Fix if this coin record is assigned to the wallet pool?
                        continue

                    singleton_tip: Optional[Coin] = get_most_recent_singleton_coin_from_coin_spend(
                        rec.singleton_tip
                    )
                    if singleton_tip is None:
                        continue

                    singleton_coin_record: Optional[
                        CoinRecord
                    ] = await self.node_rpc_client.get_coin_record_by_name(singleton_tip.name())
                    if singleton_coin_record is None:
                        continue
                    if singleton_coin_record.spent:
                        asyncio.create_task(self.get_and_validate_singleton_state(rec.launcher_id))
                        self.log.warning(
                            f"Singleton coin {singleton_coin_record.coin.name()} is spent, will not "
                            f"claim rewards"
                        )
                        farmers_seen.add(rec.launcher_id)
                        continue

                    try:
                        spend_bundle = await create_absorb_transaction(
                            self.node_rpc_client,
                            self.wallets,
                            rec,
                            self.blockchain_state["peak"].height,
                            [cr],
                            self.absorb_fee,
                            self.absorb_fee_absolute,
                            used_fee_coins,
                            self.blockchain_mempool_full_pct,
                            self.mojos_per_cost,
                            self.constants,
                        )
                    except NoCoinForFee as e:
                        self.log.error(
                            'No coin in pool wallet for absorb fee (%s). '
                            'Retrying without fee.',
                            e,
                        )
                        spend_bundle = await create_absorb_transaction(
                            self.node_rpc_client,
                            self.wallets,
                            rec,
                            self.blockchain_state["peak"].height,
                            [cr],
                            False,
                            0,
                            None,
                            self.blockchain_mempool_full_pct,
                            self.mojos_per_cost,
                            self.constants,
                        )

                    if spend_bundle is None:
                        continue

                    push_tx_response: Dict = await self.node_rpc_client.push_tx(spend_bundle)
                    if push_tx_response["status"] == "SUCCESS":
                        # See farmers_seen comment above
                        farmers_seen.add(rec.launcher_id)

                        self.log.info(
                            "Submitted transaction successfully %r with fee %r",
                            spend_bundle.name().hex(),
                            estimate_fees(spend_bundle),
                        )

                        # Best effort to make sure coins show in the same order in the wallet
                        # (confirmed block index)
                        await asyncio.sleep(30)
                    else:
                        self.log.error(f"Error submitting transaction: {push_tx_response}")

                await asyncio.sleep(self.collect_pool_rewards_interval)
            except asyncio.CancelledError:
                self.log.info("Cancelled collect_pool_rewards_loop, closing")
                return
            except Exception:
                self.log.error("Unexpected error in collect_pool_rewards_loop", exc_info=True)
                await asyncio.sleep(5)

    @task_exception
    async def create_payment_loop(self, wallet):
        """
        Calculates the points of each farmer, and splits the total funds received into coins for each farmer.
        Saves the transactions that we should make, to `amount_to_distribute`.
        """
        while True:
            try:
                if not self.blockchain_state["sync"]["synced"]:
                    self.log.warning("Not synced, waiting")
                    await asyncio.sleep(60)
                    continue

                peak_height = self.blockchain_state["peak"].height
                coin_records: List[CoinRecord] = await self.node_rpc_client.get_coin_records_by_puzzle_hash(
                    wallet['puzzle_hash'],
                    include_spent_coins=False,
                    start_height=self.scan_current_height,
                )

                if self.absorbed_extra_coins:
                    for cr in await self.node_rpc_client.get_coin_records_by_names(
                        self.absorbed_extra_coins,
                        include_spent_coins=True,
                        start_height=self.scan_start_height,
                    ):
                        if cr.coin.puzzle_hash == wallet['puzzle_hash']:
                            coin_records.append(cr)

                pending_payments_coins: List[str] = await self.store.get_pending_payments_coins(
                    wallet['puzzle_hash']
                )

                total_amount_claimed: int = 0
                absorbs: List = []
                for c in sorted(coin_records, key=lambda x: x.confirmed_block_index):

                    if c.coin.amount == 0:
                        coin_records.remove(c)
                        continue

                    if c.confirmed_block_index > peak_height - self.confirmation_security_threshold:
                        # Skip unburied coins
                        self.log.debug('Coin %r is buried, skipping.', c)
                        coin_records.remove(c)
                        continue

                    # That coin was already registered in a payment that is pending
                    if c.name.hex() in pending_payments_coins:
                        coin_records.remove(c)
                        continue

                    real_coin = c
                    # if not await self.store.block_exists(c.coin.parent_coin_info.hex()):
                    #     # Check if its a double spend to absorb with a fee
                    #     parent_coin_record: Optional[CoinRecord] = (
                    #         await self.node_rpc_client.get_coin_record_by_name(
                    #             c.coin.parent_coin_info,
                    #         )
                    #     )
                    #     # The parent coin would have same puzzle hash of the wallet
                    #     if parent_coin_record and parent_coin_record.spent and parent_coin_record.coin.puzzle_hash == wallet['puzzle_hash']:
                    #         c = parent_coin_record

                    if not await self.store.block_exists(c.coin.parent_coin_info.hex()):

                        result = None
                        try:
                            result = await find_reward_from_coinrecord(
                                self.node_rpc_client,
                                self.store,
                                c,
                            )
                        except Exception:
                            self.log.error('Failed to find absorb', exc_info=True)

                        if result is not None:
                            # Create block record in database later so we can order by
                            # farmed height.
                            reward_coin, farmer = result
                            absorbs.append((reward_coin, farmer, c.coin.parent_coin_info))
                        else:
                            coin_records.remove(real_coin)
                            self.log.info(
                                "Coin %s not in singleton, skipping", c.coin.amount / (10 ** 12)
                            )
                            continue

                    total_amount_claimed += real_coin.coin.amount

                if absorbs:
                    pool_size = await self.partials.get_pool_size()
                    pool_etw = await self.get_etw(pool_size)

                    hook_args = []
                    for reward, farmer, singleton in sorted(
                        absorbs,
                        key=lambda x: int.from_bytes(
                            bytes(x[0].coin.parent_coin_info)[16:], 'big'
                        ),
                    ):
                        self.log.info('New coin farmed by %r', farmer.launcher_id.hex())

                        launcher_etw = await self.get_etw(farmer.estimated_size)
                        last_launcher_etw = farmer.last_block_etw or -1

                        last_launcher_timestamp = await self.launchers.last_reward_update(farmer)
                        if not last_launcher_timestamp:
                            launcher_effort = -1
                        else:
                            launcher_effort = int(calculate_effort(
                                last_launcher_etw,
                                int(last_launcher_timestamp),
                                launcher_etw,
                                int(reward.timestamp),
                            ))
                            if launcher_effort < 0:
                                launcher_effort = -1

                        hook_args.append((reward, farmer))

                        await self.store.add_block(
                            reward,
                            0,
                            singleton,
                            farmer,
                            launcher_etw,
                            launcher_effort,
                            pool_size,
                            pool_etw,
                        )
                        await self.store_ts.add_block(
                            reward,
                            0,
                            singleton,
                            farmer,
                            launcher_etw,
                            launcher_effort,
                            pool_size,
                            pool_etw,
                        )
                    await self.run_hook('absorb', hook_args)

                if len(coin_records) == 0:
                    self.scan_move_payment_pending = False
                    self.log.info("No funds to distribute (wallet %s).", wallet['fingerprint'])
                    await asyncio.sleep(self.payment_interval)
                    continue

                self.scan_move_payment_pending = True

                self.log.info(f"Total amount claimed: {total_amount_claimed / (10 ** 12)}")

                async with self.store.lock:
                    # Get the points of each farmer, as well as payout instructions.
                    if self.pool_config.get('reward_system') == 'PPLNS':
                        points_data, total_points = await self.partials.get_farmer_points_data()
                    else:
                        points_data: List[dict] = await self.store.get_farmer_points_data()
                        total_points = sum([pd['points'] for pd in points_data])

                    share = await create_share(
                        self.store,
                        total_amount_claimed,
                        total_points,
                        points_data,
                        self.pool_fee,
                        self.stay_fee_discount,
                        self.stay_fee_length,
                        self.size_fee_discount,
                        self.max_fee_discount,
                    )

                    if share:

                        if share['pool_fee_amount'] < 0:
                            raise RuntimeError(
                                f'Pool fee amount is negative: {share["pool_fee_amount"] / (10 ** 12)}'
                            )

                        if share['referral_fee_amount'] < 0:
                            raise RuntimeError(
                                f'Referral amount is negative: {share["referral_fee_amount"] / (10 ** 12)}'
                            )

                        self.log.info(
                            'Pool fee amount %r', share['pool_fee_amount'] / (10 ** 12),
                        )
                        self.log.info(
                            'Referral fee amount %r', share['referral_fee_amount'] / (10 ** 12),
                        )
                        self.log.info(
                            'Total amount to distribute %r',
                            share['amount_to_distribute'] / (10 ** 12),
                        )
                        self.log.info(
                            'Remainings: %r', share['remainings'] / (10 ** 12),
                        )
                        # If more than 0.00001 was left behind there is something off
                        if share['remainings'] > 10 ** 7:
                            self.log.error('Remainings too high, aborting.')
                            await asyncio.sleep(60)
                            continue

                        await self.store.add_payout(
                            coin_records,
                            wallet['puzzle_hash'],
                            self.pool_fee_puzzle_hash,
                            total_amount_claimed,
                            share['pool_fee_amount'],
                            share['referral_fee_amount'],
                            [dict(v, puzzle_hash=k) for k, v in share['additions'].items()],
                        )

                        # Subtract the points from each farmer
                        await self.store.clear_farmer_points()
                    else:
                        self.log.info(f"No points for any farmer. Waiting {self.payment_interval}")

                await asyncio.sleep(self.payment_interval)
            except asyncio.CancelledError:
                self.log.info(
                    "Cancelled create_payments_loop (wallet %s), closing", wallet['fingerprint']
                )
                return
            except Exception as e:
                self.log.error(f"Unexpected error in create_payments_loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    @task_exception
    async def submit_payment_loop(self, wallet):
        while True:
            try:
                peak_height = self.blockchain_state["peak"].height
                try:
                    await wallet['rpc_client'].log_in(fingerprint=wallet['fingerprint'])
                except aiohttp.client_exceptions.ClientConnectorError:
                    self.log.warning(
                        'Failed to connect to wallet %s, retrying in 30 seconds',
                        wallet['fingerprint'],
                    )
                    await asyncio.sleep(30)
                    continue

                if not self.blockchain_state["sync"]["synced"] or not wallet['synced']:
                    self.log.warning("Waiting for wallet %s sync", wallet['fingerprint'])
                    await asyncio.sleep(30)
                    continue

                # Lets make sure we dont get the payments while its being added to database
                async with self.store.lock:
                    payment_targets_per_tx = await self.store.get_pending_payment_targets(
                        wallet['puzzle_hash'],
                    )

                if not payment_targets_per_tx:
                    await asyncio.sleep(60)
                    continue

                for tx_id, payment_targets in payment_targets_per_tx.items():

                    if tx_id:
                        try:
                            transaction = await wallet['rpc_client'].get_transaction(
                                wallet['id'], tx_id
                            )
                            # Transaction already exists, lets readjust transaction fee
                            transation_phs = set()
                            unaccounted_amount = None
                            for addition_coin in transaction.additions:
                                transation_phs.add(addition_coin.puzzle_hash)
                                target: List = payment_targets.get(addition_coin.puzzle_hash)
                                if target:
                                    amounts = sum(map(lambda x: x['amount'], target))
                                    fee_per_payout = (amounts - int(addition_coin.amount)) / amounts
                                    if fee_per_payout:
                                        for t in target:
                                            t['tx_fee'] = fee_per_payout
                                            t['amount'] -= fee_per_payout
                                else:
                                    if unaccounted_amount is None:
                                        # TODO: Make sure this address belong to pool wallet
                                        unaccounted_amount = int(addition_coin.amount)
                                    else:
                                        raise RuntimeError('More than one change coin %r', addition_coin)
                            # Remove payment_targets not in the transaction
                            for ph in (set(payment_targets.keys()) - transation_phs):
                                payment_targets.pop(ph)

                        except ValueError as e:
                            if 'not found' in str(e):
                                self.log.info(f'Transaction {tx_id} not found, removing.')
                                await self.store.remove_transaction(tx_id)
                                tx_id = None

                    if tx_id is None:

                        # Only allow launcher minimum payment for the first pool wallet
                        enable_launcher_min_payment = (
                            self.wallets[0]['puzzle_hash'] == wallet['puzzle_hash']
                        )
                        additions = payment_targets_to_additions(
                            payment_targets,
                            self.min_payment,
                            launcher_min_payment=enable_launcher_min_payment,
                            limit=self.max_additions_per_transaction,
                        )

                        if additions and self.payment_fee == PaymentFee.AUTO:
                            payment_fee = self.blockchain_mempool_full_pct > 10
                            self.log.info('Payment fee is AUTO. Fees? %r', payment_fee)
                        else:
                            payment_fee = self.payment_fee == PaymentFee.TRUE

                        if payment_fee and additions:
                            if not self.payment_fee_absolute:
                                additions, blockchain_fee = await subtract_fees(
                                    wallet['rpc_client'],
                                    self.blockchain_state['peak'].height,
                                    payment_targets,
                                    additions,
                                    self.min_payment,
                                    self.mojos_per_cost,
                                    enable_launcher_min_payment,
                                    self.constants,
                                )

                            # Take the cost of the payment fee out of the pool wallet
                            else:
                                # Calculate minimum cost automatically
                                if self.payment_fee_absolute == -1:
                                    transaction: TransactionRecord = await create_transaction(
                                        self.node_rpc_client,
                                        wallet,
                                        self.store,
                                        additions,
                                        # Estimated fee
                                        # Two extra additions to account for extra coins
                                        # used to remove the fee.
                                        uint64(25000000 * (len(additions) + 2)),
                                        payment_targets,
                                    )
                                    fee_absolute = (await get_cost(
                                        transaction.spend_bundle, self.blockchain_state['peak'].height, self.constants
                                    )) * self.mojos_per_cost
                                else:
                                    fee_absolute = self.payment_fee_absolute

                                self.log.info('Using absolute payment fee of %d', fee_absolute)
                                blockchain_fee = uint64(fee_absolute)
                        else:
                            blockchain_fee = uint64(0)

                        if not additions:
                            self.log.info('No payments above minimum, skipping.')
                            await asyncio.sleep(60)
                            continue

                        transaction: TransactionRecord = await create_transaction(
                            self.node_rpc_client,
                            wallet,
                            self.store,
                            additions,
                            blockchain_fee,
                            payment_targets,
                        )

                        self.log.info('Submitting a payment')

                        await wallet['rpc_client'].push_transaction(
                            wallet['id'], transaction
                        )

                        self.log.info(f"Transaction: {transaction}")
                        await self.store.add_transaction(transaction, payment_targets)

                    peak_height = await wallet['rpc_client'].get_height_info()
                    while (
                        not transaction.confirmed or not (
                            peak_height - transaction.confirmed_at_height
                        ) > self.confirmation_security_threshold
                    ):
                        transaction = await wallet['rpc_client'].get_transaction(wallet['id'], transaction.name)
                        peak_height = await wallet['rpc_client'].get_height_info()
                        self.log.info(
                            f"Waiting for transaction to obtain {self.confirmation_security_threshold} confirmations"
                        )
                        if not transaction.confirmed:
                            is_in_mempool = transaction.is_in_mempool()
                            self.log.info(f"Not confirmed. In mempool? {is_in_mempool}")
                            # FIXME: remove me after 1.3 wallet bug has been fixed
                            try:
                                if not is_in_mempool:
                                    push_tx_response: Dict = await self.node_rpc_client.push_tx(
                                        transaction.spend_bundle
                                    )
                                    if push_tx_response["status"] != "SUCCESS":
                                        self.log.error(
                                            f"Error submitting transaction: {push_tx_response}"
                                        )
                            except Exception:
                                self.log.error(
                                    'Error pushing payment directly to node', exc_info=True,
                                )
                        else:
                            self.log.info(f"Confirmations: {peak_height - transaction.confirmed_at_height}")
                        await asyncio.sleep(10)

                    await self.store.confirm_transaction(transaction, payment_targets)
                    self.log.info(f'Successfully confirmed transaction {transaction.name}')

                    asyncio.create_task(self.notifications.payment(payment_targets))

            except asyncio.CancelledError:
                self.log.info("Cancelled submit_payment_loop, closing")
                return
            except Exception as e:
                self.log.error(f"Unexpected error in submit_payment_loop: {e}", exc_info=True)
                await asyncio.sleep(60)

    @task_exception
    async def confirm_partials_loop(self):
        """
        Pulls things from the queue of partials one at a time, and adjusts balances.
        """

        processing = {}
        count = itertools.count()
        pending_partials = await self.store.get_pending_partials()
        if pending_partials:
            self.log.info('Adding %d pending partials to queue', len(pending_partials))
        for pending_partial in pending_partials:
            await self.pending_point_partials.put(pending_partial)

        while True:
            try:
                # The points are based on the difficulty at the time of partial submission,
                # not at the time of # confirmation
                partial, req_metadata, time_received, points_received = (
                    await self.pending_point_partials.get()
                )

                pid = next(count)
                processing[pid] = (partial, req_metadata, time_received, points_received)

                # Wait a few minutes to check if partial is still valid in the blockchain (no reorgs)
                await asyncio.sleep((max(0, time_received + self.partial_confirmation_delay - time.time() - 5)))

                async def process_partial(pid, partial, req_metadata, time_received, points_received):
                    try:
                        await self.check_and_confirm_partial(
                            partial, req_metadata, time_received, points_received
                        )
                    except Exception:
                        plogger.error('Failed to check and confirm partial', exc_info=True)
                    del processing[pid]

                # Starts a task to check the remaining things for this partial and optionally update points
                asyncio.create_task(process_partial(
                    int(pid), partial, req_metadata, time_received, points_received
                ))
            except asyncio.CancelledError:
                self.log.info("Cancelled confirm partials loop, closing")
                async with self.store.lock:
                    # Add remaining items in the Queue, if any.
                    while True:
                        try:
                            args = self.pending_point_partials.get_nowait()
                            await self.store.add_pending_partial(*args)
                        except asyncio.QueueEmpty:
                            break

                    for args in processing.values():
                        await self.store.add_pending_partial(*args)
                return

            except Exception as e:
                self.log.error(f"Unexpected error: {e}", exc_info=True)

    async def get_signage_point_or_eos(self, partial):
        if partial.payload.end_of_sub_slot:
            response = self.recent_eos.get(partial.payload.sp_hash)
            if not response:
                response = await self.node_rpc_client.get_recent_signage_point_or_eos(
                    None, partial.payload.sp_hash,
                )
                self.recent_eos.put(partial.payload.sp_hash, response)
        else:
            response = self.recent_signage_point.get(partial.payload.sp_hash)
            if not response:
                response = await self.node_rpc_client.get_recent_signage_point_or_eos(
                    partial.payload.sp_hash, None
                )
                self.recent_signage_point.put(partial.payload.sp_hash, response)
        return response

    async def check_and_confirm_partial(
        self,
        partial: PostPartialRequest,
        req_metadata: Optional[RequestMetadata],
        time_received: uint64,
        points_received: uint64,
    ) -> None:
        try:
            response = await self.get_signage_point_or_eos(partial)
            if response is None or response["reverted"]:
                if partial.payload.end_of_sub_slot:
                    plogger.info(f"Partial EOS reverted: {partial.payload.sp_hash}")
                    await self.partials.add_partial(
                        partial.payload, req_metadata, time_received, points_received, 'EOS_REVERTED'
                    )
                else:
                    plogger.info(f"Partial SP reverted: {partial.payload.sp_hash}")
                    await self.partials.add_partial(
                        partial.payload,
                        req_metadata,
                        time_received,
                        points_received,
                        'SP_REVERTED',
                    )
                return

            # Now we know that the partial came on time, but also that the signage point / EOS is still in the
            # blockchain. We need to check for double submissions.
            pos_hash = partial.payload.proof_of_space.get_hash()
            if self.recent_points_added.get(pos_hash):
                plogger.info(
                    'Double signage point submitted for pos_hash %r, launcher id %r',
                    pos_hash,
                    partial.payload.launcher_id,
                )
                await self.partials.add_partial(
                    partial.payload,
                    req_metadata,
                    time_received,
                    points_received,
                    'DOUBLE_SIGNAGE_POINT',
                )
                return
            self.recent_points_added.put(pos_hash, uint64(1))

            # Now we need to check to see that the singleton in the blockchain is still assigned to this pool
            singleton_state_tuple: Optional[
                Tuple[CoinSpend, PoolState, bool]
            ] = await self.get_and_validate_singleton_state(partial.payload.launcher_id)

            if singleton_state_tuple is None:
                self.log.info(f"Invalid singleton {partial.payload.launcher_id}")
                await self.partials.add_partial(
                    partial.payload,
                    req_metadata,
                    time_received,
                    points_received,
                    'INVALID_SINGLETON',
                )
                return

            _, _, is_member = singleton_state_tuple
            if not is_member:
                self.log.info("Singleton is not assigned to this pool")
                await self.partials.add_partial(
                    partial.payload,
                    req_metadata,
                    time_received,
                    points_received,
                    'SINGLETON_NOT_POOL',
                )
                farmer_record: Optional[FarmerRecord] = await self.store.get_farmer_record(partial.payload.launcher_id)
                if farmer_record and farmer_record.is_pool_member:
                    self.log.info("Updating is_pool_member to false for %r", farmer_record.launcher_id.hex())
                    farmer_dict = farmer_record.to_json_dict()
                    farmer_dict['is_pool_member'] = False
                    await self.store.add_farmer_record(
                        FarmerRecord.from_json_dict(farmer_dict), None,
                    )
                    # Reset PPLNS fields if left the pool
                    await self.store.update_farmer(
                        farmer_record.launcher_id.hex(),
                        ['estimated_size', 'points_pplns', 'share_pplns'],
                        [0, 0, 0],
                    )
                await self.partials.remove_launcher(farmer_record.launcher_id)
                return

            async with self.store.lock:
                farmer_record: Optional[FarmerRecord] = await self.store.get_farmer_record(
                    partial.payload.launcher_id
                )

                if farmer_record is None:
                    self.log.info('Unknown launcher %r', partial.payload.launcher_id.hex())
                    return

                assert (
                    partial.payload.proof_of_space.pool_contract_puzzle_hash == farmer_record.p2_singleton_puzzle_hash
                )

                if farmer_record.is_pool_member:
                    await self.partials.add_partial(
                        partial.payload, req_metadata, time_received, points_received,
                    )
                    plogger.info(
                        f"Farmer {farmer_record.launcher_id} updated points to: "
                        f"{farmer_record.points + points_received}"
                    )
        except Exception:
            self.log.error('Exception in confirming partial', exc_info=True)

    async def add_farmer(self, request: PostFarmerRequest, metadata: RequestMetadata) -> Dict:
        async with self.store.lock:
            farmer_record: Optional[FarmerRecord] = await self.store.get_farmer_record(request.payload.launcher_id)
            if farmer_record is not None:
                return error_dict(
                    PoolErrorCode.FARMER_ALREADY_KNOWN,
                    f"Farmer with launcher_id {request.payload.launcher_id} already known.",
                )

            singleton_state_tuple: Optional[
                Tuple[CoinSpend, PoolState, bool]
            ] = await self.get_and_validate_singleton_state(request.payload.launcher_id)

            if singleton_state_tuple is None:
                return error_dict(PoolErrorCode.INVALID_SINGLETON, f"Invalid singleton {request.payload.launcher_id}")

            last_spend, last_state, is_member = singleton_state_tuple
            if is_member is None:
                return error_dict(PoolErrorCode.INVALID_SINGLETON, "Singleton is not assigned to this pool")

            if (
                request.payload.suggested_difficulty is None or
                request.payload.suggested_difficulty < self.min_difficulty
            ):
                difficulty: uint64 = self.default_difficulty
            else:
                difficulty = request.payload.suggested_difficulty

            puzzle_hash: Optional[str] = await self.validate_payout_instructions(request.payload.payout_instructions)
            if puzzle_hash is None:
                return error_dict(
                    PoolErrorCode.INVALID_PAYOUT_INSTRUCTIONS,
                    'Payout instructions must be an xch address or puzzle hash for this pool.',
                )

            if not AugSchemeMPL.verify(last_state.owner_pubkey, request.payload.get_hash(), request.signature):
                return error_dict(PoolErrorCode.INVALID_SIGNATURE, "Invalid signature")

            launcher_coin: Optional[CoinRecord] = await self.node_rpc_client.get_coin_record_by_name(
                request.payload.launcher_id
            )
            assert launcher_coin is not None and launcher_coin.spent

            launcher_solution: Optional[CoinSpend] = await get_coin_spend(self.node_rpc_client, launcher_coin)
            delay_time, delay_puzzle_hash = get_delayed_puz_info_from_launcher_spend(launcher_solution)

            if delay_time < 3600:
                return error_dict(PoolErrorCode.DELAY_TIME_TOO_SHORT, "Delay time too short, must be at least 1 hour")

            p2_singleton_puzzle_hash = launcher_id_to_p2_puzzle_hash(
                request.payload.launcher_id, delay_time, delay_puzzle_hash
            )

            farmer_record = FarmerRecord(
                request.payload.launcher_id,
                p2_singleton_puzzle_hash,
                delay_time,
                delay_puzzle_hash,
                request.payload.authentication_public_key,
                last_spend,
                last_state,
                uint64(0),
                difficulty,
                puzzle_hash,
                True,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            self.scan_p2_singleton_puzzle_hashes.add(p2_singleton_puzzle_hash)
            await self.store.add_farmer_record(farmer_record, metadata)
            await self.launchers.add_last_reward(farmer_record)

            # Add new farmer singleton to the list of known singleton puzzles
            singleton_state_tuple: Optional[
                Tuple[CoinSpend, PoolState, bool]
            ] = await self.get_and_validate_singleton_state(request.payload.launcher_id)

            return PostFarmerResponse(self.welcome_message).to_json_dict()

    async def validate_payout_instructions(self, payout_instructions: str) -> Optional[str]:
        """
        Returns the puzzle hash as a hex string from the payout instructions
        (puzzle hash hex or bech32m address) if it's encoded correctly, otherwise returns None.
        """
        try:
            if len(decode_puzzle_hash(payout_instructions)) == 32:
                return decode_puzzle_hash(payout_instructions).hex()
        except ValueError:
            # Not a Chia address
            pass
        try:
            if len(hexstr_to_bytes(payout_instructions)) == 32:
                return payout_instructions
        except ValueError:
            # Not a puzzle hash
            pass
        return None

    async def update_farmer(self, request: PutFarmerRequest, metadata: RequestMetadata) -> Dict:
        launcher_id = request.payload.launcher_id
        # First check if this launcher_id is currently blocked for farmer updates, if so there is no reason to validate
        # all the stuff below
        if launcher_id in self.farmer_update_blocked:
            return error_dict(PoolErrorCode.REQUEST_FAILED, "Cannot update farmer yet.")
        farmer_record: Optional[FarmerRecord] = await self.store.get_farmer_record(launcher_id)
        if farmer_record is None:
            return error_dict(PoolErrorCode.FARMER_NOT_KNOWN, f"Farmer with launcher_id {launcher_id} not known.")

        singleton_state_tuple: Optional[
            Tuple[CoinSpend, PoolState, bool]
        ] = await self.get_and_validate_singleton_state(launcher_id)

        if singleton_state_tuple is None:
            return error_dict(PoolErrorCode.INVALID_SINGLETON, f"Invalid singleton {request.payload.launcher_id}")

        last_spend, last_state, is_member = singleton_state_tuple
        if is_member is None:
            return error_dict(PoolErrorCode.INVALID_SINGLETON, "Singleton is not assigned to this pool")

        if not AugSchemeMPL.verify(last_state.owner_pubkey, request.payload.get_hash(), request.signature):
            return error_dict(PoolErrorCode.INVALID_SIGNATURE, "Invalid signature")

        updated_record: FarmerRecord = dataclasses.replace(farmer_record)
        response_dict: Dict[str, bool] = {}
        is_new_pubkey = is_new_payout = False
        if request.payload.authentication_public_key is not None:
            if is_new_pubkey := farmer_record.authentication_public_key != request.payload.authentication_public_key:
                updated_record = dataclasses.replace(
                    updated_record,
                    authentication_public_key=request.payload.authentication_public_key,
                )
            response_dict['authentication_public_key'] = is_new_pubkey

        if request.payload.payout_instructions is not None:
            new_ph: Optional[str] = await self.validate_payout_instructions(
                request.payload.payout_instructions
            )
            if is_new_payout := (
                new_ph and
                farmer_record.payout_instructions != new_ph
            ):
                updated_record = dataclasses.replace(updated_record, payout_instructions=new_ph)
            response_dict['payout_instructions'] = is_new_payout

        if updated_record != farmer_record:
            self.log.debug(
                'Updated farmer record (pubkey: %r, payout: %r) for %s',
                is_new_pubkey,
                is_new_payout,
                updated_record.launcher_id.hex(),
            )
            await self.store.add_farmer_record(updated_record, metadata)

        # We dont want to accept farmer suggestion
        # As fo 1.3.1 is not even supported by the client
        # if request.payload.suggested_difficulty is not None:
        #     is_new_value = (
        #         farmer_record.difficulty != request.payload.suggested_difficulty
        #         and request.payload.suggested_difficulty is not None
        #         and request.payload.suggested_difficulty >= self.min_difficulty
        #     )
        #     response_dict["suggested_difficulty"] = is_new_value
        #     if is_new_value:
        #         self.farmer_update_blocked.add(launcher_id)

        #         async def update_difficulty_later(launcher_id, difficulty):
        #             await asyncio.sleep(self.farmer_update_cooldown_seconds)
        #             await self.store.update_difficulty(
        #                 launcher_id, difficulty,
        #             )
        #             self.farmer_update_blocked.remove(launcher_id)

        #         asyncio.create_task(update_difficulty_later(
        #             launcher_id, request.payload.suggested_difficulty,
        #         ))

        # TODO Fix chia-blockchain's Streamable implementation to support Optional in `from_json_dict`, then use
        # PutFarmerResponse here and in the trace up.
        return response_dict

    async def get_and_validate_singleton_state(
        self, launcher_id: bytes32, raise_exc=False,
    ) -> Optional[Tuple[CoinSpend, PoolState, bool]]:
        """
        :return: the state of the singleton, if it currently exists in the blockchain, and if it is assigned to
        our pool, with the correct parameters. Otherwise, None. Note that this state must be buried (recent state
        changes are not returned)
        """
        singleton_task: Optional[Task] = self.follow_singleton_tasks.get(launcher_id, None)
        remove_after = False
        farmer_rec = None
        if singleton_task is None or singleton_task.done():
            farmer_rec: Optional[FarmerRecord] = await self.store.get_farmer_record(launcher_id)
            singleton_task = asyncio.create_task(
                get_singleton_state(
                    self.node_rpc_client,
                    launcher_id,
                    farmer_rec,
                    self.blockchain_state["peak"].height,
                    self.confirmation_security_threshold,
                    self.constants.GENESIS_CHALLENGE,
                    raise_exc=raise_exc,
                )
            )
            self.follow_singleton_tasks[launcher_id] = singleton_task
            remove_after = True

        optional_result: Optional[Tuple[CoinSpend, PoolState, PoolState]] = await singleton_task
        if remove_after and launcher_id in self.follow_singleton_tasks:
            await self.follow_singleton_tasks.pop(launcher_id)

        if optional_result is None:
            return None

        buried_singleton_tip, buried_singleton_tip_state, singleton_tip_state = optional_result

        # Validate state of the singleton
        is_pool_member = True
        if singleton_tip_state.target_puzzle_hash not in self.default_target_puzzle_hashes:
            self.log.info(
                f"Wrong target puzzle hash: {singleton_tip_state.target_puzzle_hash} for launcher_id {launcher_id}"
            )
            is_pool_member = False
        elif singleton_tip_state.relative_lock_height != self.relative_lock_height:
            self.log.info(
                f"Wrong relative lock height: {singleton_tip_state.relative_lock_height} for launcher_id {launcher_id}"
            )
            is_pool_member = False
        elif singleton_tip_state.version != POOL_PROTOCOL_VERSION:
            self.log.info(f"Wrong version {singleton_tip_state.version} for launcher_id {launcher_id}")
            is_pool_member = False
        elif singleton_tip_state.state == PoolSingletonState.SELF_POOLING.value:
            self.log.info(f"Invalid singleton state {singleton_tip_state.state} for launcher_id {launcher_id}")
            is_pool_member = False
        elif singleton_tip_state.state == PoolSingletonState.LEAVING_POOL.value:
            coin_record: Optional[CoinRecord] = await self.node_rpc_client.get_coin_record_by_name(
                buried_singleton_tip.coin.name()
            )
            assert coin_record is not None
            if self.blockchain_state["peak"].height - coin_record.confirmed_block_index > self.relative_lock_height:
                self.log.info(f"launcher_id {launcher_id} got enough confirmations to leave the pool")
                is_pool_member = False

        self.log.debug(f"Is {launcher_id} pool member: {is_pool_member}")

        if not is_pool_member:
            await self.partials.remove_launcher(launcher_id)

        if farmer_rec is not None and is_pool_member and not farmer_rec.is_pool_member:
            self.scan_p2_singleton_puzzle_hashes.add(farmer_rec.p2_singleton_puzzle_hash)

        if farmer_rec is not None and (
            farmer_rec.singleton_tip != buried_singleton_tip or
            farmer_rec.singleton_tip_state != buried_singleton_tip_state or
            await self.store.singleton_exists(launcher_id) is None
        ):
            # This means the singleton has been changed in the blockchain (either by us or someone else). We
            # still keep track of this singleton if the farmer has changed to a different pool, in case they
            # switch back.
            self.log.info(f"Updating singleton state for {launcher_id}")
            singleton_coin = get_most_recent_singleton_coin_from_coin_spend(buried_singleton_tip)
            if is_pool_member and not farmer_rec.is_pool_member and not farmer_rec.last_block_timestamp:
                await self.launchers.add_last_reward(farmer_rec)

            await self.store.update_singleton(
                farmer_rec, singleton_coin, buried_singleton_tip, buried_singleton_tip_state, is_pool_member
            )

        return buried_singleton_tip, buried_singleton_tip_state, is_pool_member

    async def process_partial(
        self,
        partial: PostPartialRequest,
        farmer_record: FarmerRecord,
        req_metadata: Optional[RequestMetadata],
        time_received_partial: uint64,
    ) -> Dict:
        # Validate signatures
        message: bytes32 = partial.payload.get_hash()
        pk1: G1Element = partial.payload.proof_of_space.plot_public_key
        pk2: G1Element = farmer_record.authentication_public_key
        valid_sig = AugSchemeMPL.aggregate_verify([pk1, pk2], [message, message], partial.aggregate_signature)
        peak_height = self.blockchain_state['peak'].height

        if farmer_record.launcher_id.hex() in self.launchers_banned:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'LAUNCHER_BANNED',
            )
            return error_dict(
                PoolErrorCode.NOT_FOUND,
                'Farmer has been banned from the pool: '
                f'{self.launchers_banned[farmer_record.launcher_id.hex()]}.',
            )

        if not valid_sig:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_AGG_SIGNATURE',
            )
            return error_dict(
                PoolErrorCode.INVALID_SIGNATURE,
                f"The aggregate signature is invalid {partial.aggregate_signature}",
            )

        if partial.payload.proof_of_space.pool_contract_puzzle_hash != farmer_record.p2_singleton_puzzle_hash:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_POOL_CONTRACT',
            )
            return error_dict(
                PoolErrorCode.INVALID_P2_SINGLETON_PUZZLE_HASH,
                f"Invalid pool contract puzzle hash {partial.payload.proof_of_space.pool_contract_puzzle_hash}",
            )

        # Refuse Chia version < 2.5.1
        chia_version = None
        if req_metadata:
            chia_version = req_metadata.get_chia_version()
        if self.testnet:
            pass
        elif not chia_version or (chia_version.release < Version('2.5.1').release):
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_VERSION',
            )
            return error_dict(
                PoolErrorCode.REQUEST_FAILED,
                f"Invalid version {chia_version}, make sure to use client version 2.5.1 or higher.",
            )
        elif chia_version:
            plogger.debug('Client version: %r', chia_version)

        response = await self.get_signage_point_or_eos(partial)
        if response is None:
            # Try again after 30 seconds in case we just didn't yet receive the signage point
            await asyncio.sleep(30)
            response = await self.get_signage_point_or_eos(partial)

        if response is None or response["reverted"]:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_SIGNAGE_OR_EOS',
            )
            return error_dict(
                PoolErrorCode.NOT_FOUND, f"Did not find signage point or EOS {partial.payload.sp_hash}, {response}"
            )
        node_time_received_sp = response["time_received"]

        signage_point: Optional[SignagePoint] = response.get("signage_point", None)
        end_of_sub_slot: Optional[EndOfSubSlotBundle] = response.get("eos", None)

        if time_received_partial - node_time_received_sp > self.partial_time_limit:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_TOO_LATE',
            )
            return error_dict(
                PoolErrorCode.TOO_LATE,
                f"Received partial in {time_received_partial - node_time_received_sp}. "
                f"Make sure your proof of space lookups are fast, and network connectivity is good."
                f"Response must happen in less than {self.partial_time_limit} seconds. NAS or network"
                f" farming can be an issue",
            )

        # Validate the proof
        if signage_point is not None:
            challenge_hash: bytes32 = signage_point.cc_vdf.challenge
        else:
            challenge_hash = end_of_sub_slot.challenge_chain.get_hash()

        # Note the use of peak_height + 1. We Are evaluating the suitability for the next block
        quality_string: Optional[bytes32] = verify_and_get_quality_string(
            partial.payload.proof_of_space, self.constants, challenge_hash, partial.payload.sp_hash,
            height=uint32(peak_height + 1)
        )
        if quality_string is None:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'INVALID_PROOF_OF_SPACE',
            )
            return error_dict(PoolErrorCode.INVALID_PROOF, f"Invalid proof of space {partial.payload.sp_hash}")

        current_difficulty = farmer_record.difficulty
        required_iters: uint64 = calculate_iterations_quality(
            self.constants.DIFFICULTY_CONSTANT_FACTOR,
            quality_string,
            partial.payload.proof_of_space.size,
            current_difficulty,
            partial.payload.sp_hash,
        )

        if required_iters >= self.iters_limit:
            await self.partials.add_partial(
                partial.payload,
                req_metadata,
                time_received_partial,
                farmer_record.difficulty,
                'PROOF_NOT_GOOD_ENOUGH',
            )
            return error_dict(
                PoolErrorCode.PROOF_NOT_GOOD_ENOUGH,
                f"Proof of space has required iters {required_iters}, too high for difficulty " f"{current_difficulty}",
            )

        await self.pending_point_partials.put(
            (partial, req_metadata, time_received_partial, current_difficulty)
        )

        try:
            launcher_lock = self.launcher_lock[partial.payload.launcher_id]
            await asyncio.wait_for(launcher_lock.acquire(), timeout=5)

            # Obtains the new record in case we just updated difficulty
            farmer_record: Optional[FarmerRecord] = await self.store.get_farmer_record(partial.payload.launcher_id)
            if farmer_record is not None:
                current_difficulty = farmer_record.difficulty
                # Decide whether to update the difficulty
                recent_partials = await self.partials.get_recent_partials(
                    partial.payload.launcher_id, self.number_of_partials_target
                )
                # Only update the difficulty if we meet certain conditions
                new_difficulty: uint64 = get_new_difficulty(
                    recent_partials,
                    int(self.number_of_partials_target),
                    int(self.time_target),
                    current_difficulty,
                    farmer_record.custom_difficulty,
                    time_received_partial,
                    self.min_difficulty,
                )

                if current_difficulty != new_difficulty:
                    await self.store.update_difficulty(partial.payload.launcher_id, new_difficulty)
                    current_difficulty = new_difficulty
        except asyncio.TimeoutError:
            self.log.warning(
                'Failed to acquire lock for %s to check difficulty',
                partial.payload.launcher_id.hex(),
            )
        finally:
            launcher_lock.release()

        return PostPartialResponse(current_difficulty).to_json_dict()
