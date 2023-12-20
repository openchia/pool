from typing import Dict, List, Optional, Tuple
import logging

from chia_rs import G2Element

from chia.consensus.block_record import BlockRecord
from chia.consensus.coinbase import pool_parent_id
from chia.consensus.constants import ConsensusConstants
from chia.pools.pool_puzzles import (
    create_absorb_spend,
    solution_to_pool_state,
    get_most_recent_singleton_coin_from_coin_spend,
    pool_state_to_inner_puzzle,
    create_full_puzzle,
    get_delayed_puz_info_from_launcher_spend,
)
from chia.pools.pool_wallet import PoolSingletonState
from chia.pools.pool_wallet_info import PoolState
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_record import CoinRecord
from chia.types.coin_spend import CoinSpend
from chia.types.spend_bundle import SpendBundle
from chia.util.ints import uint32, uint64

from .absorb_spend import spend_with_fee
from .record import FarmerRecord
from .types import AbsorbFee

logger = logging.getLogger('singleton')


class LastSpendCoinNotFound(Exception):
    def __init__(self, last_not_none_state):
        self.last_not_none_state = last_not_none_state


async def get_coin_spend(node_rpc_client: FullNodeRpcClient, coin_record: CoinRecord) -> Optional[CoinSpend]:
    if not coin_record.spent:
        return None
    return await node_rpc_client.get_puzzle_and_solution(coin_record.coin.name(), coin_record.spent_block_index)


def validate_puzzle_hash(
    launcher_id: bytes32,
    delay_ph: bytes32,
    delay_time: uint64,
    pool_state: PoolState,
    outer_puzzle_hash: bytes32,
    genesis_challenge: bytes32,
) -> bool:
    inner_puzzle: Program = pool_state_to_inner_puzzle(pool_state, launcher_id, genesis_challenge, delay_time, delay_ph)
    new_full_puzzle: Program = create_full_puzzle(inner_puzzle, launcher_id)
    return new_full_puzzle.get_tree_hash() == outer_puzzle_hash


async def get_singleton_state(
    node_rpc_client: FullNodeRpcClient,
    launcher_id: bytes32,
    farmer_record: Optional[FarmerRecord],
    peak_height: uint32,
    confirmation_security_threshold: int,
    genesis_challenge: bytes32,
    raise_exc=False,
) -> Optional[Tuple[CoinSpend, PoolState, PoolState]]:
    try:
        if farmer_record is None:
            launcher_coin: Optional[CoinRecord] = await node_rpc_client.get_coin_record_by_name(launcher_id)
            if launcher_coin is None:
                logger.warning(f"Can not find genesis coin {launcher_id}")
                return None
            if not launcher_coin.spent:
                logger.warning(f"Genesis coin {launcher_id} not spent")
                return None

            last_spend: Optional[CoinSpend] = await get_coin_spend(node_rpc_client, launcher_coin)
            delay_time, delay_puzzle_hash = get_delayed_puz_info_from_launcher_spend(last_spend)
            saved_state = solution_to_pool_state(last_spend)
            assert last_spend is not None and saved_state is not None
        else:
            last_spend = farmer_record.singleton_tip
            saved_state = farmer_record.singleton_tip_state
            delay_time = farmer_record.delay_time
            delay_puzzle_hash = farmer_record.delay_puzzle_hash

        saved_spend = last_spend
        last_not_none_state: PoolState = saved_state
        assert last_spend is not None

        last_coin_record: Optional[CoinRecord] = await node_rpc_client.get_coin_record_by_name(last_spend.coin.name())
        if last_coin_record is None:
            if raise_exc:
                raise LastSpendCoinNotFound(last_not_none_state)
            logger.info('Last spend coin record for %s is None', launcher_id.hex())
            if last_not_none_state:
                logger.info('Last pool url %s', last_not_none_state.pool_url)
            return None

        while True:
            # Get next coin solution
            next_coin: Optional[Coin] = get_most_recent_singleton_coin_from_coin_spend(last_spend)
            if next_coin is None:
                # This means the singleton is invalid
                return None
            next_coin_record: Optional[CoinRecord] = await node_rpc_client.get_coin_record_by_name(next_coin.name())
            assert next_coin_record is not None

            if not next_coin_record.spent:
                if not validate_puzzle_hash(
                    launcher_id,
                    delay_puzzle_hash,
                    delay_time,
                    last_not_none_state,
                    next_coin_record.coin.puzzle_hash,
                    genesis_challenge,
                ):
                    logger.warning(f"Invalid singleton puzzle_hash for {launcher_id}")
                    return None
                break

            last_spend: Optional[CoinSpend] = await get_coin_spend(node_rpc_client, next_coin_record)
            assert last_spend is not None

            pool_state: Optional[PoolState] = solution_to_pool_state(last_spend)

            if pool_state is not None:
                last_not_none_state = pool_state
            if peak_height - confirmation_security_threshold >= next_coin_record.spent_block_index:
                # There is a state transition, and it is sufficiently buried
                saved_spend = last_spend
                saved_state = last_not_none_state

        return saved_spend, saved_state, last_not_none_state
    except LastSpendCoinNotFound:
        raise
    except Exception as e:
        logger.error(f"Error getting singleton: {e}", exc_info=True)
        return None


def get_farmed_height(reward_coin_record: CoinRecord, genesis_challenge: bytes32) -> Optional[uint32]:
    # Returns the height farmed if it's a coinbase reward, otherwise None
    for block_index in range(
        reward_coin_record.confirmed_block_index, reward_coin_record.confirmed_block_index - 128, -1
    ):
        if block_index < 0:
            break
        pool_parent = pool_parent_id(uint32(block_index), genesis_challenge)
        if pool_parent == reward_coin_record.coin.parent_coin_info:
            return uint32(block_index)
    return None


async def create_absorb_transaction(
    node_rpc_client: FullNodeRpcClient,
    wallets: List[Dict],
    farmer_record: FarmerRecord,
    peak_height: uint32,
    reward_coin_records: List[CoinRecord],
    fee: AbsorbFee,
    absolute_fee: Optional[int],
    used_fee_coins: Optional[List],
    mempool_full_pct: int,
    mojos_per_cost: int,
    constants: ConsensusConstants,
) -> Optional[SpendBundle]:
    singleton_state_tuple: Optional[Tuple[CoinSpend, PoolState, PoolState]] = await get_singleton_state(
        node_rpc_client, farmer_record.launcher_id, farmer_record, peak_height, 0, constants.GENESIS_CHALLENGE
    )
    if singleton_state_tuple is None:
        logger.info(f"Invalid singleton {farmer_record.launcher_id}.")
        return None
    last_spend, last_state, last_state_2 = singleton_state_tuple
    # Here the buried state is equivalent to the latest state, because we use 0 as the security_threshold
    assert last_state == last_state_2

    if last_state.state == PoolSingletonState.SELF_POOLING:
        logger.info(f"Don't try to absorb from former farmer {farmer_record.launcher_id}.")
        return None

    launcher_coin_record: Optional[CoinRecord] = await node_rpc_client.get_coin_record_by_name(
        farmer_record.launcher_id
    )
    assert launcher_coin_record is not None

    all_spends: List[CoinSpend] = []
    for reward_coin_record in reward_coin_records:
        found_block_index: Optional[uint32] = get_farmed_height(reward_coin_record, constants.GENESIS_CHALLENGE)
        if not found_block_index:
            # The puzzle does not allow spending coins that are not a coinbase reward
            logger.info(f"Received reward {reward_coin_record.coin} that is not a pool reward.")
            continue

        absorb_spend: List[CoinSpend] = create_absorb_spend(
            last_spend,
            last_state,
            launcher_coin_record.coin,
            found_block_index,
            constants.GENESIS_CHALLENGE,
            farmer_record.delay_time,
            farmer_record.delay_puzzle_hash,
        )
        last_spend = absorb_spend[0]
        all_spends += absorb_spend

    if len(all_spends) == 0:
        return None

    if fee == AbsorbFee.AUTO:
        with_fee = mempool_full_pct > 10
        logger.info(
            'Absorb fee is AUTO. Mempool is %d%% full. Fees: %r', mempool_full_pct, with_fee,
        )
    else:
        with_fee = fee == AbsorbFee.TRUE

    if with_fee:
        assert used_fee_coins is not None
        return await spend_with_fee(
            node_rpc_client,
            peak_height,
            wallets,
            all_spends,
            constants,
            absolute_fee,
            mojos_per_cost,
            used_fee_coins,
        )
    else:
        return SpendBundle(all_spends, G2Element())


async def find_reward_from_coinrecord(
    node_rpc_client: FullNodeRpcClient, store, coin_record: CoinRecord,
) -> Optional[Tuple[CoinRecord, FarmerRecord]]:

    farmer: FarmerRecord = await store.get_farmer_record_from_singleton(
        coin_record.coin.parent_coin_info
    )

    if not farmer:
        return None

    block_record: BlockRecord = await node_rpc_client.get_block_record_by_height(coin_record.confirmed_block_index)
    if block_record.is_transaction_block:
        additions, removals = await node_rpc_client.get_additions_and_removals(
            block_record.header_hash
        )
        for cr in additions:
            if cr.name == coin_record.name:
                break
        else:
            return

        for cr in removals:
            if cr.spent and cr.spent_block_index == coin_record.confirmed_block_index and \
                    cr.coin.puzzle_hash == farmer.p2_singleton_puzzle_hash:
                return cr, farmer


async def find_last_reward_from_launcher(
    node_rpc_client: FullNodeRpcClient,
    farmer: FarmerRecord,
    current_reward_timestamp: uint64,
) -> Optional[CoinRecord]:

    end = int(current_reward_timestamp) - 1
    last_reward = None
    while end > 0:
        start = end - 50000
        if start < 0:
            start = 0
        coin_records = await node_rpc_client.get_coin_records_by_puzzle_hash(
            farmer.p2_singleton_puzzle_hash,
            include_spent_coins=True,
            start_height=start,
            end_height=end,
        )
        # Only get coinbase coins
        coin_records = list(filter(lambda x: x.coinbase, coin_records))
        if coin_records:
            last_reward = sorted(coin_records, key=lambda x: x.confirmed_block_index)[-1]
            break
        end = start
    return last_reward
