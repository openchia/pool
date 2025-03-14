import logging

from typing import Any, Dict, List, Optional
from chia_rs import AugSchemeMPL, G2Element, PrivateKey

from chia.consensus.block_rewards import calculate_pool_reward
from chia.consensus.constants import ConsensusConstants
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.rpc.wallet_request_types import CreateSignedTransactionsResponse
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.blockchain_format.serialized_program import SerializedProgram
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend, compute_additions
from chia.types.condition_opcodes import ConditionOpcode
from chia.types.spend_bundle import SpendBundle
from chia.util.condition_tools import conditions_dict_for_solution
from chia.util.ints import uint32, uint64
from chia.util.hash import std_hash
from chia.wallet.conditions import AssertCoinAnnouncement
from chia.wallet.derive_keys import master_sk_to_wallet_sk
from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import (
    DEFAULT_HIDDEN_PUZZLE_HASH,
    calculate_synthetic_secret_key,
    puzzle_for_pk,
)
from chia.wallet.util.tx_config import DEFAULT_TX_CONFIG, CoinSelectionConfig, TXConfig
from chia.wallet.wallet import Wallet
from chia.wallet.wallet_info import WalletInfo

from .fee import get_cost

logger = logging.getLogger('absorb_spend')


# FIXME: use https://github.com/Chia-Network/chia-blockchain/blob/c4f2595e54d75b844a85df2bf405335224cfc4af/chia/consensus/block_rewards.py#L19-L30
COIN_SELECTION_CONFIG = CoinSelectionConfig(
    uint64(0),
    uint64(DEFAULT_CONSTANTS.MAX_COIN_AMOUNT),
    [uint64(0), uint64(875000000000)],
    []
)

ABSORB_TX_CONFIG = TXConfig(
    COIN_SELECTION_CONFIG.min_coin_amount,
    COIN_SELECTION_CONFIG.max_coin_amount,
    COIN_SELECTION_CONFIG.excluded_coin_amounts,
    COIN_SELECTION_CONFIG.excluded_coin_ids,
    False,
)


class NoCoinForFee(Exception):
    pass


async def spend_with_fee(
    node_rpc_client,
    peak_height: uint32,
    wallets: List[Dict],
    spends: List[CoinSpend],
    constants: ConsensusConstants,
    absolute_fee: Optional[int],
    mojos_per_cost: int,
    used_fee_coins: List,
):

    rewarded_coin: Coin = compute_additions(spends[0])[-1]
    p2_coin = spends[1].coin

    for wallet in wallets:
        if wallet['puzzle_hash'] == rewarded_coin.puzzle_hash:
            break
    else:
        raise RuntimeError(f"No wallet with puzzle hash {rewarded_coin.puzzle_hash.hex()} found")

    spend_reward = False
    if not spend_reward:
        transaction: CreateSignedTransactionsResponse = await wallet['rpc_client'].create_signed_transactions([{
            'puzzle_hash': wallet['puzzle_hash'],
            'amount': 5 * 10 ** 10,
        }], tx_config=ABSORB_TX_CONFIG)

        if hasattr(transaction.signed_tx, 'spend_bundle'):
            for coin in transaction.signed_tx.spend_bundle.removals():
                if (
                    coin.name() not in used_fee_coins and  # Do not use same coin twice between absorbs
                    coin.amount >= (absolute_fee or 200000000) and
                    coin.amount != calculate_pool_reward(uint32(1))
                ):
                    break
            else:
                raise NoCoinForFee("No coin big enough for a fee!")
        else:
            raise NoCoinForFee("No spend bundle returned from the transaction!")

        spend_coin = coin

        transaction: CreateSignedTransactionsResponse = await wallet['rpc_client'].create_signed_transactions(
            additions=[{'puzzle_hash': wallet['puzzle_hash'], 'amount': 0}],
            tx_config=DEFAULT_TX_CONFIG,
            coins=[spend_coin],
            fee=uint64(1),
            extra_conditions=(AssertCoinAnnouncement(asserted_id=p2_coin.name(), asserted_msg=b"$"),)
        )

        original_sb = SpendBundle(spends, G2Element())
        sb = SpendBundle.aggregate([original_sb, transaction.signed_tx.spend_bundle])
    else:
        spend_coin = rewarded_coin

        keys: Dict = await wallet['rpc_client'].get_private_key(wallet['fingerprint'])

        private_key = PrivateKey.from_bytes(bytes.fromhex(keys['sk']))
        private_key = master_sk_to_wallet_sk(private_key, 0)
        pubkey = private_key.get_g1()

        puzzle: Program = puzzle_for_pk(bytes(pubkey))

        sb = await create_spendbundle_with_fee(
            constants,
            private_key,
            wallet['puzzle_hash'],
            puzzle,
            list(spends),
            spend_coin,
            p2_coin,
            uint64(absolute_fee or 1),
        )

    if absolute_fee:
        return sb

    fee = uint64((await get_cost(sb, peak_height, constants)) * mojos_per_cost)

    if fee > spend_coin.amount:
        raise NoCoinForFee(f"Selected fee coin lower than the spend fee ({fee} > {spend_coin.amount})!")

    if not spend_reward:
        transaction: CreateSignedTransactionsResponse = await wallet['rpc_client'].create_signed_transactions(
            additions=[{'puzzle_hash': wallet['puzzle_hash'], 'amount': spend_coin.amount - fee}],
            tx_config=DEFAULT_TX_CONFIG,
            coins=[spend_coin],
            fee=uint64(fee),
            extra_conditions=(AssertCoinAnnouncement(asserted_id=p2_coin.name(), asserted_msg=b"$"),)
        )
        used_fee_coins.append(spend_coin.name())
        return SpendBundle.aggregate([original_sb, transaction.signed_tx.spend_bundle])
    else:
        return await create_spendbundle_with_fee(
            constants,
            private_key,
            wallet['puzzle_hash'],
            puzzle,
            list(spends),
            spend_coin,
            p2_coin,
            fee,
        )


async def create_spendbundle_with_fee(
    constants,
    private_key,
    puzzle_hash,
    puzzle,
    spends,
    spend_coin,
    p2_coin,
    fee
):
    primaries: List[Dict[str, Any]] = [{
        'puzzlehash': puzzle_hash,
        'amount': spend_coin.amount - fee,
    }]

    message_list = [spend_coin.name()]
    for i in primaries:
        message_list.append(
            Coin(spend_coin.name(),
            i['puzzlehash'],
            i['amount']).name()
        )
    message: bytes32 = std_hash(b"".join(message_list))

    wi = WalletInfo(0, '', 0, '')
    w = await Wallet.create(None, wi)

    solution: Program = w.make_solution(
        primaries=primaries,
        coin_announcements={message},
        fee=fee,
        coin_announcements_to_assert={AssertCoinAnnouncement(asserted_id=p2_coin.name(), asserted_msg=bytes(b"$")).name()},
    )

    coin_spend = CoinSpend(
        spend_coin,
        SerializedProgram.from_bytes(bytes(puzzle)),
        SerializedProgram.from_bytes(bytes(solution)),
    )

    conditions_dict = conditions_dict_for_solution(
        coin_spend.puzzle_reveal,
        coin_spend.solution,
        constants.MAX_BLOCK_COST_CLVM
    )

    synthetic_secret_key = calculate_synthetic_secret_key(
        private_key,
        DEFAULT_HIDDEN_PUZZLE_HASH
    )

    signatures = []
    for cwa in conditions_dict.get(ConditionOpcode.AGG_SIG_UNSAFE, []):
        msg = cwa.vars[1]
        signature = AugSchemeMPL.sign(synthetic_secret_key, msg)
        signatures.append(signature)

    for cwa in conditions_dict.get(ConditionOpcode.AGG_SIG_ME, []):
        msg = cwa.vars[1] + bytes(coin_spend.coin.name()) + constants.AGG_SIG_ME_ADDITIONAL_DATA
        signature = AugSchemeMPL.sign(synthetic_secret_key, msg)
        signatures.append(signature)

    spends.append(coin_spend)
    aggsig = AugSchemeMPL.aggregate(signatures)

    return SpendBundle(spends, aggsig)
