import logging
from typing import Any, Dict, List, Optional
from blspy import AugSchemeMPL, PrivateKey

from chia.consensus.block_rewards import calculate_pool_reward
from chia.consensus.constants import ConsensusConstants
from chia.types.announcement import Announcement
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program, SerializedProgram
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.types.condition_opcodes import ConditionOpcode
from chia.types.spend_bundle import SpendBundle
from chia.util.condition_tools import conditions_by_opcode, conditions_for_solution
from chia.util.ints import uint32, uint64
from chia.util.hash import std_hash
from chia.wallet.derive_keys import master_sk_to_wallet_sk
from chia.wallet.puzzles.p2_delegated_puzzle_or_hidden_puzzle import (
    DEFAULT_HIDDEN_PUZZLE_HASH,
    calculate_synthetic_secret_key,
    puzzle_for_pk,
)
from blspy import G2Element
from chia.wallet.wallet import Wallet
from chia.wallet.wallet_info import WalletInfo

from .fee import get_cost

logger = logging.getLogger('absorb_spend')


class NoCoinForFee(Exception):
    pass


async def spend_with_fee(
    node_rpc_client,
    wallets: List[Dict],
    spends: List[CoinSpend],
    constants: ConsensusConstants,
    absolute_fee: Optional[int],
    mojos_per_cost: int,
    used_fee_coins: List,
):

    rewarded_coin: Coin = spends[0].additions()[-1]
    p2_coin = spends[1].coin

    for wallet in wallets:
        if wallet['puzzle_hash'] == rewarded_coin.puzzle_hash:
            break
    else:
        raise RuntimeError(
            f'No wallet with puzzle hash {rewarded_coin.puzzle_hash.hex()} found.'
        )

    # Spending the reward is broken!!!
    spend_reward = False
    if not spend_reward:
        # Use a wallet coin to spend for the fee
        balance = await wallet['rpc_client'].get_wallet_balance(wallet['id'])
        transaction = await wallet['rpc_client'].create_signed_transaction([{
            'puzzle_hash': wallet['puzzle_hash'],
            'amount': balance['spendable_balance'],
        }])

        # Find a coin that is big enough for the fee and also not a reward
        for coin in transaction.spend_bundle.removals():
            if (
                coin.name() not in used_fee_coins and  # Do not use same coin twice between absorbs
                coin.amount >= (absolute_fee or 200000000) and
                coin.amount != calculate_pool_reward(uint32(1))
            ):
                break
        else:
            raise NoCoinForFee('No coin big enough for a fee!')

        spend_coin = coin

        transaction = await wallet['rpc_client'].create_signed_transaction(
            additions=[{'puzzle_hash': wallet['puzzle_hash'], 'amount': 0}],
            coins=[spend_coin],
            coin_announcements=[Announcement(p2_coin.name(), b"$")],
            fee=uint64(1),
        )

        original_sb = SpendBundle(spends, G2Element())
        sb = SpendBundle.aggregate([original_sb, transaction.spend_bundle])
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

    fee = uint64((await get_cost(sb, constants)) * mojos_per_cost)

    if fee > spend_coin.amount:
        raise NoCoinForFee(
            f'Selected fee coin lower than the spend fee ({fee} > {spend_coin.amount})!'
        )

    if not spend_reward:
        transaction = await wallet['rpc_client'].create_signed_transaction(
            additions=[{'puzzle_hash': wallet['puzzle_hash'], 'amount': 0}],
            coins=[spend_coin],
            coin_announcements=[Announcement(p2_coin.name(), b"$")],
            fee=uint64(fee),
        )
        used_fee_coins.append(spend_coin.name())
        return SpendBundle.aggregate([original_sb, transaction.spend_bundle])
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


async def create_spendbundle_with_fee(constants, private_key, puzzle_hash, puzzle, spends, spend_coin, p2_coin, fee):
    primaries: List[Dict[str, Any]] = [{
        'puzzlehash': puzzle_hash,
        'amount': spend_coin.amount - fee,
    }]
    message_list = [spend_coin.name()]
    for i in primaries:
        message_list.append(
            Coin(spend_coin.name(), i['puzzlehash'], i['amount']).name()
        )
    message: bytes32 = std_hash(b"".join(message_list))

    # Hack to use Wallet implementation of make_solution
    wi = WalletInfo(0, '', 0, '')
    w = await Wallet.create(None, wi)

    solution: Program = w.make_solution(
        primaries=primaries,
        coin_announcements={message},
        fee=fee,
        coin_announcements_to_assert={Announcement(p2_coin.name(), bytes(b"$")).name()},
    )
    coin_spend = CoinSpend(
        spend_coin,
        SerializedProgram.from_bytes(bytes(puzzle)),
        SerializedProgram.from_bytes(bytes(solution)),
    )
    err, con, cost = conditions_for_solution(
        coin_spend.puzzle_reveal, coin_spend.solution, constants.MAX_BLOCK_COST_CLVM
    )
    if not con:
        raise ValueError(err)
    conditions_dict = conditions_by_opcode(con)

    synthetic_secret_key = calculate_synthetic_secret_key(
        private_key, DEFAULT_HIDDEN_PUZZLE_HASH
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
