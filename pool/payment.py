import logging
import math

from decimal import Decimal as D
from typing import Dict, List, Tuple
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.util.ints import uint64
from chia.wallet.transaction_record import TransactionRecord
from chia.wallet.util.tx_config import DEFAULT_TX_CONFIG

from .fee import get_cost
from .util import (
    payment_targets_to_additions,
    size_discount,
    stay_fee_discount,
)

logger = logging.getLogger('payment')


async def subtract_fees(
    wallet_rpc_client: WalletRpcClient,
    height: int,
    payment_targets: Dict,
    additions: List,
    min_payment: int,
    mojos_per_cost: int,
    enable_launcher_min_payment: bool,
    constants,
) -> Tuple[List, uint64]:
    transaction: TransactionRecord = await wallet_rpc_client.create_signed_transactions(
        additions=additions,
        tx_config=DEFAULT_TX_CONFIG,
    )
    total_cost = (await get_cost(
        transaction.spend_bundle, height, constants
    )) * mojos_per_cost
    cost_per_target = math.ceil(D(total_cost) / D(len(payment_targets)))

    for targets in payment_targets.values():
        cost_per_payout = math.ceil(cost_per_target / len(targets))
        pending_amount = 0
        for i in targets:
            subtract = i['amount'] - cost_per_payout - pending_amount
            if subtract < 0:
                i['tx_fee'] = i['amount']
                i['amount'] = 0
                pending_amount = abs(subtract)
            else:
                i['tx_fee'] = cost_per_payout + pending_amount
                pending_amount = 0
                i['amount'] = subtract
        if pending_amount > 0:
            raise RuntimeError('Launcher id does not have enough for a fee payment')

    # Redo additions with proper amount this time
    additions = payment_targets_to_additions(
        payment_targets, min_payment,
        launcher_min_payment=enable_launcher_min_payment,
    )

    # Recalculate fee after ceiling value per target
    blockchain_fee = uint64(cost_per_target * len(payment_targets))

    return additions, blockchain_fee


async def create_share(
    store,
    total_amount: int,
    total_points: int,
    farmer_points_data: List,
    pool_fee: float,
    stay_fee_discount_v: float,
    stay_fee_length: int,
    size_fee_discount: float,
    max_fee_discount: D,
):

    additions: Dict = {}
    share = {
        'pool_fee_amount': 0,
        'referral_fee_amount': 0,
        'additions': additions,
        'amount_to_distribute': 0,
    }

    if not farmer_points_data or total_points <= 0:
        return

    mojo_per_point = D(total_amount) / D(total_points)
    logger.info(f"Paying out {mojo_per_point} mojo / point")

    referrals = await store.get_referrals()

    for i in farmer_points_data:
        points = i['points']
        ph = i['payout_instructions']

        if points <= 0:
            continue

        if ph not in additions:
            additions[ph] = {'amount': 0, 'pool_fee': 0, 'launcher_ids': []}
        if i['launcher_id'] not in additions[ph]['launcher_ids']:
            additions[ph]['launcher_ids'].append(i['launcher_id'])

        farmer_stay_fee: D = stay_fee_discount(
            stay_fee_discount_v, stay_fee_length, i['days_pooling'],
        )
        size_fee: D = D('0')
        if size_fee_discount:
            size_fee = size_discount(
                i['estimated_size'], size_fee_discount,
            )

        mojos = points * mojo_per_point
        fee_discount = min(farmer_stay_fee + size_fee, max_fee_discount)
        pool_fee_pct = D(pool_fee) * (1 - fee_discount)

        # Just be extra sure pool is getting enough fee
        assert pool_fee_pct > pool_fee / 2

        addition = mojos * (1 - pool_fee_pct)
        pool_fee_mojos = mojos - addition

        additions[ph]['amount'] += int(addition)
        additions[ph]['pool_fee'] += int(pool_fee_mojos)

        if ph in referrals:
            # Divide between pool fee and referral fee
            referral_fee = pool_fee_mojos * D(0.2)  # 20% fixed for now
            share['pool_fee_amount'] += pool_fee_mojos - referral_fee

            referral_fee = math.floor(referral_fee)
            share['referral_fee_amount'] += referral_fee

            referral = referrals[ph]
            target_ph = referral['target_payout_instructions']
            if target_ph not in additions:
                additions[target_ph] = {
                    'amount': 0, 'pool_fee': 0, 'launcher_ids': [],
                }

            additions[target_ph]['amount'] += referral_fee
            if referral['target_launcher_id'] not in additions[target_ph]['launcher_ids']:
                additions[target_ph]['launcher_ids'].append(referral['target_launcher_id'])

            additions[ph]['referral'] = referral['id']
            additions[ph]['referral_amount'] = referral_fee
        else:
            share['pool_fee_amount'] += pool_fee_mojos

    share['pool_fee_amount'] = math.floor(share['pool_fee_amount'])
    share['amount_to_distribute'] = sum(map(lambda x: x['amount'], additions.values()))
    share['remainings'] = int(total_amount - share['amount_to_distribute'] - share['pool_fee_amount'])
    return share
