import math

from decimal import Decimal as D
from typing import Dict, List, Tuple
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.util.ints import uint64
from chia.wallet.transaction_record import TransactionRecord

from .fee import get_cost
from .util import payment_targets_to_additions


async def subtract_fees(
    wallet_rpc_client: WalletRpcClient,
    payment_targets: Dict,
    additions: List,
    min_payment: int,
    mojos_per_cost: int,
    enable_launcher_min_payment: bool,
    constants,
) -> Tuple[List, uint64]:
    transaction: TransactionRecord = await wallet_rpc_client.create_signed_transaction(
        additions,
    )
    total_cost = (await get_cost(
        transaction.spend_bundle, constants
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
