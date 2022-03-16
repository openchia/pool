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
        additions, fee=25000000 * len(additions),  # Estimated fee
    )
    total_cost = (await get_cost(
        transaction.spend_bundle, constants
    )) * mojos_per_cost
    cost_per_target = math.ceil(D(total_cost) / D(len(payment_targets)))

    # Recalculate fee after ceiling value per target
    blockchain_fee = uint64(cost_per_target * len(payment_targets))

    for targets in payment_targets.values():
        cost_per_payout = math.ceil(cost_per_target / len(targets))
        total = 0
        for i in targets:
            i['amount'] -= cost_per_payout
            total += i['amount']
            i['tx_fee'] = cost_per_payout
        if total <= 0:
            raise RuntimeError('Launcher id does not have enough for a fee payment')
    # Redo additions with proper amount this time
    additions = payment_targets_to_additions(
        payment_targets, min_payment,
        launcher_min_payment=enable_launcher_min_payment,
    )
    return additions, blockchain_fee
