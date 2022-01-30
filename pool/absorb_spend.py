import logging
from typing import Dict, List

from chia.consensus.constants import ConsensusConstants
from chia.types.coin_spend import CoinSpend

logger = logging.getLogger('absorb_spend')


async def spend_with_fee(
    node_rpc_client,
    wallets: List[Dict],
    spends: List[CoinSpend],
    constants: ConsensusConstants,
):
    raise NotImplementedError
