import logging
from dataclasses import dataclass
from typing import Dict, Mapping

from chia.protocols.pool_protocol import PoolErrorCode, ErrorResponse
from chia.util.ints import uint16
from chia.util.json_util import obj_to_response

logger = logging.getLogger('util')


def error_response(code: PoolErrorCode, message: str):
    error: ErrorResponse = ErrorResponse(uint16(code.value), message)
    return obj_to_response(error)


def error_dict(code: PoolErrorCode, message: str):
    error: ErrorResponse = ErrorResponse(uint16(code.value), message)
    return error.to_json_dict()


@dataclass
class RequestMetadata:
    """
    HTTP-related metadata passed with HTTP requests
    """
    url: str  # original request url, as used by the client
    scheme: str  # for example https
    headers: Mapping[str, str]  # header names are all lower case
    cookies: Dict[str, str]
    query: Dict[str, str]  # query params passed in the url. These are not used by chia clients at the moment, but
    # allow for a lot of adjustments and thanks to including them now they can be used without introducing breaking changes
    remote: str  # address of the client making the request

    def __post_init__(self):
        self.headers = {k.lower(): v for k, v in self.headers.items()}


def payment_targets_to_additions(payment_targets, min_payment):
    additions = []
    for ph, amounts in list(payment_targets.items()):
        amount = sum([i['amount'] for i in amounts])
        if amount >= min_payment:
            additions.append({'puzzle_hash': ph, 'amount': amount})
        else:
            payment_targets.pop(ph)
    return additions


def check_transaction(transaction, wallet_ph):

    # We expect all non spent reward coins to be used in the transaction.
    # The goal is to only use coins assigned to a payout.
    # All other coins should be leftover (change) of previous payouts.
    # Coins in the wallet first address puzzle hash are reward coins.
    puzzle_hash_coins = set()
    non_puzzle_hash_coins = set()
    for coin in transaction.spend_bundle.removals():
        if coin.puzzle_hash == wallet_ph:
            puzzle_hash_coins.add(coin)
        else:
            non_puzzle_hash_coins.add(coin)

    return puzzle_hash_coins, non_puzzle_hash_coins


async def create_transaction(
    node_rpc_client,
    wallet_rpc_client,
    wallet_ph,
    store,
    additions,
    fee,
    payment_targets,
):

    # Lets get all coins rewards that are associated with the payouts in this round
    payout_ids = set()
    for targets in payment_targets.values():
        for t in targets:
            payout_ids.add(t['payout_id'])
    coin_rewards_names = await store.get_coin_rewards_from_payout_ids(
        payout_ids
    )

    coin_records = await node_rpc_client.get_coin_records_by_names(
        coin_rewards_names,
        include_spent_coins=True,
    )
    # Make sure to filter the not spent coins.
    # Coin rewards can be spent if they were part of a previous payment (min payment).
    unspent_coins = {cr.coin for cr in filter(lambda x: not x.spent, coin_records)}

    # If no reward coins are spent we can use them as sole source coins for the transaction
    if len(coin_records) == len(unspent_coins):
        transaction = await wallet_rpc_client.create_signed_transaction(
            additions, coins=list(unspent_coins), fee=fee
        )
        return transaction

    # If a coin was spent we give a shot for the Wallet automatically select the required coins
    transaction = await wallet_rpc_client.create_signed_transaction(
        additions, fee=fee
    )

    ph_coins, non_ph_coins = check_transaction(transaction, wallet_ph)
    # If there are more coins in wallet puzzle hash than from unspent coin for the payouts
    # we try once again using only the unspent reward coins and the coins outside wallet puzzle hash.
    if ph_coins - unspent_coins:
        logger.info('Redoing transaction to only include reward coins')
        transaction = await wallet_rpc_client.create_signed_transaction(
            additions, coins=list(unspent_coins) + list(non_ph_coins), fee=fee
        )
    return transaction
