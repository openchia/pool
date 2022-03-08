import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal as D
from typing import Dict, Mapping, Optional
from urllib.parse import urlparse

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

    def get_chia_version(self) -> Optional[str]:
        user_agent = self.headers.get('user-agent')
        if not user_agent:
            return
        if user_agent.startswith('Chia Blockchain v.'):
            return user_agent.split('Chia Blockchain v.', 1)[-1]

    def get_host(self) -> Optional[str]:
        try:
            forwarded = self.headers.get('x-forwarded-host')
            if forwarded:
                return forwarded
            parse = urlparse(self.url)
            return parse.hostname
        except ValueError:
            return None

    def get_remote(self) -> Optional[str]:
        if self.remote:
            return self.remote.split(',', 1)[0] or None

    def to_json_dict(self):
        return asdict(self)

    @classmethod
    def from_json_dict(cls, data):
        return cls(**data)


def payment_targets_to_additions(
        payment_targets: Dict, min_payment, launcher_min_payment: bool = True,
        limit: Optional[int] = None,
):
    additions = []
    for ph, payment in list(payment_targets.items()):

        if limit and len(additions) > limit:
            payment_targets.pop(ph)
            continue

        amount = 0
        min_pay = min_payment
        for i in payment:
            amount += i['amount']
            if launcher_min_payment:
                launcher_min_pay = i.get('min_payout', None) or 0
                if launcher_min_pay > min_pay:
                    min_pay = launcher_min_pay

        if amount >= min_pay:
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
    wallet,
    store,
    additions,
    fee,
    payment_targets,
):

    if wallet.get('use_reward_coin', True) is False:
        transaction = await wallet['rpc_client'].create_signed_transaction(
            additions, fee=fee
        )
        return transaction

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
        transaction = await wallet['rpc_client'].create_signed_transaction(
            additions, coins=list(unspent_coins), fee=fee
        )
        return transaction

    # If a coin was spent we give a shot for the Wallet automatically select the required coins
    transaction = await wallet['rpc_client'].create_signed_transaction(additions, fee=fee)

    ph_coins, non_ph_coins = check_transaction(transaction, wallet['puzzle_hash'])
    # If there are more coins in wallet puzzle hash than from unspent coin for the payouts
    # we try once again using only the unspent reward coins and the coins outside wallet puzzle hash.
    if ph_coins - unspent_coins:
        logger.info('Redoing transaction to only include reward coins')

        total_additions = sum(a['amount'] for a in additions)
        total_coins = sum(int(c.amount) for c in list(unspent_coins) + list(non_ph_coins))
        if total_additions <= total_coins:
            transaction = await wallet['rpc_client'].create_signed_transaction(
                additions, coins=list(unspent_coins) + list(non_ph_coins), fee=fee
            )
        else:
            # We are short of coins to make the payment
            logger.info('Getting extra non ph coins')
            balance = await wallet['rpc_client'].get_wallet_balance(wallet['id'])
            transaction = await wallet['rpc_client'].create_signed_transaction([{
                'puzzle_hash': wallet['puzzle_hash'],
                'amount': balance['spendable_balance'],
            }])

            amount_missing = total_additions - total_coins
            for coin in transaction.spend_bundle.removals():
                if coin.puzzle_hash == wallet['puzzle_hash']:
                    continue
                if coin not in non_ph_coins:
                    amount_missing -= int(coin.amount)
                    non_ph_coins.add(coin)
                    if amount_missing <= 0:
                        break
            else:
                raise RuntimeError('Not enough non puzzle hash coins for payment')
            transaction = await wallet['rpc_client'].create_signed_transaction(
                additions, coins=list(unspent_coins) + list(non_ph_coins), fee=fee
            )
    return transaction


def days_pooling(
    joined_at: Optional[datetime], left_at: Optional[datetime], is_pool_member: bool,
) -> int:
    if not is_pool_member:
        return 0
    if not joined_at:
        joined_at = datetime(2021, 8, 9)

    if not left_at:
        left_at = datetime.now(timezone.utc)

    return (left_at - joined_at).days


def stay_fee_discount(stay_fee_discount: float, stay_fee_length: int, days_passed: int) -> D:
    if days_passed <= 0:
        return D('0')

    # fee discount increases every week, not every day
    days_passed = D((days_passed // 7) * 7)

    passed_pct = min(days_passed / D(stay_fee_length), D('1'))

    return passed_pct * D(stay_fee_discount)
