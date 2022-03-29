import copy
import pytest
from decimal import Decimal as D, localcontext
from unittest.mock import patch, AsyncMock

from pool.payment import subtract_fees, create_share
from pool.util import payment_targets_to_additions

DEC_XCH = 10 ** 11
FEE_XCH = 10 ** 10
REWARD_XCH = 1.75 * 10 ** 12


@pytest.mark.parametrize('payment_targets,total_cost,rv_additions,rv_payment_targets,exc', [
    (
        {
            b'\65' * 32: [
                {'id': 1, 'payout_id': 1, 'amount': DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
            ],
        },
        FEE_XCH,
        [{'puzzle_hash': b'\65' * 32, 'amount': DEC_XCH - FEE_XCH}],
        {
            b'\65' * 32: [
                FEE_XCH,
            ]
        },
        None,
    ),
    (
        {
            b'\65' * 32: [
                {'id': 1, 'payout_id': 1, 'amount': DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
                {'id': 3, 'payout_id': 1, 'amount': 2 * DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
            ],
            b'\66' * 32: [
                {'id': 2, 'payout_id': 1, 'amount': 9 * DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '2' * 32},
            ],
        },
        FEE_XCH,
        [
            {'puzzle_hash': b'\65' * 32, 'amount': 3 * DEC_XCH - FEE_XCH // 2},
            {'puzzle_hash': b'\66' * 32, 'amount': 9 * DEC_XCH - FEE_XCH // 2},
        ],
        {
            b'\65' * 32: [FEE_XCH // 2 // 2, FEE_XCH // 2 // 2],
            b'\66' * 32: [FEE_XCH // 2],
        },
        None,
    ),
    (
        {
            b'\65' * 32: [
                # Use one share that is smaller than the full fee
                {'id': 1, 'payout_id': 1, 'amount': DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
                {'id': 3, 'payout_id': 1, 'amount': 2 * DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
            ],
            b'\66' * 32: [
                {'id': 2, 'payout_id': 1, 'amount': 9 * DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '2' * 32},
            ],
        },
        55 * FEE_XCH,
        [
            {'puzzle_hash': b'\65' * 32, 'amount': 3 * DEC_XCH - 55 * FEE_XCH // 2},
            {'puzzle_hash': b'\66' * 32, 'amount': 9 * DEC_XCH - 55 * FEE_XCH // 2},
        ],
        {
            # First share fee is the same as the amount
            b'\65' * 32: [DEC_XCH, 55 * FEE_XCH // 2 - DEC_XCH],
            b'\66' * 32: [55 * FEE_XCH // 2],
        },
        None
    ),
    # Except an exception as the fee is bigger than an entire payment target
    (
        {
            b'\65' * 32: [
                # Use one share that is smaller than the full fee
                {'id': 1, 'payout_id': 1, 'amount': DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
                {'id': 3, 'payout_id': 1, 'amount': DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '1' * 32},
            ],
            b'\66' * 32: [
                {'id': 2, 'payout_id': 1, 'amount': 9 * DEC_XCH, 'fee': False, 'min_payout': 0, 'launcher_id': '2' * 32},
            ],
        },
        55 * FEE_XCH,
        [
            {'puzzle_hash': b'\65' * 32, 'amount': 3 * DEC_XCH - 55 * FEE_XCH // 2},
            {'puzzle_hash': b'\66' * 32, 'amount': 9 * DEC_XCH - 55 * FEE_XCH // 2},
        ],
        {
            # First share fee is the same as the amount
            b'\65' * 32: [DEC_XCH, 55 * FEE_XCH // 2 - DEC_XCH],
            b'\66' * 32: [55 * FEE_XCH // 2],
        },
        RuntimeError,
    ),
])
async def test__subtract_fees(wallet_rpc_client, payment_targets, total_cost, rv_additions, rv_payment_targets, exc):

    additions = payment_targets_to_additions(payment_targets, 0, True)

    orig_payment_targets = copy.deepcopy(payment_targets)

    with patch('pool.payment.get_cost') as mock:
        mock.return_value = total_cost
        try:
            additions, fee = await subtract_fees(wallet_rpc_client, payment_targets, additions, 0, 1, True, object())
        except Exception as e:
            if exc is not None and isinstance(e, exc):
                return
    assert rv_additions == additions
    assert fee == total_cost

    for ph, targets in orig_payment_targets.items():
        tx_fees = rv_payment_targets[ph]
        for i, target in enumerate(targets):
            assert tx_fees[i] == payment_targets[ph][i]['tx_fee']
            assert payment_targets[ph][i]['amount'] == target['amount'] - tx_fees[i]


@pytest.mark.parametrize('fees, farmers_points_data, referrals, total_amount, total_points, rv', [
    # Pool fee only
    (
        {'stay_d': 0.0, 'stay_l': 2, 'pool': 0.01, 'size': {}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 2, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '2', 'points': 900000, 'days_pooling': 2, 'estimated_size': 1, 'launcher_id': 'b'},
        ],
        {},
        REWARD_XCH,
        1000000,
        {
            'pool_fee_amount': int(REWARD_XCH * 0.01),
            'referral_fee_amount': 0,
            'additions': {
                '1': {'amount': int(REWARD_XCH * 0.99 * 0.1), 'pool_fee': int(REWARD_XCH * 0.01 * 0.1), 'launcher_ids': ['a']},
                '2': {'amount': int(REWARD_XCH * 0.99 * 0.9), 'pool_fee': int(REWARD_XCH * 0.01 * 0.9), 'launcher_ids': ['b']},
            },
            'amount_to_distribute': int(REWARD_XCH * 0.99),
            'remainings': 0,
        },
    ),
    # Pool fee only + shared payout instructions
    (
        {'stay_d': 0.0, 'stay_l': 2, 'pool': 0.01, 'size': {}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 2, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '1', 'points': 900000, 'days_pooling': 2, 'estimated_size': 1, 'launcher_id': 'b'},
        ],
        {},
        REWARD_XCH,
        1000000,
        {
            'pool_fee_amount': int(REWARD_XCH * 0.01),
            'referral_fee_amount': 0,
            'additions': {
                '1': {'amount': int(REWARD_XCH * 0.99), 'pool_fee': int(REWARD_XCH * 0.01), 'launcher_ids': ['a', 'b']},
            },
            'amount_to_distribute': int(REWARD_XCH * 0.99),
            'remainings': 0,
        },
    ),
    # Pool fee + stay of length
    (
        {'stay_d': 0.1, 'stay_l': 100, 'pool': 0.01, 'size': {}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 0, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '2', 'points': 900000, 'days_pooling': 50, 'estimated_size': 1, 'launcher_id': 'b'},
            {'payout_instructions': '3', 'points': 1000000, 'days_pooling': 150, 'estimated_size': 1, 'launcher_id': 'c'},
        ],
        {},
        REWARD_XCH,
        2000000,
        {
            'pool_fee_amount': int(
               D(REWARD_XCH * 0.05 * 0.01) +
               D(REWARD_XCH * 0.45 * 0.00951) +
               D(REWARD_XCH * 0.5 * 0.009) + 1
            ),
            'referral_fee_amount': 0,
            'additions': {
                '1': {'amount': int(D(REWARD_XCH) * D('0.99') * D('0.05')), 'pool_fee': int(D(REWARD_XCH) * D('0.01') * D('0.05')), 'launcher_ids': ['a']},
                '2': {'amount': int(REWARD_XCH * (1 - 0.00951) * 0.45), 'pool_fee': int(REWARD_XCH * (0.00951) * 0.45) + 1, 'launcher_ids': ['b']},
                '3': {'amount': int(REWARD_XCH * 0.991 * 0.5), 'pool_fee': int(REWARD_XCH * 0.009 * 0.5) + 1, 'launcher_ids': ['c']},
            },
            'amount_to_distribute': int(REWARD_XCH * 0.99 * 0.05) + int(REWARD_XCH * (1 - 0.00951) * 0.45) + int(REWARD_XCH * 0.991 * 0.5),
            'remainings': 0,
        },
    ),
    # Pool fee + stay of length + size
    (
        {'stay_d': 0.1, 'stay_l': 100, 'pool': 0.01, 'size': {100: 0.1, 500: 0.2}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 0, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '2', 'points': 900000, 'days_pooling': 50, 'estimated_size': 150 * 1024 ** 4, 'launcher_id': 'b'},
            {'payout_instructions': '3', 'points': 1000000, 'days_pooling': 150, 'estimated_size': 800 * 1024 ** 4, 'launcher_id': 'c'},
        ],
        {},
        REWARD_XCH,
        2000000,
        {
            'pool_fee_amount': int(
               D(REWARD_XCH * 0.05 * 0.01) +
               D(REWARD_XCH * 0.45 * 0.00851) +
               D(REWARD_XCH * 0.5 * 0.0075)
            ),
            'referral_fee_amount': 0,
            'additions': {
                '1': {'amount': int(D(REWARD_XCH) * D('0.99') * D('0.05')), 'pool_fee': int(D(REWARD_XCH) * D('0.01') * D('0.05')), 'launcher_ids': ['a']},
                '2': {'amount': int(REWARD_XCH * (1 - 0.00851) * 0.45), 'pool_fee': int(REWARD_XCH * 0.00851 * 0.45), 'launcher_ids': ['b']},
                '3': {'amount': int(REWARD_XCH * 0.9925 * 0.5), 'pool_fee': int(REWARD_XCH * 0.0075 * 0.5), 'launcher_ids': ['c']},
            },
            'amount_to_distribute': int(REWARD_XCH * 0.99 * 0.05) + int(REWARD_XCH * (1 - 0.00851) * 0.45) + int(REWARD_XCH * 0.9925 * 0.5),
            'remainings': 0,
        },
    ),
    # Pool fee + stay of length + size + referral
    (
        {'stay_d': 0.1, 'stay_l': 100, 'pool': 0.01, 'size': {100: 0.1, 500: 0.2}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 0, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '2', 'points': 900000, 'days_pooling': 50, 'estimated_size': 150 * 1024 ** 4, 'launcher_id': 'b'},
            {'payout_instructions': '3', 'points': 1000000, 'days_pooling': 150, 'estimated_size': 800 * 1024 ** 4, 'launcher_id': 'c'},
        ],
        {
            '3': {
                'id': 11,
                'target_payout_instructions': '4',
                'target_launcher_id': 'd',
            },
        },
        REWARD_XCH,
        2000000,
        {
            'pool_fee_amount': int(
               D(REWARD_XCH * 0.05 * 0.01) +
               D(REWARD_XCH * 0.45 * 0.00851) +
               D(REWARD_XCH * 0.5 * 0.0075 * 0.8) # 20% of fee goes to referral
            ),
            'referral_fee_amount': int(REWARD_XCH * 0.0075 * 0.5 * 0.2),
            'additions': {
                '1': {'amount': int(D(REWARD_XCH) * D('0.99') * D('0.05')), 'pool_fee': int(D(REWARD_XCH) * D('0.01') * D('0.05')), 'launcher_ids': ['a']},
                '2': {'amount': int(REWARD_XCH * (1 - 0.00851) * 0.45), 'pool_fee': int(REWARD_XCH * 0.00851 * 0.45), 'launcher_ids': ['b']},
                '3': {'amount': int(REWARD_XCH * 0.9925 * 0.5), 'referral': 11, 'referral_amount': int(REWARD_XCH * 0.0075 * 0.5 * 0.2), 'pool_fee': int(REWARD_XCH * 0.0075 * 0.5), 'launcher_ids': ['c']},
                '4': {'amount': int(REWARD_XCH * 0.0075 * 0.5 * 0.2), 'pool_fee': 0, 'launcher_ids': ['d']},
            },
            'amount_to_distribute': int(REWARD_XCH * 0.99 * 0.05) + int(REWARD_XCH * (1 - 0.00851) * 0.45) + int(REWARD_XCH * 0.9925 * 0.5) + int(REWARD_XCH * 0.0075 * 0.5 * 0.2),
            'remainings': 0,
        },
    ),
    # Pool fee + stay of length + size + 2 referrals
    (
        {'stay_d': 0.1, 'stay_l': 100, 'pool': 0.01, 'size': {100: 0.1, 500: 0.2}, 'max': D('0.25')},
        [
            {'payout_instructions': '1', 'points': 100000, 'days_pooling': 0, 'estimated_size': 1, 'launcher_id': 'a'},
            {'payout_instructions': '2', 'points': 900000, 'days_pooling': 50, 'estimated_size': 150 * 1024 ** 4, 'launcher_id': 'b'},
            {'payout_instructions': '3', 'points': 1000000, 'days_pooling': 150, 'estimated_size': 800 * 1024 ** 4, 'launcher_id': 'c'},
        ],
        {
            '3': {
                'id': 11,
                'target_payout_instructions': '4',
                'target_launcher_id': 'd',
            },
            '1': {
                'id': 12,
                'target_payout_instructions': '2',
                'target_launcher_id': 'b',
            }
        },
        REWARD_XCH,
        2000000,
        {
            'pool_fee_amount': int(
               D(REWARD_XCH * 0.05 * 0.01 * 0.8) +
               D(REWARD_XCH * 0.45 * 0.00851) +
               D(REWARD_XCH * 0.5 * 0.0075 * 0.8) # 20% of fee goes to referral
            ),
            'referral_fee_amount': (
                int(REWARD_XCH * 0.0075 * 0.5 * 0.2) +
                int(REWARD_XCH * 0.01 * 0.05 * 0.2)
            ),
            'additions': {
                '1': {'amount': int(REWARD_XCH * 0.99 * 0.05), 'referral': 12, 'referral_amount': int(REWARD_XCH * 0.05 * 0.01 * 0.2), 'pool_fee': int(REWARD_XCH * 0.01 * 0.05), 'launcher_ids': ['a']},
                '2': {'amount': int(REWARD_XCH * (1 - 0.00851) * 0.45) + int(REWARD_XCH * 0.05 * 0.01 * 0.2), 'pool_fee': int(REWARD_XCH * 0.00851 * 0.45), 'launcher_ids': ['b']},
                '3': {'amount': int(REWARD_XCH * 0.9925 * 0.5), 'referral': 11, 'referral_amount': int(REWARD_XCH * 0.0075 * 0.5 * 0.2), 'pool_fee': int(REWARD_XCH * 0.0075 * 0.5), 'launcher_ids': ['c']},
                '4': {'amount': int(REWARD_XCH * 0.0075 * 0.5 * 0.2), 'pool_fee': 0, 'launcher_ids': ['d']},
            },
            'amount_to_distribute': (
                int(REWARD_XCH * 0.99 * 0.05) +
                int(REWARD_XCH * (1 - 0.00851) * 0.45) +
                int(REWARD_XCH * 0.9925 * 0.5) +
                # referrals amounts
                int(REWARD_XCH * 0.0075 * 0.5 * 0.2) +
                int(REWARD_XCH * 0.01 * 0.05 * 0.2)
            ),
            'remainings': 0,
        },
    ),
])
async def test__create_share(fees, farmers_points_data, referrals, total_amount, total_points, rv):
    with localcontext() as ctx:
        ctx.prec = 17
        ctx.clear_flags()
        amock = AsyncMock()
        amock.get_referrals.return_value = referrals
        share = await create_share(
            amock,
            total_amount,
            total_points,
            farmers_points_data,
            fees['pool'], fees['stay_d'], fees['stay_l'], fees['size'], fees['max'],
        )
        assert share == rv
