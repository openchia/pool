import copy
import pytest
from unittest.mock import patch, AsyncMock

from pool.payment import subtract_fees
from pool.util import payment_targets_to_additions

DEC_XCH = 10 ** 11
FEE_XCH = 10 ** 10


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
