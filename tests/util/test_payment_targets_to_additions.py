import pytest

from pool.util import payment_targets_to_additions


@pytest.mark.parametrize('payment_targets,min_payment,launcher_min_payment_limit,limit,rv', [
    (
        {
            'foo': [
                {'amount': 30000, 'min_payout': 50000},
            ],
        },
        0,
        True,
        None,
        [],
    ),
    (
        {
            'foo': [
                {'amount': 30000, 'min_payout': 10000},
            ],
        },
        0,
        True,
        None,
        [{'puzzle_hash': 'foo', 'amount': 30000}],
    ),
    (
        {
            'foo': [
                {'amount': 30000, 'min_payout': 80000},
            ],
        },
        0,
        False,
        None,
        [{'puzzle_hash': 'foo', 'amount': 30000}],
    ),
    (
        {
            'foo': [
                {'amount': 30000, 'min_payout': 50000},
                {'amount': 30000, 'min_payout': 50000},
            ],
        },
        0,
        True,
        None,
        [{'puzzle_hash': 'foo', 'amount': 60000}],
    ),
    (
        {
            'foo': [
                {'amount': 30000, 'min_payout': 10000},
                {'amount': 30000, 'min_payout': 10000},
            ],
            'bar': [
                {'amount': 50000, 'min_payout': 10000},
                {'amount': 50000, 'min_payout': 10000},
            ],
        },
        0,
        True,
        1,
        [{'puzzle_hash': 'foo', 'amount': 60000}],
    ),
])
def test_payment_targets_to_additions(
    payment_targets,
    min_payment,
    launcher_min_payment_limit,
    limit,
    rv,
):
    assert payment_targets_to_additions(
        payment_targets,
        min_payment,
        launcher_min_payment_limit,
        limit,
    ) == rv
