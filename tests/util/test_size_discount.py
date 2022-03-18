import pytest

from pool.util import size_discount


TiB = 1024 ** 4

DISCOUNTS = {
    200: 0.05,
    500: 0.1,
    1000: 0.15,
}


@pytest.mark.parametrize('size, discounts, rv', [
    (10 * TiB, DISCOUNTS, 0),
    (200 * TiB - 1, DISCOUNTS, 0),
    (200 * TiB, DISCOUNTS, 0.05),
    (300 * TiB, DISCOUNTS, 0.05),
    (500 * TiB - 1, DISCOUNTS, 0.05),
    (500 * TiB, DISCOUNTS, 0.1),
    (800 * TiB, DISCOUNTS, 0.1),
    (1000 * TiB, DISCOUNTS, 0.15),
    (10000 * TiB, DISCOUNTS, 0.15),
    (300 * TiB, dict(reversed(sorted(DISCOUNTS.items()))), 0.05),
    (10000 * TiB, dict(reversed(sorted(DISCOUNTS.items()))), 0.15),
])
def test__size_discount(size, discounts, rv):
    assert size_discount(size, discounts) == rv
