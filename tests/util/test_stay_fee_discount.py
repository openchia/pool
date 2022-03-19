import pytest
from decimal import Decimal as D, getcontext

from pool.util import stay_fee_discount


getcontext().prec = 7


@pytest.mark.parametrize('discount,length,days,rv', [
    (0.5, 14, 7, D('0.25')),
    (0.5, 100, 50, D('0.245')),
    (0.1, 70, 7, D('0.01')),
    (0.1, 70, 8, D('0.01')),
    (0.1, 70, 13, D('0.01')),
    (0.1, 70, 14, D('0.02')),
    (0.1, 70, 14, D('0.02')),
    (0.5, 0, 1, D('0')),
    (0, 0, 1, D('0')),
    (0, 0, 0, D('0')),
    (0, 1, 0, D('0')),
])
def test__stay_fee_discount(discount, length, days, rv):
    assert stay_fee_discount(discount, length, days) == rv
