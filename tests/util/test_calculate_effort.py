import pytest

from pool.util import calculate_effort


@pytest.mark.parametrize('last_etw, last_timestamp, now_etw, now_timestamp, rv', [
    (60 * 60, 16500000000, 60 * 60,     16500000000 + 60 * 60, 100),
    (60 * 60, 16500000000, 60 * 60 * 2, 16500000000 + 60 * 60, 66),
    (-1,      16500000000, 60 * 60 * 2, 16500000000 + 60 * 60, 50),
    (-1,      16500000000, 60 * 60 / 2, 16500000000 + 60 * 60, 200),
    (60 * 60, 16500000000, 60 * 60 / 2, 16500000000 + 60 * 60, 133),
])
def test__calculate_effort(last_etw, last_timestamp, now_etw, now_timestamp, rv):
    assert int(calculate_effort(last_etw, last_timestamp, now_etw, now_timestamp)) == rv
