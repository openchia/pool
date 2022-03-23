import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from pool.util import days_pooling


def test__days_pooling__none():
    assert days_pooling(None, None, True) > 0


def test__days_pooling__not_member():
    assert days_pooling(None, None, False) == 0

@pytest.mark.parametrize('joined_at,left_at,days', [
    (
        datetime(2020, 10, 1, tzinfo=timezone.utc),
        datetime(2020, 10, 11, tzinfo=timezone.utc),
        10
    ),
    (
        datetime(2020, 10, 1, tzinfo=timezone.utc),
        datetime(2021, 10, 1, tzinfo=timezone.utc),
        365
    ),
    (
        datetime(2021, 10, 1, tzinfo=timezone.utc),
        datetime(2020, 10, 1, tzinfo=timezone.utc),
        31
    ),
])
@patch('pool.util.datetime')
def test__days_pooling__(mock, joined_at, left_at, days):
    mock.now.return_value = datetime(2021, 11, 1, tzinfo=timezone.utc)
    assert days_pooling(joined_at, left_at, True) == days
