import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from pool.store.pgsql_store import left_join_cooldown


@pytest.mark.parametrize(
    'now, left_last_at, left_at, is_member, was_member, cooldown_hours, rv',
    [
        # joining soon enough
        (
            datetime(2022, 3, 1, 4, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            True,
            False,
            12,
            [
                ('left_last_at', None),
            ],
        ),
        # joining at exactly cooldown
        (
            datetime(2022, 3, 1, 12, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            True,
            False,
            12,
            [
                ('left_last_at', None),
            ],
        ),
        # joining a second after cooldown
        (
            datetime(2022, 3, 1, 12, 0, 1, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            True,
            False,
            12,
            [
                ('joined_last_at', datetime(2022, 3, 1, 12, 0, 1, tzinfo=timezone.utc)),
            ],
        ),
        # joining soon enough with previous left already
        (
            datetime(2022, 3, 1, 4, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            datetime(2022, 2, 1, tzinfo=timezone.utc),
            True,
            False,
            12,
            [
                ('left_last_at', datetime(2022, 2, 1, tzinfo=timezone.utc)),
            ],
        ),
        # joining after cooldown
        (
            datetime(2022, 3, 2, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            True,
            False,
            12,
            [
                ('joined_last_at', datetime(2022, 3, 2, tzinfo=timezone.utc))
            ],
        ),
        # leaving pool
        (
            datetime(2022, 3, 1, 4, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            False,
            True,
            12,
            [
                ('left_last_at', datetime(2022, 3, 1, 4, tzinfo=timezone.utc)),
                ('left_at', datetime(2022, 3, 1, tzinfo=timezone.utc)),
                ('last_block_timestamp', None),
                ('last_block_etw', None),
            ],
        ),
        # not in the pool
        (
            datetime(2022, 3, 1, 4, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            False,
            False,
            12,
            [],
        ),
        # already in the pool
        (
            datetime(2022, 3, 1, 4, tzinfo=timezone.utc),
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            None,
            True,
            True,
            12,
            [],
        ),
    ],
)
@patch('pool.store.pgsql_store.datetime.datetime')
def test__left_join_cooldown(
    mock, now, left_last_at, left_at, is_member, was_member, cooldown_hours, rv
):

    mock.now.return_value = now

    _rv = left_join_cooldown(
        left_last_at, left_at, is_member, was_member, cooldown_hours,
    )
    assert _rv == rv
