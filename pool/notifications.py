import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

from .task import common_loop, task_exception

logger = logging.getLogger('notifications')


class Notifications(object):

    def __init__(self, pool):
        self.pool = pool
        self.store = pool.store
        self.store_ts = pool.store_ts

        self.loop_launcher_size_task: Optional[asyncio.Task] = None

    async def start(self):
        self.loop_launcher_size_task = asyncio.create_task(common_loop(
            self.loop_launcher_size_drop,
            init_coro=self.loop_launcher_size_drop_init(),
            sleep=120,
            log=logger,
        ))

    async def stop(self):
        if self.loop_launcher_size_task:
            self.loop_launcher_size_task.cancel()

    async def loop_launcher_size_drop_init(self):
        self._drop_size_state = {}

    async def loop_launcher_size_drop(self):
        # FIXME: do not get it every round
        notifications = dict(filter(
            lambda x: bool(x[1]['size_drop']), (await self.store.get_notifications()).items()
        ))

        all_enabled = set(notifications.keys())
        existing_enabled = set(self._drop_size_state.keys())

        for i in existing_enabled - all_enabled:
            existing_enabled.remove(i)
            self._drop_size_state.pop(i, None)

        for i in all_enabled - existing_enabled:
            # Default values
            size_drop_interval = notifications[i].pop('size_drop_interval', None) or 60
            size_drop_percent = notifications[i].pop('size_drop_percent', None) or 25

            self._drop_size_state[i] = dict(
                last_checked=0,
                size_drop_interval=size_drop_interval,
                size_drop_percent=size_drop_percent,
                **notifications[i],
            )

        # Check at most 200 or 1/4 of the launchers per round to
        # spread the load
        max_checks_per_round = max(200, len(self._drop_size_state) / 4)

        logger.debug('Going to check size drop %d notifications', len(self._drop_size_state))

        time_now = time.monotonic()
        for launcher, attrs in list(filter(
            lambda x: x[1]['last_checked'] < time_now - x[1]['size_drop_interval'],
            self._drop_size_state.items()
        ))[:max_checks_per_round]:

            interval = attrs['size_drop_interval']
            interval_s = interval * 60

            # Only check launchers that have not been checked soon enough
            # 3 is arbitrary, re-checking times within the period
            if attrs['last_checked'] > (time.monotonic() - interval_s) // 3:
                logger.debug('Launcher %r already checked, skipping.', launcher)
                continue

            attrs['last_checked'] = time_now

            sizes = await self.store_ts.get_launcher_sizes(launcher, f'-{interval * 2}m')

            start = None
            end = None

            # Start from the end of the size points so we can get the closest interval to the end
            for date, size in reversed(sizes):
                if end is None:
                    end = (date, size)
                    continue
                if (end[0] - date).total_seconds() >= interval_s:
                    start = (date, size)
                    break

            logger.debug('Launcher %r sizes of %r and %r', launcher, start, end)

            if start is None or end is None:
                continue

            if start[1] == 0:
                continue

            # Get the percentage of size decreased
            rate = end[1] / start[1]

            logger.debug('Launcher %r size rate of %f', launcher, rate)

            # If the percentage increased or decreased lower than what we should alert on, skip
            if rate > 1 or rate >= (1 - attrs['size_drop_percent'] / 100):
                if attrs['size_drop_last_sent']:
                    await self.store.update_notifications_last_sent(launcher, 'size_drop', None)
                continue

            if attrs['size_drop_last_sent']:
                continue

            await self.pool.run_hook(
                'size_drop',
                await self.store.get_farmer_record(bytes.fromhex(launcher)),
                dict(rate=rate, **attrs),
            )

            await self.store.update_notifications_last_sent(
                launcher, 'size_drop', datetime.utcnow(),
            )

    @task_exception
    async def payment(self, payment_targets):

        payments = defaultdict(int)
        # Only send notification for launchers with a minimum payout
        for payouts in payment_targets.values():
            for payout in payouts:
                if not payout['min_payout']:
                    continue
                payments[payout['launcher_id']] += payout['amount']

        logger.info('%d payments to notify', len(payments))
        if payments:
            await self.pool.run_hook('payment', payments)
