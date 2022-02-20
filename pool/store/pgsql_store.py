from collections import defaultdict
from decimal import Decimal
import json
import logging
import math
from typing import Optional, Set, List, Tuple, Dict

import aiopg
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
from chia.protocols.pool_protocol import PostPartialPayload, PostPartialRequest
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.coin import Coin
from chia.types.coin_record import CoinRecord
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata

logger = logging.getLogger('pgsql_store')


class PgsqlPoolStore(AbstractPoolStore):
    def __init__(self, pool_config: Dict):
        super().__init__()
        self.pool_config = pool_config
        self.pool = None

    async def _execute(self, sql, args=None) -> Optional[List]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, args or [])
                if sql.lower().startswith(('select', 'with')) or ' returning ' in sql.lower():
                    return await cursor.fetchall()
                elif sql.lower().startswith('delete'):
                    return cursor.rowcount

    async def connect(self):
        if dsn := self.pool_config.get('database_dsn'):
            self.pool = await aiopg.create_pool(dsn)
        else:
            self.pool = await aiopg.create_pool(
                f'host={self.pool_config["database_host"]} '
                f'user={self.pool_config["database_user"]} '
                f'password={self.pool_config["database_password"]} '
                f'dbname={self.pool_config["database_name"]}'
            )

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    @staticmethod
    def _row_to_farmer_record(row) -> FarmerRecord:
        return FarmerRecord(
            bytes.fromhex(row[0]),
            bytes.fromhex(row[1]),
            row[2],
            bytes.fromhex(row[3]),
            G1Element.from_bytes(bytes.fromhex(row[4])),
            CoinSpend.from_bytes(row[5]),
            PoolState.from_bytes(row[6]),
            row[7],
            row[8],
            row[9],
            True if row[10] == 1 else False,
            row[11],
            row[12],
            row[13],
            row[14],
            row[15],
            row[16],
            row[17],
            row[18],
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT launcher_id, is_pool_member FROM farmer WHERE launcher_id = %s", (farmer_record.launcher_id.hex(),))
                exists = await cursor.fetchone()
                if not exists:
                    await cursor.execute(
                        "INSERT INTO farmer ("
                        "launcher_id, p2_singleton_puzzle_hash, delay_time, delay_puzzle_hash, authentication_public_key, singleton_tip, singleton_tip_state, points, difficulty, payout_instructions, is_pool_member, estimated_size, points_pplns, share_pplns, joined_at, left_at, push_payment, push_block_farmed, custom_difficulty, minimum_payout) VALUES ("
                        "%s,          %s,                       %s,         %s,                %s,                        %s,            %s,                  0,      %s,         %s,                  %s,             0,              0,            0,           NOW(),     NULL,    false,        true,              NULL,              NULL)",
                        (
                            farmer_record.launcher_id.hex(),
                            farmer_record.p2_singleton_puzzle_hash.hex(),
                            farmer_record.delay_time,
                            farmer_record.delay_puzzle_hash.hex(),
                            bytes(farmer_record.authentication_public_key).hex(),
                            bytes(farmer_record.singleton_tip),
                            bytes(farmer_record.singleton_tip_state),
                            int(farmer_record.difficulty),
                            farmer_record.payout_instructions,
                            bool(farmer_record.is_pool_member),
                        ),
                    )
                else:
                    if exists[1] and not farmer_record.is_pool_member:
                        left_at = ', left_at = NOW()'
                    elif not exists[1] and farmer_record.is_pool_member:
                        left_at = ', left_at = NULL'
                    else:
                        left_at = ''
                    await cursor.execute(
                        f"UPDATE farmer SET p2_singleton_puzzle_hash = %s, delay_time = %s, delay_puzzle_hash = %s, authentication_public_key = %s, singleton_tip = %s, singleton_tip_state = %s, difficulty = %s, payout_instructions = %s, is_pool_member = %s{left_at} WHERE launcher_id = %s",
                        (
                            farmer_record.p2_singleton_puzzle_hash.hex(),
                            farmer_record.delay_time,
                            farmer_record.delay_puzzle_hash.hex(),
                            bytes(farmer_record.authentication_public_key).hex(),
                            bytes(farmer_record.singleton_tip),
                            bytes(farmer_record.singleton_tip_state),
                            int(farmer_record.difficulty),
                            farmer_record.payout_instructions,
                            bool(farmer_record.is_pool_member),
                            farmer_record.launcher_id.hex(),
                        ),
                    )

    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        # TODO(pool): use cache
        row = await self._execute(
            "SELECT %s from farmer where launcher_id=%%s" % (', '.join((FarmerRecord.__annotations__.keys())), ),
            (launcher_id.hex(),),
        )
        if not row or not row[0]:
            return None
        return self._row_to_farmer_record(row[0])

    async def get_farmer_records(self, filters) -> Optional[FarmerRecord]:
        args = []
        where = []
        for k, op, v in filters:
            if op.lower() not in ('is not null', 'is null', '>', '=', '<', '>=', '<='):
                continue
            if op.lower() not in ('is not null', 'is null'):
                where.append(f'{k} {op} %s')
                args.append(v)
            else:
                where.append(f'{k} {op}')

        if not where:
            where.append('1 = 1')

        fields = ', '.join((FarmerRecord.__annotations__.keys()))
        return {
            i[0]: self._row_to_farmer_record(i)
            for i in await self._execute(
                f"SELECT {fields} FROM farmer WHERE {' AND '.join(where)}",
                args,
            )
        }

    async def update_estimated_size_and_pplns(self, launcher_id: str, size: int, points: int, share: Decimal):
        await self._execute(
            "UPDATE farmer SET estimated_size=%s, points_pplns=%s, share_pplns=%s WHERE launcher_id=%s", (size, points, share, launcher_id)
        )

    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        await self._execute(
            "UPDATE farmer SET difficulty=%s WHERE launcher_id=%s", (difficulty, launcher_id.hex())
        )

    async def update_singleton(
        self,
        farmer_record: FarmerRecord,
        singleton_coin: Coin,
        singleton_tip: CoinSpend,
        singleton_tip_state: PoolState,
        is_pool_member: bool,
    ):

        if farmer_record.is_pool_member and not is_pool_member:
            left_at = ', left_at = NOW()'
        elif not farmer_record.is_pool_member and is_pool_member:
            left_at = ', left_at = NULL'
        else:
            left_at = ''
        await self._execute(
            f'UPDATE farmer SET singleton_tip=%s, singleton_tip_state=%s, is_pool_member=%s{left_at} WHERE launcher_id=%s',
            (
                bytes(singleton_tip),
                bytes(singleton_tip_state),
                is_pool_member,
                farmer_record.launcher_id.hex(),
            ),
        )
        if singleton_coin:
            await self._execute(
                'INSERT INTO singleton ('
                ' launcher_id, singleton_name, singleton_tip, singleton_tip_state, created_at'
                ') VALUES ('
                ' %s,          %s,             %s,            %s,                  NOW()'
                ')',
                (
                    farmer_record.launcher_id.hex(),
                    singleton_coin.name().hex(),
                    bytes(singleton_tip),
                    bytes(singleton_tip_state),
                ),
            )

    async def update_farmer(self, launcher_id: bytes32, attributes: List, values: List) -> None:
        attrs = []
        farmer_attributes = list(FarmerRecord.__annotations__.keys())
        for i in attributes:
            if i not in farmer_attributes:
                raise RuntimeError(f'{i} is not a valid farmer attribute')
            attrs.append(f'{i} = %s')

        values = list(values)
        values.append(launcher_id.hex())
        await self._execute(f"UPDATE farmer SET {', '.join(attrs)} WHERE launcher_id = %s", values)

    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        """
        Only get farmers that are still members or that have left in less than 6 hours.
        6 hours is arbitrary, in theory we could be as low as 10 minutes.
        """
        rows = await self._execute(
            "SELECT p2_singleton_puzzle_hash FROM farmer WHERE is_pool_member = true OR ("
            " is_pool_member = false AND left_at >= NOW() - interval '6 hours'"
            ")"
        )

        all_phs: Set[bytes32] = set()
        for row in rows:
            all_phs.add(bytes32(bytes.fromhex(row[0])))
        return all_phs

    async def get_farmer_records_for_p2_singleton_phs(self, puzzle_hashes: Set[bytes32]) -> List[FarmerRecord]:
        if len(puzzle_hashes) == 0:
            return []
        puzzle_hashes_db = tuple([ph.hex() for ph in list(puzzle_hashes)])
        rows = await self._execute(
            f'SELECT {", ".join((FarmerRecord.__annotations__.keys()))} from farmer WHERE p2_singleton_puzzle_hash in ({"%s," * (len(puzzle_hashes_db) - 1)}%s) ',
            puzzle_hashes_db,
        )
        return [self._row_to_farmer_record(row) for row in rows]

    async def get_farmer_points_and_payout_instructions(self) -> List[Tuple[uint64, bytes]]:
        rows = await self._execute("SELECT points, payout_instructions FROM farmer WHERE is_pool_member = true")
        accumulated: Dict[bytes32, uint64] = {}
        for row in rows:
            points: uint64 = uint64(row[0])
            ph: bytes32 = bytes32(bytes.fromhex(row[1]))
            if ph in accumulated:
                accumulated[ph] += points
            else:
                accumulated[ph] = points

        ret: List[Tuple[uint64, bytes32]] = []
        for ph, total_points in accumulated.items():
            ret.append((total_points, ph))
        return ret

    async def get_launcher_id_and_payout_instructions(self, reward_system) -> dict:
        if reward_system == 'PPLNS':
            field = 'points_pplns'
        else:
            field = 'points'
        return {
            i[0]: bytes32(bytes.fromhex(i[1]))
            for i in await self._execute(
                f'SELECT launcher_id, payout_instructions FROM farmer WHERE {field} > 0'
            )
        }

    async def clear_farmer_points(self) -> None:
        await self._execute("UPDATE farmer set points=0")

    async def add_pending_partial(
        self,
        partial: PostPartialRequest,
        req_metadata: RequestMetadata,
        time_received: uint64,
        points_received: uint64,
    ) -> None:
        await self._execute(
            "INSERT INTO pending_partial ("
            "partial, req_metadata, time_received, points_received"
            ") VALUES ("
            "%s,      %s,           %s,            %s"
            ")",
            (
                json.dumps(partial.to_json_dict()),
                json.dumps(req_metadata.to_json_dict()),
                int(time_received),
                int(points_received),
            ),
        )

    async def get_pending_partials(self) -> List[Tuple[
        PostPartialRequest, Optional[RequestMetadata], uint64, uint64
    ]]:
        partials = [
            (
                PostPartialRequest.from_json_dict(i[0]),
                RequestMetadata.from_json_dict(i[1]) if i[1] else None,
                uint64(i[2]),
                uint64(i[3]),
            )
            for i in await self._execute(
                "SELECT partial, req_metadata, time_received, points_received FROM pending_partial "
                "ORDER BY id ASC"
            )
        ]
        await self._execute("DELETE FROM pending_partial")
        return partials

    async def add_partial(self,
        partial_payload: PostPartialPayload,
        req_metadata: Optional[RequestMetadata],
        timestamp: uint64,
        difficulty: uint64,
        error: Optional[str] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO partial ("
                    " launcher_id, timestamp, difficulty, error, harvester_id, plot_id,"
                    " chia_version, remote, pool_host"
                    ") VALUES ("
                    " %s,          %s,        %s,         %s,    %s,           %s,"
                    " %s,           %s,     %s"
                    ")",
                    (
                        partial_payload.launcher_id.hex(),
                        timestamp,
                        difficulty,
                        error,
                        partial_payload.harvester_id.hex(),
                        partial_payload.proof_of_space.get_plot_id().hex(),
                        ((req_metadata.get_chia_version() or '')[:20] or None) if req_metadata else None,
                        (req_metadata.remote or None) if req_metadata else None,
                        req_metadata.get_host() if req_metadata else None,
                    ),
                )

            if error is None:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE farmer SET points = points + %s WHERE launcher_id=%s",
                        (difficulty, partial_payload.launcher_id.hex()),
                    )

    async def get_recent_partials(self, start_time, launcher_id: Optional[str] = None) -> List[Tuple[str, int, int]]:
        args = [start_time]
        if launcher_id:
            args.append(launcher_id)
        rows = await self._execute(
            "SELECT p.launcher_id, p.timestamp, p.difficulty FROM partial p "
            "JOIN farmer f ON p.launcher_id = f.launcher_id "
            "WHERE p.timestamp >= %s AND p.error IS NULL{} AND f.is_pool_member = true ORDER BY p.timestamp ASC".format(' AND f.launcher_id = %s' if launcher_id else ''),
            args,
        )
        ret: List[Tuple[str, int, int]] = [
            (launcher_id, timestamp, difficulty)
            for launcher_id, timestamp, difficulty in rows
        ]
        return ret

    async def get_points_per_pool_host(self, start_time) -> Dict[str, int]:
        # FIXME: Verify query speed, check index on timestamp
        return {
            i[0]: i[1]
            for i in await self._execute(
                'SELECT p.pool_host, SUM(p.difficulty) FROM partial p'
                ' JOIN farmer f ON p.launcher_id = f.launcher_id'
                ' WHERE f.is_pool_member = true AND p.error IS NULL AND p.timestamp > %s'
                ' GROUP BY p.pool_host',
                (start_time, ),
            )
        }

    async def get_launchers_without_recent_partials(self, start_time) -> List[bytes32]:
        return [
            bytes32(bytes.fromhex(i[0]))
            for i in await self._execute(
                "SELECT DISTINCT p.launcher_id FROM partial p INNER JOIN farmer f "
                "ON p.launcher_id = f.launcher_id "
                "WHERE f.is_pool_member = true AND p.launcher_id NOT IN ("
                " SELECT DISTINCT launcher_id FROM partial WHERE timestamp >= %s "
                ")",
                [start_time],
            )
        ]

    async def add_block(
        self,
        reward_record: CoinRecord,
        absorb_fee: int,
        singleton: bytes32,
        farmer: FarmerRecord,
        pool_space: int,
        estimate_to_win: int,
    ) -> None:
        last_block = await self._execute(
            "SELECT estimate_to_win, timestamp FROM block ORDER BY -confirmed_block_index LIMIT 1"
        )
        if last_block and last_block[0]:
            last_etw = last_block[0][0]
            last_timestamp = last_block[0][1]

            # Effective ETW is the mean between last ETW and current ETW
            if last_etw != -1:
                effective_etw = (last_etw + estimate_to_win) / 2
            else:
                effective_etw = estimate_to_win

            time_since_last = int(reward_record.timestamp) - last_timestamp
            # If time is negative means we are adding a block that was won some time ago
            # e.g. farmer that wasn't sending partials to the pool
            if time_since_last < 0:
                luck = 0
            else:
                luck = int((time_since_last / effective_etw) * 100)
        else:
            luck = 100

        globalinfo = await self.get_globalinfo()

        await self._execute(
            "INSERT INTO block ("
            " name, singleton, timestamp, farmed_height, confirmed_block_index, puzzle_hash, amount, farmed_by_id, estimate_to_win, pool_space, luck, absorb_fee, xch_price"
            ") VALUES ("
            " %s,   %s,        %s,        %s,            %s,                    %s,          %s,     %s,           %s,              %s,         %s,   %s,         %s"
            ")",
            (
                reward_record.name.hex(),
                singleton.hex(),
                int(reward_record.timestamp),
                int.from_bytes(bytes(reward_record.coin.parent_coin_info)[16:], 'big'),
                int(reward_record.confirmed_block_index),
                reward_record.coin.puzzle_hash.hex(),
                int(reward_record.coin.amount),
                farmer.launcher_id.hex(),
                estimate_to_win,
                pool_space,
                luck,
                absorb_fee,
                json.dumps(globalinfo['xch_current_price']) if globalinfo else None,
            )
        )

    async def block_exists(self, singleton: str) -> bool:
        return bool(await self._execute('SELECT id FROM block WHERE singleton = %s', [singleton]))

    async def get_farmer_record_from_singleton(self, singleton: bytes32) -> Optional[FarmerRecord]:
        rv = await self._execute('SELECT launcher_id FROM singleton WHERE singleton_name = %s', [
            singleton.hex()
        ])
        if not rv:
            return None
        return await self.get_farmer_record(bytes32(bytes.fromhex(rv[0][0])))

    async def singleton_exists(self, launcher_id: bytes32) -> Optional[bytes32]:
        rv = await self._execute(
            'SELECT singleton_name FROM singleton WHERE launcher_id = %s LIMIT 1',
            (launcher_id.hex(), ),
        )
        if not rv:
            return None
        return True

    async def add_payout(
        self,
        coin_records,
        pool_puzzle_hash,
        fee_puzzle_hash,
        amount,
        pool_fee,
        referral,
        payment_targets
    ) -> int:

        assert len(payment_targets) > 0

        payout_id = None
        coin_rewards_added = []
        payout_addresses_ids = []
        try:
            rv = await self._execute(
                "INSERT INTO payout "
                "(datetime, amount, fee, referral) VALUES "
                "(NOW(),    %s,     %s,  %s) "
                "RETURNING id",
                (amount, pool_fee, referral),
            )
            payout_id = rv[0][0]
            for coin_record in coin_records:
                await self._execute(
                    'INSERT INTO coin_reward ('
                    ' name, payout_id'
                    ') VALUES ('
                    ' %s,   %s'
                    ')',
                    (coin_record.name.hex(), payout_id),
                )
                coin_rewards_added.append(coin_record)

                await self._execute(
                    "UPDATE block SET payout_id = %s WHERE singleton = %s",
                    (payout_id, coin_record.coin.parent_coin_info.hex()),
                )

            max_additions = self.pool_config["max_additions_per_transaction"]
            rounds = math.ceil(len(payment_targets) / max_additions)

            # We can lose one mojo here due to rounding, but not important
            pool_fee_per_round = int(pool_fee / rounds)

            payout_round = 1
            for idx, i in enumerate(payment_targets):

                if idx % max_additions == 0:
                    payout_round += 1
                    rv = await self._execute(
                        "INSERT INTO payout_address "
                        "(payout_id, payout_round, fee, tx_fee, puzzle_hash, pool_puzzle_hash, launcher_id, amount, referral_id, referral_amount, transaction_id) "
                        "VALUES "
                        "(%s,        %s,           true, 0,     %s,          %s,               NULL,        %s,     NULL,        0,               NULL) "
                        "RETURNING id",
                        (payout_id, payout_round, fee_puzzle_hash.hex(), pool_puzzle_hash.hex(), pool_fee_per_round),
                    )
                    payout_addresses_ids.append(rv[0][0])

                rv = await self._execute(
                    "SELECT launcher_id FROM farmer WHERE payout_instructions = %s "
                    "ORDER BY joined_at NULLS FIRST",
                    (i["puzzle_hash"].hex(),),
                )
                if rv and rv[0]:
                    farmer = "'" + rv[0][0] + "'"
                else:
                    farmer = 'NULL'
                rv = await self._execute(
                    "INSERT INTO payout_address "
                    "(payout_id, payout_round, fee, tx_fee, puzzle_hash, pool_puzzle_hash, launcher_id, amount, referral_id, referral_amount, transaction_id) "
                    "VALUES "
                    "(%%s,       %%s,          false, %%s,  %%s,         %%s,              %s,          %%s,    %%s,         %%s,             NULL) "
                    "RETURNING id" % (farmer,),
                    (payout_id, payout_round, i.get('tx_fee') or 0, i["puzzle_hash"].hex(), pool_puzzle_hash.hex(), i["amount"], i.get('referral'), i.get('referral_amount') or 0),
                )
                payout_addresses_ids.append(rv[0][0])
                if referral := i.get('referral'):
                    # FIXME: rollback referral
                    await self._execute(
                        "UPDATE referral_referral SET total_income = total_income + %s WHERE id = %s",
                        (i.get('referral_amount') or 0, referral),
                    )
            return payout_id
        except Exception as e:
            try:
                for coin_record in coin_rewards_added:
                    await self._execute(
                        "DELETE FROM coin_reward WHERE name = %s",
                        (coin_record.name.hex(),),
                    )
                for pid in payout_addresses_ids:
                    await self._execute(
                        "DELETE FROM payout_address WHERE id = %s",
                        (pid,),
                    )
                if payout_id:
                    await self._execute(
                        "UPDATE block SET payout_id = NULL WHERE payout_id = %s",
                        (payout_id,),
                    )
                    await self._execute(
                        "DELETE FROM payout WHERE id = %s",
                        (payout_id,),
                    )
            except Exception:
                logger.error('Failed to rollback', exc_info=True)
            raise e

    async def add_transaction(self, transaction, payment_targets) -> None:
        ids = []
        for targets in payment_targets.values():
            ids += [str(i['id']) for i in targets]

        globalinfo = await self.get_globalinfo()

        rv = await self._execute(
            "INSERT INTO transaction ("
            " transaction, xch_price, created_at_time"
            ") VALUES ("
            " %s,          %s,        to_timestamp(%s)"
            ") RETURNING id",
            (
                transaction.name.hex(),
                json.dumps(globalinfo['xch_current_price']) if globalinfo else None,
                int(transaction.created_at_time),
            ),
        )
        tx_id = rv[0][0]
        await self._execute(
            "UPDATE payout_address SET transaction_id = %s WHERE id IN ({})".format(
                ', '.join(ids)
            ),
            (tx_id,),
        )

    async def get_pending_payments_coins(self, pool_puzzle_hash: bytes32):
        return [
            i[0]
            for i in await self._execute(
                'WITH unconfirmed AS ('
                ' SELECT DISTINCT payout_id FROM payout_address p LEFT JOIN transaction t '
                '  ON p.transaction_id = t.id '
                '  WHERE pool_puzzle_hash = %s AND t.confirmed_block_index IS NULL'
                ') SELECT name FROM coin_reward c JOIN unconfirmed u ON c.payout_id = u.payout_id',
                (pool_puzzle_hash.hex(), ),
            )
        ]

    async def get_coin_rewards_from_payout_ids(self, payout_ids: Set[int]) -> Set[bytes32]:
        return {
            bytes32(bytes.fromhex(i[0]))
            for i in await self._execute(
                'SELECT name FROM coin_reward WHERE payout_id IN ({})'.format(
                    ', '.join([str(j) for j in payout_ids])
                ),
            )
        }

    async def get_pending_payment_targets(self, pool_puzzle_hash: bytes32):
        payment_targets_per_tx = defaultdict(lambda: defaultdict(list))
        for i in await self._execute(
            'SELECT p.id, t.transaction, p.payout_id, p.puzzle_hash, p.amount, p.fee,'
            '  f.minimum_payout'
            ' FROM payout_address p LEFT JOIN transaction t ON p.transaction_id = t.id'
            '  LEFT JOIN farmer f ON p.launcher_id = f.launcher_id'
            ' WHERE p.pool_puzzle_hash = %s AND t.confirmed_block_index IS NULL'
            ' ORDER BY p.id ASC',
            (pool_puzzle_hash.hex(), ),
        ):

            if i[1]:
                tx_id = bytes32(bytes.fromhex(i[1]))
            else:
                tx_id = i[1]
            payment_targets_per_tx[tx_id][bytes.fromhex(i[3])].append({
                "id": i[0],
                "payout_id": i[2],
                "amount": i[4],
                "fee": i[5],
                "min_payout": i[6],
            })
        return payment_targets_per_tx

    async def confirm_transaction(self, transaction, payment_targets):
        await self._execute(
            "UPDATE transaction SET confirmed_block_index = %s WHERE transaction = %s",
            (int(transaction.confirmed_at_height), transaction.name.hex()),
        )
        for targets in payment_targets.values():
            for i in targets:
                await self._execute(
                    "UPDATE payout_address SET amount = %s, tx_fee = %s WHERE id = %s",
                    (i['amount'], i.get('tx_fee') or 0, i['id']),
                )

    async def remove_transaction(self, tx_id: bytes32):
        rowid = await self._execute(
            "SELECT id FROM transaction WHERE transaction = %s",
            (tx_id.hex(), ),
        )
        rowid = rowid[0][0]
        await self._execute(
            "UPDATE payout_address SET transaction_id = NULL WHERE transaction_id = %s",
            (rowid, ),
        )
        await self._execute(
            "DELETE FROM transaction WHERE transaction = %s",
            (tx_id.hex(), ),
        )

    async def get_last_singletons(self) -> List[str]:
        # Get the last 10 singletons of each launcher
        singletons = [
            i[0] for i in
            await self._execute(
                'WITH c AS ('
                ' SELECT singleton_name, row_number() OVER ('
                '  PARTITION BY launcher_id ORDER BY created_at DESC'
                ' ) AS singleton_rank FROM singleton ORDER BY created_at DESC'
                ') SELECT singleton_name FROM c WHERE singleton_rank <= 10'
            )
        ]
        return singletons

    async def set_pool_size(self, size: int) -> None:
        await self._execute(
            "INSERT INTO space (date, size) VALUES (NOW(), %s)",
            (size,)
        )

    async def scrub_pplns(self, start_time: int) -> None:
        await self._execute(
            "UPDATE farmer SET points_pplns = 0, share_pplns = 0 WHERE launcher_id NOT IN ("
            "SELECT DISTINCT launcher_id FROM partial WHERE timestamp >= %s AND error IS NULL"
            ")",
            (start_time,)
        )

    async def set_globalinfo(self, attrs: Dict) -> None:
        args = []
        sql = []
        for i in (
            'xch_current_price',
            'blockchain_height',
            'blockchain_space',
            'blockchain_avg_block_time',
            'wallets',
        ):
            if i in attrs:
                sql.append(f'{i} = %s')
                args.append(attrs[i])
        await self._execute(f"UPDATE globalinfo SET {', '.join(sql)}", args)

    async def get_globalinfo(self) -> Dict:
        rv = await self._execute(
            "SELECT"
            " xch_current_price,"
            " blockchain_height,"
            " blockchain_space,"
            " blockchain_avg_block_time,"
            " wallets"
            " FROM globalinfo WHERE id = 1"
        )
        if not rv:
            return None
        rv = rv[0]
        return {
            'xch_current_price': rv[0],
            'blockchain_height': rv[1],
            'blockchain_space': rv[2],
            'blockchain_avg_block_time': rv[3],
            'wallets': rv[4],
        }

    async def get_referrals(self):
        # TODO: only get launchers id getting paid
        return {
            bytes32(bytes.fromhex(i[1])): {
                'id': i[0],
                'target_payout_instructions': bytes32(bytes.fromhex(i[2])),
            } for i in await self._execute(
                "SELECT r.id, fl.payout_instructions, fr.payout_instructions "
                "FROM referral_referral r INNER JOIN farmer fr ON r.referrer_id = fr.launcher_id "
                "INNER JOIN farmer fl ON r.launcher_id = fl.launcher_id "
                "WHERE active = true"
            )
        }

    async def get_block_timestamp(self, before_timestamp: int):
        # farmed_height is indexed, faster
        rv = await self._execute(
            "SELECT timestamp FROM block WHERE timestamp <= %s ORDER BY farmed_height DESC LIMIT 1",
            (before_timestamp, ),
        )
        if rv:
            return rv[0][0]

    async def remove_partials(self, before_timestamp: int) -> int:
        rowcount = await self._execute(
            "DELETE FROM partial WHERE timestamp <= %s",
            (before_timestamp, ),
        )
        return rowcount
