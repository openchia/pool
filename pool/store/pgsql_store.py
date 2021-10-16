from collections import defaultdict
from decimal import Decimal
import json
import math
from typing import Optional, Set, List, Tuple, Dict

import aiopg
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
from chia.protocols.pool_protocol import PostPartialPayload, PostPartialRequest
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_record import CoinRecord
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata


class PgsqlPoolStore(AbstractPoolStore):
    def __init__(self, pool_config: Dict):
        super().__init__()
        self.pool_config = pool_config
        self.pool = None

    async def _execute(self, sql, args=None) -> Optional[List]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, args or [])
                if sql.lower().startswith('select') or ' returning ' in sql.lower():
                    return await cursor.fetchall()

    async def connect(self):
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
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT launcher_id, is_pool_member FROM farmer WHERE launcher_id = %s", (farmer_record.launcher_id.hex(),))
                exists = await cursor.fetchone()
                if not exists:
                    await cursor.execute(
                        "INSERT INTO farmer ("
                        "launcher_id, p2_singleton_puzzle_hash, delay_time, delay_puzzle_hash, authentication_public_key, singleton_tip, singleton_tip_state, points, difficulty, payout_instructions, is_pool_member, estimated_size, points_pplns, share_pplns, joined_at, left_at) VALUES ("
                        "%s,          %s,                       %s,         %s,                %s,                        %s,            %s,                  0,      %s,         %s,                  %s,             0,              0,            0,           NOW(),     NULL)",
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
                    if not exists[1] and farmer_record.is_pool_member:
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

    async def add_pending_partial(self, partial: PostPartialRequest, time_received: uint64, points_received: uint64) -> None:
        await self._execute(
            "INSERT INTO pending_partial ("
            "partial, time_received, points_received"
            ") VALUES ("
            "%s     , %s           , %s"
            ")",
            (json.dumps(partial.to_json_dict()), int(time_received), int(points_received)),
        )

    async def get_pending_partials(self) -> List[Tuple[PostPartialRequest, uint64, uint64]]:
        partials = [
            (
                PostPartialRequest.from_json_dict(i[0]),
                uint64(i[1]),
                uint64(i[2]),
            )
            for i in await self._execute(
                "SELECT partial, time_received, points_received FROM pending_partial "
                "ORDER BY id ASC"
            )
        ]
        await self._execute("DELETE FROM pending_partial")
        return partials

    async def add_partial(self, partial_payload: PostPartialPayload, timestamp: uint64, difficulty: uint64, error: Optional[str] = None):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO partial ("
                    " launcher_id, timestamp, difficulty, error, harvester_id"
                    ") VALUES ("
                    " %s,          %s,        %s,         %s,    %s"
                    ")",
                    (
                        partial_payload.launcher_id.hex(),
                        timestamp,
                        difficulty,
                        error,
                        partial_payload.harvester_id.hex(),
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
        self, coin_record: CoinRecord, singleton_coin_record: CoinRecord, farmer: FarmerRecord,
        pool_space: int, estimate_to_win: int,
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

            time_since_last = int(coin_record.timestamp) - last_timestamp
            # If time is negative means we are adding a block that was won some time ago
            # e.g. farmer that wasn't sending partials to the pool
            if time_since_last < 0:
                luck = 0
            else:
                luck = int((time_since_last / effective_etw) * 100)
        else:
            luck = 100

        await self._execute(
            "INSERT INTO block ("
            " name, singleton, timestamp, farmed_height, confirmed_block_index, puzzle_hash, amount, farmed_by_id, estimate_to_win, pool_space, luck"
            ") VALUES ("
            " %s,   %s,        %s,        %s,            %s,                    %s,          %s,     %s,           %s             , %s,         %s"
            ")",
            (
                coin_record.name.hex(),
                singleton_coin_record.name.hex(),
                int(coin_record.timestamp),
                int.from_bytes(bytes(coin_record.coin.parent_coin_info)[16:], 'big'),
                int(coin_record.confirmed_block_index),
                coin_record.coin.puzzle_hash.hex(),
                int(coin_record.coin.amount),
                farmer.launcher_id.hex(),
                estimate_to_win,
                pool_space,
                luck,
            )
        )

    async def add_payout(self, coin_records, pool_puzzle_hash, fee_puzzle_hash, amount, pool_fee, referral, payment_targets) -> int:

        assert len(payment_targets) > 0

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
                await self._execute(
                    "INSERT INTO payout_address "
                    "(payout_id, payout_round, fee, puzzle_hash, pool_puzzle_hash, launcher_id, amount, referral_id, referral_amount, transaction) "
                    "VALUES "
                    "(%s,        %s,           true, %s,         %s,               NULL,        %s,     NULL,        0,               NULL)",
                    (payout_id, payout_round, fee_puzzle_hash.hex(), pool_puzzle_hash.hex(), pool_fee_per_round),
                )

            rv = await self._execute(
                "SELECT launcher_id FROM farmer WHERE payout_instructions = %s",
                (i["puzzle_hash"].hex(),),
            )
            if rv and rv[0]:
                farmer = "'" + rv[0][0] + "'"
            else:
                farmer = 'NULL'
            await self._execute(
                "INSERT INTO payout_address "
                "(payout_id, payout_round, fee, puzzle_hash, pool_puzzle_hash, launcher_id, amount, referral_id, referral_amount, transaction) "
                "VALUES "
                "(%%s,       %%s,          false, %%s,       %%s,              %s,          %%s,    %%s,         %%s,              NULL)" % (farmer,),
                (payout_id, payout_round, i["puzzle_hash"].hex(), pool_puzzle_hash.hex(), i["amount"], i.get('referral'), i.get('referral_amount') or 0),
            )
            if referral := i.get('referral'):
                await self._execute(
                    "UPDATE referral_referral SET total_income = total_income + %s WHERE id = %s",
                    (i.get('referral_amount') or 0, referral),
                )
        return payout_id

    async def add_transaction(self, transaction, payment_targets) -> None:
        ids = [str(i['id']) for i in payment_targets]
        await self._execute(
            "UPDATE payout_address SET transaction = %s WHERE id IN ({})".format(
                ', '.join(ids)
            ),
            (transaction.name.hex(),),
        )

    async def pending_payment_targets_exists(self, pool_puzzle_hash: bytes32):
        return (await self._execute(
            "SELECT COUNT(*) FROM payout_address WHERE pool_puzzle_hash = %s AND confirmed_block_index IS NULL",
            (pool_puzzle_hash.hex(), ),
        ))[0][0] > 0

    async def get_pending_payment_targets(self, pool_puzzle_hash: bytes32):
        payment_targets_per_tx = defaultdict(list)
        payout_round = None
        for i in await self._execute(
            "SELECT id, transaction, payout_id, puzzle_hash, amount, payout_round, fee FROM payout_address WHERE pool_puzzle_hash = %s AND confirmed_block_index IS NULL ORDER BY payout_round ASC, fee DESC, id ASC" ,
            (pool_puzzle_hash.hex(), ),
        ):
            # Only get payout addresses for the same round
            if payout_round is None:
                payout_round = i[5]
            elif payout_round != i[5]:
                break

            if i[1]:
                tx_id = bytes32(bytes.fromhex(i[1]))
            else:
                tx_id = i[1]
            payment_targets_per_tx[tx_id].append({
                "id": i[0],
                "payout_id": i[2],
                "puzzle_hash": bytes32(bytes.fromhex(i[3])),
                "amount": i[4],
                "fee": i[6],
            })
        return payment_targets_per_tx

    async def confirm_transaction(self, transaction, fee_payment_target):
        await self._execute(
            "UPDATE payout_address SET confirmed_block_index = %s WHERE transaction = %s",
            (int(transaction.confirmed_at_height), transaction.name.hex()),
        )
        if int(transaction.fee_amount) > 0:
            # Subtract the transaction fee from pool fee
            await self._execute(
                "UPDATE payout_address SET amount = amount - %s WHERE id = %s",
                (int(transaction.fee_amount), fee_payment_target['id']),
            )

    async def get_last_block_singletons(self) -> List[str]:
        return [
            i[0] for i in
            await self._execute(
                "SELECT singleton FROM block ORDER BY confirmed_block_index DESC LIMIT 20"
            )
        ]

    async def set_pool_size(self, size: int) -> None:
        await self._execute(
            "INSERT INTO space (date, size) VALUES (NOW(), %s)",
            (size,)
        )

    async def scrub_pplns(self, start_time: int) -> None:
        await self._execute(
            "UPDATE farmer SET points_pplns = 0, share_pplns = 0 WHERE launcher_id NOT IN ("
            "SELECT DISTINCT launcher_id FROM partial WHERE timestamp >= %s"
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
