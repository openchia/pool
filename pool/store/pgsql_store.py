from decimal import Decimal
from typing import Optional, Set, List, Tuple, Dict

import aiopg
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
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
                await cursor.execute("SELECT launcher_id FROM farmer WHERE launcher_id = %s", (farmer_record.launcher_id.hex(),))
                exists = await cursor.fetchone()
                if not exists:
                    await cursor.execute(
                        "INSERT INTO farmer ("
                        "launcher_id, p2_singleton_puzzle_hash, delay_time, delay_puzzle_hash, authentication_public_key, singleton_tip, singleton_tip_state, points, difficulty, payout_instructions, is_pool_member, estimated_size, points_pplns, share_pplns, joined_at) VALUES ("
                        "%s,          %s,                       %s,         %s,                %s,                        %s,            %s,                  0,      %s,         %s,                  %s,             0,              0,            0,           NOW())",
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
                    await cursor.execute(
                        "UPDrTE farmer SET p2_singleton_puzzle_hash = %s, delay_time = %s, delay_puzzle_hash = %s, authentication_public_key = %s, singleton_tip = %s, singleton_tip_state = %s, difficulty = %s, payout_instructions = %s, is_pool_member = %s WHERE launcher_id = %s",
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
        launcher_id: bytes32,
        singleton_tip: CoinSpend,
        singleton_tip_state: PoolState,
        is_pool_member: bool,
    ):
        if is_pool_member:
            entry = (bytes(singleton_tip), bytes(singleton_tip_state), True, launcher_id.hex())
        else:
            entry = (bytes(singleton_tip), bytes(singleton_tip_state), False, launcher_id.hex())
        await self._execute(
            "UPDATE farmer SET singleton_tip=%s, singleton_tip_state=%s, is_pool_member=%s WHERE launcher_id=%s",
            entry,
        )

    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        rows = await self._execute("SELECT p2_singleton_puzzle_hash from farmer")

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
        rows = await self._execute("SELECT points, payout_instructions from farmer")
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

    async def add_partial(self, launcher_id: bytes32, timestamp: uint64, difficulty: uint64, error: Optional[str] = None):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO partial (launcher_id, timestamp, difficulty, error) VALUES(%s, %s, %s, %s)",
                    (launcher_id.hex(), timestamp, difficulty, error),
                )

            if error is None:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE farmer SET points = points + %s WHERE launcher_id=%s",
                        (difficulty, launcher_id.hex()),
                    )

    async def get_recent_partials(self, start_time) -> List[Tuple[str, int, int]]:
        rows = await self._execute(
            "SELECT launcher_id, timestamp, difficulty FROM partial "
            "WHERE timestamp >= %s AND error IS NULL ORDER BY timestamp ASC",
            (start_time, ),
        )
        ret: List[Tuple[str, int, int]] = [
            (launcher_id, timestamp, difficulty)
            for launcher_id, timestamp, difficulty in rows
        ]
        return ret

    async def add_block(self, coin_record: CoinRecord, singleton_coin_record: CoinRecord, farmer: FarmerRecord) -> None:
        await self._execute(
            "INSERT INTO block ("
            " name, singleton, timestamp, confirmed_block_index, puzzle_hash, amount, farmed_by_id"
            ") VALUES ("
            " %s,   %s,        %s,        %s,                    %s,          %s,     %s"
            ")",
            (
                coin_record.name.hex(),
                singleton_coin_record.name.hex(),
                int(coin_record.timestamp),
                int(coin_record.confirmed_block_index),
                coin_record.coin.puzzle_hash.hex(),
                int(coin_record.coin.amount),
                farmer.launcher_id.hex(),
            )
        )

    async def add_payout(self, coin_records, amount, fee, payment_targets) -> int:
        rv = await self._execute(
            "INSERT INTO payout "
            "(datetime, amount, fee) VALUES "
            "(NOW(),    %s,     %s) "
            "RETURNING id",
            (amount, fee),
        )
        payout_id = rv[0][0]
        for coin_record in coin_records:
            await self._execute(
                "UPDATE block SET payout_id = %s WHERE singleton = %s",
                (payout_id, coin_record.coin.parent_coin_info.hex()),
            )
        for i in payment_targets:
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
                "(payout_id, puzzle_hash, launcher_id, amount, transaction) VALUES "
                "(%%s,       %%s,         %s,          %%s,    NULL)" % (farmer,),
                (payout_id, i["puzzle_hash"].hex(), i["amount"]),
            )
        return payout_id

    async def add_transaction(self, transaction, payment_targets) -> None:
        ids = [i['id'] for i in payment_targets]
        await self._execute(
            "UPDATE payout_address SET transaction = %s WHERE id IN (%s)",
            (transaction.name.hex(), ', '.join(ids)),
        )

    async def pending_payment_targets_exists(self):
        return (await self._execute(
            "SELECT COUNT(*) FROM payout_address WHERE transaction IS NULL",
        ))[0][0] > 0

    async def get_pending_payment_targets(self, limit):
        return [{
            "id": i[0],
            "payout_id": i[1],
            "puzzle_hash": bytes32(bytes.fromhex(i[2])),
            "amount": i[3],
        } for i in await self._execute(
            "SELECT id, payout_id, puzzle_hash, amount FROM payout_address WHERE transaction IS NULL LIMIT %s" % limit,
        )]

    async def confirm_transaction(self, transaction):
        await self._execute(
            "UPDATE payout_address SET confirmed_block_index = %s WHERE transaction = %s",
            (int(transaction.confirmed_at_height), transaction.name.hex()),
        )

    async def get_block_singletons(self):
        return [
            i[0] for i in
            await self._execute("SELECT singleton FROM block")
        ]

    async def set_pool_size(self, size: int) -> None:
        await self._execute(
            "INSERT INTO space (date, size) VALUES (NOW(), %s)",
            (size,)
        )
