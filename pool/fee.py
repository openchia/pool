from chia.consensus.cost_calculator import NPCResult
from chia.full_node.bundle_tools import simple_solution_generator
from chia._tests.util.get_name_puzzle_conditions import get_name_puzzle_conditions
from chia.types.spend_bundle import SpendBundle
from chia.util.ints import uint32, uint64


async def get_cost(
    bundle: SpendBundle,
    height: uint32,
    constants
) -> None:
    program = simple_solution_generator(bundle)
    npc_result: NPCResult = get_name_puzzle_conditions(
        program,
        constants.MAX_BLOCK_COST_CLVM,
        mempool_mode=True,
        height=height,
        constants=constants,
    )
    if npc_result is not None and npc_result.error is not None:
        raise RuntimeError(f'rpc result error: {npc_result.error}')

    cost = uint64(0 if npc_result.conds is None else npc_result.conds.cost)

    if cost >= (0.5 * constants.MAX_BLOCK_COST_CLVM):
        raise RuntimeError('SpendBundle too costly')

    return cost
