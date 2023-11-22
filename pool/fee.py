from chia.full_node.bundle_tools import simple_solution_generator
from chia.full_node.mempool_check_conditions import get_name_puzzle_conditions
from chia.types.spend_bundle import SpendBundle
from chia.util.ints import uint32


async def get_cost(bundle: SpendBundle, height: uint32, constants) -> None:
    """
    Checks that the cost of the transaction does not exceed blockchain limits. As of version 1.1.2, the mempool limits
    transactions to 50% of the block limit, or 0.5 * 11000000000 = 5.5 billion cost.
    """
    program = simple_solution_generator(bundle)
    npc_result = get_name_puzzle_conditions(
        program,
        constants.MAX_BLOCK_COST_CLVM,
        mempool_mode=True,
        height=height,
        constants=constants,
    )
    if npc_result is not None and npc_result.error is not None:
        raise RuntimeError(f'rpc result error: {npc_result.error}')

    cost = npc_result.cost

    if cost >= (0.5 * constants.MAX_BLOCK_COST_CLVM):
        raise RuntimeError('SpendBundle too costly')
    return cost
