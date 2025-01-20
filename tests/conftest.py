import pytest
from unittest.mock import AsyncMock


class WalletRpcClient(object):
    async def create_signed_transactions(self):
        pass


@pytest.fixture(scope="module")
def wallet_rpc_client():
    async_mock = AsyncMock(WalletRpcClient)
    return async_mock
