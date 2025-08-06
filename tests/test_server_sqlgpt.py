
import os
import pytest
import pytest_asyncio
import logging
from fastmcp import Client

from mcp_kinetica.server import mcp

LOG = logging.getLogger(__name__)

@pytest_asyncio.fixture
async def client():
    async with Client(mcp) as mcp_client:
        LOG.info(f"Connected: {mcp_client.is_connected()}")
        await mcp_client.ping()
        yield mcp_client


@pytest.mark.asyncio
async def test_generate_sql(client: Client):
    os.environ["KINETICA_CONTEXT_NAME"] = "user_cjuliano.fsq_ctx"
    result = await client.call_tool("generate_sql", {
        "question": f"How many starbucks locations are there?"
    })

    sql_query = result.structured_content['result']
    LOG.info(f"Generate SQL result: %s", sql_query)

    assert isinstance(sql_query, str)
