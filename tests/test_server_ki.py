
import json
import logging
import os

import pytest
import pytest_asyncio
from fastmcp import Client

os.environ["KINETICA_TTS_MODE"] = "server"

LOG = logging.getLogger(__name__)

@pytest_asyncio.fixture
async def client():
    from mcp_kinetica.mcp_main import mcp
    async with Client(mcp) as mcp_client:
        LOG.info(f"Connected: {mcp_client.is_connected()}")
        await mcp_client.ping()
        yield mcp_client

@pytest.mark.asyncio
async def test_list_contexts(client: Client):
    contexts = await client.call_tool("list_sql_contexts")
    ctx_formatted = json.dumps(contexts.structured_content, indent=4)
    LOG.info(f"List SQL contexts result: {ctx_formatted}")


@pytest.mark.asyncio
async def test_generate_sql(client: Client):
    result = await client.call_tool("generate_sql", {
        "question": "How many starbucks locations are there?",
        "context_name": "user_cjuliano.fsq_ctx"
    })

    sql_query = result.structured_content['result']
    LOG.info("Generate SQL result: %s", sql_query)
    assert isinstance(sql_query, str)
