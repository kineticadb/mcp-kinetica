
import os
import pytest
import pytest_asyncio
import json
import logging
import pandas as pd
from gpudb import GPUdb, GPUdbTable
from fastmcp import Client
from fastmcp.exceptions import ToolError

from mcp_kinetica.server_li import mcp

SCHEMA = "user_cjuliano"

LOG = logging.getLogger(__name__)

TABLE = f"{SCHEMA}.mcp_test_users"

@pytest_asyncio.fixture
async def client():
    async with Client(mcp) as mcp_client:
        LOG.info(f"Connected: {mcp_client.is_connected()}")
        await mcp_client.ping()
        yield mcp_client


def test_create_test_table():
    expected_users = {
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com")
    }
    df = pd.DataFrame(expected_users, columns=['user_id', 'name', 'email'])

    LOG.info(f"Creating test table {TABLE} with data:\n{df}")
    dbc = GPUdb.get_connection()
    gpudb_table = GPUdbTable.from_df(df, db=dbc, 
                                table_name=TABLE, 
                                column_types = { 'name': 'char16' },
                                clear_table=True)
    type_df = gpudb_table.type_as_df()
    LOG.info(f"Created table {TABLE} with types:\n{type_df}")


@pytest.mark.asyncio
async def test_list_tables(client: Client):
    result = await client.call_tool(name="list_tables")
    tables = result.structured_content['result']
    LOG.info(f"Tables: {tables}")
    assert isinstance(tables, list)


@pytest.mark.asyncio
async def test_describe_table(client: Client):
    result = await client.call_tool(name="describe_table", arguments={"table_name": TABLE})
    table_columns = result.structured_content
    LOG.info(f"Table info: {table_columns}")
    assert isinstance(table_columns, dict)


@pytest.mark.asyncio
async def test_get_records(client: Client):
    """Verify that known sample records exist in the table."""
    result = await client.call_tool("get_records", {"table_name": TABLE})
    records = result.structured_content['result']

    # Check that at least 2 records exist
    assert isinstance(records, list)
    assert len(records) >= 2

    # Assert presence of sample records
    expected_users = {
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com")
    }

    actual_users = {
        (rec["user_id"], rec["name"], rec["email"])
        for rec in records if "user_id" in rec
    }

    for user in expected_users:
        assert user in actual_users


@pytest.mark.asyncio
async def test_insert_records(client: Client):
    """Insert unique rows """
    unique_records = [
        {"user_id": 5001, "name": "TempUserA", "email": "a@temp.com"},
        {"user_id": 5002, "name": "TempUserB", "email": "b@temp.com"},
    ]

    # Insert the unique records
    result = await client.call_tool("insert_records", {
        "table_name": TABLE,
        "records": unique_records
    })

    LOG.info(f"Insert result: {result.structured_content}")
    result_count = result.structured_content['result']
    assert result_count == len(unique_records)


@pytest.mark.asyncio
async def test_query_sql_success(client: Client):
    """Verify that a valid SQL query returns expected results."""
    # Query the table
    query_result = await client.call_tool("query_sql", {
        "sql": f"SELECT * FROM {TABLE} where user_id <= 2"
    })

    records = query_result.structured_content['result']
    LOG.info(f"Result records: {records}")
    first_rec = records[0]

    assert len(records) == 2
    assert "user_id" in first_rec.keys()


@pytest.mark.asyncio
async def test_query_sql_failure(client: Client):
    """Ensure failed queries return structured error."""
    with pytest.raises(ToolError):
        result = await client.call_tool("query_sql", {
            "sql": "SELECT * FROM nonexistent_table_xyz"
        })
        LOG.info(f"Query error result: {result.structured_content}")


def test_create_context():
    context_sql = f"""
    CREATE or replace CONTEXT {SCHEMA}."mcp_ctx"
    (TABLE = {SCHEMA}."mcp_test_users",
        COMMENT = 'This is a test',
        RULES = ('Test table rule','Test table rule '' with quote'),
        COMMENTS = ('email' = 'email column',
        'name' = 'user name')),
    (SAMPLES = ('How many users are there?' = 'select count(1) from {SCHEMA}.mcp_test_users',
        'What are all the users?' = 'select * from {SCHEMA}.mcp_test_users')),
    (RULES = ('Test context rule.','Test context  rule '' with quote'))
    """

    dbc = GPUdb.get_connection()
    dbc.execute(context_sql)


@pytest.mark.asyncio
async def test_get_sql_context(client: Client):
    context_name = f"{SCHEMA}.mcp_ctx"
    result = await client.read_resource(f"sql-context://{context_name}")
    context = json.loads(result[0].text)

    ctx_formatted = json.dumps(context, indent=4)
    LOG.info(f"SQL context result: {ctx_formatted}")

    assert isinstance(context, dict)
    assert context.get("context_name") == context_name
    assert "tables" in context
    assert "samples" in context
    assert "rules" in context and isinstance(context["rules"], list)


@pytest.mark.asyncio
async def test_get_prompt(client: Client):
    result = await client.get_prompt("kinetica-sql-agent")
    message = result.messages[0]
    prompt_text = message.content.text
    LOG.info(f"Prompt content: {prompt_text[0:200]}")
