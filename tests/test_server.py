import pytest
import pytest_asyncio
from fastmcp import Client
from mcp_kinetica.server import mcp
import json

SCHEMA = "ki_home"
TABLE = f"{SCHEMA}.sample"

@pytest_asyncio.fixture
async def client():
    async with Client(mcp) as c:
        yield c

@pytest.mark.asyncio
async def test_list_tables(client):
    result = await client.call_tool("list_tables", {})

    if isinstance(result, list) and hasattr(result[0], "text"):
        tables = json.loads(result[0].text)
    else:
        raise TypeError("Expected a list with a TextContent object containing `.text`")

    assert isinstance(tables, list)
    assert TABLE in tables


@pytest.mark.asyncio
async def test_describe_table(client):
    result = await client.call_tool("describe_table", {"table_name": TABLE})

    if isinstance(result, list) and hasattr(result[0], "text"):
        data = json.loads(result[0].text)
    else:
        raise TypeError("Expected a list with a TextContent object containing `.text`")

    assert isinstance(data, dict)
    assert "table_info" in data
    assert data["table_info"]["table_name"] == TABLE


@pytest.mark.asyncio
async def test_get_records(client):
    """Verify that known sample records exist in the table."""
    result = await client.call_tool("get_records", {"table_name": TABLE})

    if isinstance(result, list) and hasattr(result[0], "text"):
        records = json.loads(result[0].text)
    else:
        raise TypeError(f"Unexpected result format: {result}")

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
async def test_query_sql_success(client):
    """Insert unique rows and verify they appear in SELECT query."""
    unique_records = [
        {"user_id": 5001, "name": "TempUserA", "email": "a@temp.com"},
        {"user_id": 5002, "name": "TempUserB", "email": "b@temp.com"},
    ]

    # Insert the unique records
    insert_result = await client.call_tool("insert_json", {
        "table_name": TABLE,
        "records": unique_records
    })

    if isinstance(insert_result, list) and hasattr(insert_result[0], "text"):
        insert_data = json.loads(insert_result[0].text)
    else:
        raise TypeError(f"Unexpected insert result format: {insert_result}")

    assert "data" in insert_data
    assert insert_data["data"]["count_inserted"] == len(unique_records)

    # Query the table
    query_result = await client.call_tool("query_sql", {
        "sql": f"SELECT * FROM {TABLE}"
    })

    if isinstance(query_result, list) and hasattr(query_result[0], "text"):
        parsed = json.loads(query_result[0].text)
    else:
        raise TypeError(f"Unexpected query result format: {query_result}")

    assert "column_1" in parsed
    assert "column_headers" in parsed

    user_ids = parsed["column_1"]
    assert 5001 in user_ids
    assert 5002 in user_ids

@pytest.mark.asyncio
async def test_query_sql_failure(client):
    """Ensure failed queries return structured error."""
    result = await client.call_tool("query_sql", {
        "sql": "SELECT * FROM nonexistent_table_xyz"
    })

    if isinstance(result, list) and hasattr(result[0], "text"):
        parsed = json.loads(result[0].text)
    else:
        raise TypeError(f"Unexpected result format: {result}")

    assert "error" in parsed

@pytest.mark.asyncio
async def test_insert_json_isolated(client):
    """Verify insert_json handles valid payload and returns count."""
    new_record = [{"user_id": 9999, "name": "Charlie", "email": "charlie@example.com"}]
    
    result = await client.call_tool("insert_json", {
        "table_name": TABLE,
        "records": new_record
    })

    if isinstance(result, list) and hasattr(result[0], "text"):
        parsed = json.loads(result[0].text)
    else:
        raise TypeError(f"Unexpected result format: {result}")

    assert "data" in parsed
    assert "count_inserted" in parsed["data"]
    assert parsed["data"]["count_inserted"] >= 1

@pytest.mark.asyncio
async def test_get_sql_context(client):
    context_name = "kgraph_ctx"
    raw = await client.read_resource(f"sql-context://{context_name}")

    assert isinstance(raw, list) and hasattr(raw[0], "text"), f"Unexpected result format: {raw}"

    context = json.loads(raw[0].text)

    assert isinstance(context, dict)
    assert context.get("context_name") == context_name
    assert "table" in context
    assert "comment" in context
    assert "rules" in context and isinstance(context["rules"], list)
