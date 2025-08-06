##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import importlib

from gpudb import GPUdbTable
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .server_util import ( create_kinetica_connection,
                          query_sql_sub )

logger = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-table")

@mcp.prompt(name="kinetica-sql-agent")
def kinetica_sql_prompt() -> str:
    """
    System prompt to help Claude generate valid, performant Kinetica SQL queries.
    Loaded from markdown file for easier editing and versioning.
    """

    # Note: this may not work with a fastmcp install, depending on environment.
    #       It will work for fastmcp dev and PyPI-based installs
    with (importlib.resources.files("mcp_kinetica") / 'kinetica_sql_system_prompt.md').open("r") as f:
        return f.read()

@mcp.tool()
def list_tables(schema: str = "*") -> list[str]:
    """List all available tables, views, and schemas in the database."""
    logger.info("Fetching all tables, views, and schemas")
    dbc = create_kinetica_connection()

    try:
        response = dbc.show_table(schema, options={"show_children": "true"})
        return sorted(response.get("table_names", []))
    
    except Exception as e:
        raise ToolError(f"Failed to list tables: {str(e)}")


@mcp.tool()
def describe_table(table_name: str) -> dict[str, str]:
    """Return a dictionary of column name to column type."""
    logger.info(f"Describing table: {table_name}")
    dbc = create_kinetica_connection()

    try:
        result_rows = dbc.query(f"describe {table_name}")
        result_dict = {}
        for row in result_rows:
            result_dict[row[1]] = row[3]
        return result_dict
    
    except Exception as e:
        raise ToolError(f"Failed to describe table '{table_name}': {str(e)}")



@mcp.tool()
def query_sql(sql: str, limit: int = 10) -> list[dict]:
    """Run a safe SQL query on the Kinetica database."""
    logger.info(f"Executing SQL: {sql}")
    dbc = create_kinetica_connection()    
    return query_sql_sub(dbc=dbc, sql=sql, limit=limit)


@mcp.tool()
def get_records(table_name: str, limit: int = 10) -> list[dict]:
    """Fetch raw JSON records from a given table."""
    logger.info(f"Getting records from {table_name}")
    dbc = create_kinetica_connection()
    return query_sql_sub(dbc=dbc, sql=f"SELECT * FROM {table_name}", limit=limit)


@mcp.tool()
def insert_records(table_name: str, records: list[dict]) -> int:
    """Insert records into a specified table."""
    logger.info(f"Inserting into table {table_name}")
    dbc = create_kinetica_connection()

    try:
        result_table = GPUdbTable(name=table_name, db=dbc)
        orig_size = result_table.size()
        result_table.insert_records(records)
        new_size = result_table.size() - orig_size
        return new_size

    except Exception as e:
        raise ToolError(f"Insertion failed: {str(e)}")
