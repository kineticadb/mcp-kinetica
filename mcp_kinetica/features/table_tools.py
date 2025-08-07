##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import importlib
import os

from gpudb import GPUdbTable
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .util import ( create_kinetica_connection,
                          query_sql_sub )

LOG = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-table")

SCHEMA =  os.getenv("KINETICA_SCHEMA", default="*")

@mcp.prompt(name="kinetica-sql-agent")
def kinetica_sql_prompt() -> str:
    """
    System prompt to help Claude generate valid, performant Kinetica SQL queries.
    Loaded from markdown file for easier editing and versioning.
    """

    # Note: this may not work with a fastmcp install, depending on environment.
    #       It will work for fastmcp dev and PyPI-based installs
    return importlib.resources.read_text(__package__, 'kinetica_sql_system_prompt.md')


@mcp.tool()
def list_tables() -> list[str]:
    """List all available tables, views, and schemas in the database."""

    LOG.info(f"list_tables: shcema={SCHEMA}")
    dbc = create_kinetica_connection()
    try:
        response = dbc.show_table(SCHEMA, options={"show_children": "true"})
        return sorted(response.get("table_names", []))
    
    except Exception as e:
        raise ToolError(f"Failed to list tables: {str(e)}")


@mcp.tool()
def describe_table(table_name: str) -> dict[str, str]:
    """Return a dictionary of column name to column type."""

    LOG.info(f"describe_table: {table_name}")
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
    LOG.info(f"query_sql: {sql}")
    dbc = create_kinetica_connection()    
    return query_sql_sub(dbc=dbc, sql=sql, limit=limit)


@mcp.tool()
def get_records(table_name: str, limit: int = 10) -> list[dict]:
    """Fetch raw JSON records from a given table."""
    LOG.info(f"get_records: table={table_name}")
    dbc = create_kinetica_connection()
    return query_sql_sub(dbc=dbc, sql=f"SELECT * FROM {table_name}", limit=limit)


@mcp.tool()
def insert_records(table_name: str, records: list[dict]) -> int:
    """Insert records into a specified table."""
    LOG.info(f"insert_records: table={table_name}")
    dbc = create_kinetica_connection()

    try:
        result_table = GPUdbTable(name=table_name, db=dbc)
        orig_size = result_table.size()
        result_table.insert_records(records)
        new_size = result_table.size() - orig_size
        return new_size

    except Exception as e:
        raise ToolError(f"Insertion failed: {str(e)}")
