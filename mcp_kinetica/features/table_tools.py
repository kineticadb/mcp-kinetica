##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import importlib

from gpudb import GPUdbTable
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .util import ( DBC,
                    query_sql_sub,
                     SCHEMA )

LOG = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-table")

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

    schema_filter = SCHEMA
    if schema_filter is None:
        schema_filter = "*"

    LOG.info(f"list_tables: schema={schema_filter}")
    try:
        response = DBC.show_table(table_name=schema_filter, options={"show_children": "true"})
        return sorted(response.get("table_names", []))
    
    except Exception as e:
        raise ToolError(f"Failed to list tables: {str(e)}")


@mcp.tool()
def get_records(table_name: str, limit: int = 10) -> list[dict]:
    """Fetch raw JSON records from a given table."""
    LOG.info(f"get_records: table={table_name}")
    return query_sql_sub(dbc=DBC, sql=f"SELECT * FROM {table_name}", limit=limit)


@mcp.tool()
def insert_records(table_name: str, records: list[dict]) -> int:
    """Insert records into a specified table."""
    LOG.info(f"insert_records: table={table_name}")

    try:
        result_table = GPUdbTable(name=table_name, db=DBC)
        orig_size = result_table.size()
        result_table.insert_records(records)
        new_size = result_table.size() - orig_size
        return new_size

    except Exception as e:
        raise ToolError(f"Insertion failed: {str(e)}")
