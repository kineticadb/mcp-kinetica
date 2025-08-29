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

mcp = FastMCP("mcp-kinetica-sql")

@mcp.tool()
def query_sql(sql: str, limit: int = 10) -> list[dict]:
    """Run a safe SQL query on the Kinetica database."""
    LOG.info(f"query_sql: {sql}")
    return query_sql_sub(dbc=DBC, sql=sql, limit=limit)


@mcp.tool()
def describe_table(table_name: str) -> dict[str, str]:
    """Return a dictionary of column name to column type."""

    LOG.info(f"describe_table: {table_name}")

    try:
        result_rows = DBC.query(f"describe {table_name}")
        result_dict = {}
        for row in result_rows:
            result_dict[row[1]] = row[3]
        return result_dict
    
    except Exception as e:
        raise ToolError(f"Failed to describe table '{table_name}': {str(e)}")
