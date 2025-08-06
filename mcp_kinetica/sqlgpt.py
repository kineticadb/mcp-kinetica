##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import os
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .server_util import ( create_kinetica_connection,
                          query_sql_sub )

LOG = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-sqlgpt")

@mcp.tool()
def generate_sql(question: str) -> str:
    """Generate SQL queries using Kinetica's text-to-SQL capabilities."""
    LOG.info("Generate SQL: %s", question)

    context_name =  os.getenv("KINETICA_CONTEXT_NAME")
    if context_name is None:
        raise ToolError("Env variable KINETICA_CONTEXT_NAME is not set.")

    dbc = create_kinetica_connection()
    sql = f"generate sql for '{question}' with options (context_names = ('{context_name}'));"
    result = query_sql_sub(dbc, sql)
    return result[0]['Response']
