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

CONTEXT_NAME =  os.getenv("KINETICA_CONTEXT_NAME")

mcp = FastMCP("mcp-kinetica-sqlgpt")

@mcp.tool()
def generate_sql(question: str) -> str:
    """Generate SQL queries using Kinetica's text-to-SQL capabilities."""

    LOG.info("Generate SQL: %s", question)
    
    if CONTEXT_NAME is None:
        raise ToolError("Env variable KINETICA_CONTEXT_NAME is not set.")
    LOG.info("Using context: %s", CONTEXT_NAME)

    dbc = create_kinetica_connection()
    sql = f"generate sql for '{question}' with options (context_names = ('{CONTEXT_NAME}'));"
    
    result = query_sql_sub(dbc, sql)
    return result[0]['Response']
