##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .util import ( query_sql_sub,
                    SCHEMA,
                    DBC )

LOG = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-sqlgpt")

@mcp.tool()
def list_sql_contexts() -> dict[str, list]:
    """List available SQL contexts and their corresponding tables."""

    ctx_filter = "*"
    if SCHEMA is not None:
        ctx_filter = SCHEMA + ".*"

    LOG.info("list_sql_contexts: filter=%s", ctx_filter)

    #dbc = create_kinetica_connection()
    sql = f"describe context {ctx_filter}"

    context_dict = {}
    for row in query_sql_sub(DBC, sql):
        context_name = row['CONTEXT_NAME']
        object_name = row['OBJECT_NAME']

        context_name = context_name.replace('"', '')
        object_name = object_name.replace('"', '')

        if(object_name == 'samples' or object_name == 'rules'):
            continue
        
        table_list = context_dict.get(context_name, None)
        if( table_list is None):
            table_list = []
            context_dict[context_name] = table_list

        description = row['OBJECT_DESCRIPTION']
        table_list.append(dict(table=object_name, description=description))

        LOG.info(f"Found context: {context_name} with object: {object_name}")
    
    return context_dict


@mcp.tool()
def generate_sql(context_name: str, question: str) -> str:
    """Generate SQL queries using Kinetica's text-to-SQL capabilities."""

    LOG.info("Generate SQL (%s): %s", context_name, question)

    #dbc = create_kinetica_connection()
    sql = f"generate sql for '{question}' with options (context_names = ('{context_name}'));"
    
    result = query_sql_sub(DBC, sql)
    return result[0]['Response']
