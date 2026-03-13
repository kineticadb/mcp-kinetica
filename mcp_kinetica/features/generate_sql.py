##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging

from fastmcp import Context, FastMCP
from gpudb import GPUdb

from ..kinetica_util import KineticaUtil

LOG = logging.getLogger(__name__)

class GenerateSqlProvider:
    
    def __init__(self, mcp_instance: FastMCP, k_util: KineticaUtil) -> None:
        mcp_instance.tool(self.list_sql_contexts)
        mcp_instance.tool(self.generate_sql)
        self.k_util = k_util
    

    def list_sql_contexts(self, ctx: Context) -> dict[str, list]:
        """List available SQL contexts and their corresponding tables."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        ctx_filter = "*"
        if self.k_util.schema is not None:
            ctx_filter = self.k_util.schema + ".*"

        LOG.info("list_sql_contexts: filter=%s", ctx_filter)
        sql = f"describe context {ctx_filter}"

        context_dict = {}
        for row in self.k_util.query_sql_sub(dbc=dbc, sql=sql):
            context_name = row['CONTEXT_NAME']
            object_name = row['OBJECT_NAME']

            context_name = context_name.replace('"', '')
            object_name = object_name.replace('"', '')

            if(object_name in {'samples', 'rules'}):
                continue
            
            table_list = context_dict.get(context_name)
            if( table_list is None):
                table_list = []
                context_dict[context_name] = table_list

            description = row['OBJECT_DESCRIPTION']
            table_list.append(dict(table=object_name, description=description))

            LOG.info(f"Found context: {context_name} with object: {object_name}")
        
        return context_dict


    def generate_sql(self, ctx: Context, context_name: str, question: str) -> str:
        """Generate SQL queries using Kinetica's text-to-SQL capabilities."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        LOG.info("Generate SQL (%s): %s", context_name, question)

        # escape single quotes in the question
        question = question.replace("'", "''")

        #dbc = create_kinetica_connection()
        sql = f"generate sql for '{question}' with options (context_names = ('{context_name}'));"
        
        result = self.k_util.query_sql_sub(dbc=dbc, sql=sql)
        return result[0]['Response']
