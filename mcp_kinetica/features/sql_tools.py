##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from gpudb import GPUdb

from ..kinetica_util import KineticaUtil

LOG = logging.getLogger(__name__)

class SqlToolsProvider:
    
    def __init__(self, mcp_instance: FastMCP, k_util: KineticaUtil) -> None:
        mcp_instance.tool(self.query_sql)
        mcp_instance.tool(self.describe_table)
        self.k_util = k_util


    def query_sql(self, ctx: Context, sql: str, limit: int = 10) -> list[dict]:
        """Run a safe SQL query on the Kinetica database."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        LOG.info(f"query_sql: {sql}")
        return self.k_util.query_sql_sub(dbc=dbc, sql=sql, limit=limit)


    def describe_table(self, ctx: Context, table_name: str) -> dict[str, str]:
        """Return a dictionary of column name to column type."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)
        LOG.info(f"describe_table: {table_name}")
        try:
            result_rows = dbc.query(f"describe {table_name}")
            result_dict = {}
            for row in result_rows:
                result_dict[row[1]] = row[3]
            return result_dict
        
        except Exception as e:
            raise ToolError(f"Failed to describe table '{table_name}': {str(e)}") from e
