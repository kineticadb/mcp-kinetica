##
# Copyright (c) 2025, Kinetica DB Inc.
##

import importlib
import logging

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from gpudb import GPUdb, GPUdbTable

from ..kinetica_util import KineticaUtil

LOG = logging.getLogger(__name__)

class TableToolsProvider:
    
    def __init__(self, mcp_instance: FastMCP, k_util: KineticaUtil) -> None:
        mcp_instance.prompt(self.kinetica_sql_prompt, name="kinetica-sql-agent")
        mcp_instance.tool(self.list_tables)
        mcp_instance.tool(self.get_records)
        mcp_instance.tool(self.insert_records)
        self.k_util = k_util


    def kinetica_sql_prompt(self, ctx: Context) -> str:
        """
        System prompt to help Claude generate valid, performant Kinetica SQL queries.
        Loaded from markdown file for easier editing and versioning.
        """
        # Note: this may not work with a fastmcp install, depending on environment.
        #       It will work for fastmcp dev and PyPI-based installs
        return importlib.resources.read_text(__package__, 'kinetica_sql_system_prompt.md')


    def list_tables(self, ctx: Context) -> list[str]:
        """List all available tables, views, and schemas in the database."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        schema_filter = self.k_util.schema
        if schema_filter is None:
            schema_filter = "*"

        LOG.info(f"list_tables: schema={schema_filter}")
        try:
            response = dbc.show_table(table_name=schema_filter, options={"show_children": "true"})
            return sorted(response.get("table_names", []))
        
        except Exception as e:
            raise ToolError(f"Failed to list tables: {str(e)}") from e


    def get_records(self, ctx: Context, table_name: str, limit: int = 10) -> list[dict]:
        """Fetch raw JSON records from a given table."""
        dbc: GPUdb = self.k_util.get_gpudb(ctx)
        LOG.info(f"get_records: table={table_name}")
        return self.k_util.query_sql_sub(dbc=dbc, sql=f"SELECT * FROM {table_name}", limit=limit)


    def insert_records(self, ctx: Context, table_name: str, records: list[dict]) -> int:
        """Insert records into a specified table."""
        LOG.info(f"insert_records: table={table_name}")
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        try:
            result_table = GPUdbTable(name=table_name, db=dbc)
            orig_size = result_table.size()
            result_table.insert_records(records)
            new_size = result_table.size() - orig_size
            return new_size

        except Exception as e:
            raise ToolError(f"Insertion failed: {str(e)}") from e
