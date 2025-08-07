##
# Copyright (c) 2025, Kinetica DB Inc.
##

from dotenv import load_dotenv
import logging
import os
import asyncio
from fastmcp import FastMCP
import fastmcp.settings

load_dotenv()

from mcp_kinetica.features.table_monitor import mcp as mcp_table_monitor
from mcp_kinetica.features.sql_context import mcp as mcp_sql_context
from mcp_kinetica.features.table_tools import mcp as mcp_table_tools
from mcp_kinetica.features.sql_tools import mcp as mcp_sql_tools

LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL",  "WARNING")
fastmcp.settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)

mcp: FastMCP = FastMCP("mcp-kinetica")
#dependencies=["gpudb", "python-dotenv"])

async def setup():
    await mcp.import_server(mcp_sql_context)
    await mcp.import_server(mcp_table_tools)
    await mcp.import_server(mcp_table_monitor)
    await mcp.import_server(mcp_sql_tools)

try:
    # Utilities like `fastmcp dev` already have a running event loop
    # so we can't use `asyncio.run()`
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(setup(), loop=loop)

except RuntimeError:
    # no event loop is running, so we create one
    asyncio.run(setup())


def main() -> None:
    mcp.run()

if __name__ == "__main__":
    main()
