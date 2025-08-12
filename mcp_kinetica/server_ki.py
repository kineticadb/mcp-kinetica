##
# Copyright (c) 2025, Kinetica DB Inc.
##

from dotenv import load_dotenv
import logging
import os
import asyncio
from fastmcp import FastMCP
import fastmcp.settings

# Load environment variables before importing features
load_dotenv()

from mcp_kinetica.features.generate_sql import mcp as mcp_kinetica_sqlgpt
from mcp_kinetica.features.sql_tools import mcp as mcp_sql_tools

LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", "WARNING")
fastmcp.settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)

mcp: FastMCP = FastMCP("mcp-sqlgpt")
#dependencies=["gpudb", "python-dotenv"])

async def setup():
    # add modular features to this server
    await mcp.import_server(mcp_kinetica_sqlgpt)
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
