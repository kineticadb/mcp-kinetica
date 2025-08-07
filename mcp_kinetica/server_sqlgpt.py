##
# Copyright (c) 2025, Kinetica DB Inc.
##

from dotenv import load_dotenv
import logging
import os
import asyncio

from fastmcp import FastMCP
import fastmcp.settings

from mcp_kinetica.features.generate_sql import mcp as mcp_kinetica_sqlgpt

load_dotenv()
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", "WARNING")
fastmcp.settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)

mcp: FastMCP = FastMCP("mcp-sqlgpt")
#dependencies=["gpudb", "python-dotenv"])

async def setup():
    await mcp.import_server(mcp_kinetica_sqlgpt)

asyncio.run(setup())

def main() -> None:
    mcp.run()

if __name__ == "__main__":
    main()
