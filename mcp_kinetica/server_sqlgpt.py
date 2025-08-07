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

# Load environment variables
load_dotenv()

DEFAULT_LOG_LEVEL = "WARNING"

# Text-based log level
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", DEFAULT_LOG_LEVEL)

# Set MCP server log level
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
