##
# Copyright (c) 2025, Kinetica DB Inc.
##

from dotenv import load_dotenv
import logging
import os
import argparse
import asyncio

from fastmcp import FastMCP
import fastmcp.settings

from mcp_kinetica.table_monitor import mcp as mcp_table_monitor
from mcp_kinetica.sql_context import mcp as mcp_sql_context
from mcp_kinetica.table_tools import mcp as mcp_table_tools

# Load environment variables
load_dotenv()

DEFAULT_LOG_LEVEL = "WARNING"

# Text-based log level
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", DEFAULT_LOG_LEVEL)

# Set MCP server log level
fastmcp.settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)

mcp: FastMCP = FastMCP("mcp-kinetica", dependencies=["gpudb", "python-dotenv"])

async def setup():
    await mcp.import_server(mcp_sql_context)
    await mcp.import_server(mcp_table_tools)
    await mcp.import_server(mcp_table_monitor)
    
asyncio.run(setup())

def main() -> None:
    parser = argparse.ArgumentParser(description='Kinetica mcp server')
    parser.add_argument('--sqlgpt-mode', help="Force use of Kinetica text-to-sql generation", action='store_true')
    args = parser.parse_args()
    mcp.run()


if __name__ == "__main__":
    main()
