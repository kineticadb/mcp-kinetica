##
# Copyright (c) 2025, Kinetica DB Inc.
##

from dotenv import load_dotenv
import logging
import os

from fastmcp import FastMCP
import fastmcp.settings

from .table_monitor import add_monitor_tools
from .sql_context import add_sql_context_resource
from .table_tools import add_table_tools

# Load environment variables
load_dotenv()

DEFAULT_LOG_LEVEL = "WARNING"

# Text-based log level
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", DEFAULT_LOG_LEVEL)

# Set MCP server log level
fastmcp.settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("mcp-kinetica")

mcp = FastMCP("mcp-kinetica", dependencies=["gpudb", "python-dotenv"])
add_monitor_tools(mcp)
add_sql_context_resource(mcp)
add_table_tools(mcp)

def main():
    mcp.run()

if __name__ == "__main__":
    main()
