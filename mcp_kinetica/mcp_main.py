##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import logging.config
import os
import warnings

import fastmcp.settings
import uvicorn

from mcp_kinetica.mcp_app import create_app

warnings.filterwarnings("ignore", category=DeprecationWarning) # silence websockets

LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", "WARNING")
fastmcp.settings.log_level = LOG_LEVEL
logging.basicConfig(level=LOG_LEVEL, format='%(levelname)s [%(name)s] %(message)s')
LOG = logging.getLogger(__name__)

MCP_PORT = int(os.getenv("KINETICA_MCP_PORT", "8390"))
mcp = create_app(MCP_PORT)

def main() -> None:
    mcp.run()

if __name__ == "__main__":
    # When running stanadalone launch with bin/start_mcp.sh
    log_dir = os.getenv("LOG_DIR")
    log_config = None
    if log_dir is not None:
        from mcp_kinetica.logging_config import LOGGING_CONFIG
        LOGGING_CONFIG['handlers']['file_handler']['filename'] = os.path.join(log_dir, "mcp_kinetica.log")
        logging.config.dictConfig(LOGGING_CONFIG)
        log_config = LOGGING_CONFIG

    uvicorn.run(mcp.http_app(), 
                port=MCP_PORT, 
                log_level=LOG_LEVEL.lower(), 
                log_config=log_config)
