##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import os

from fastmcp import FastMCP
from fastmcp.server.auth.providers.workos import WorkOSProvider

from mcp_kinetica.kinetica_util import KineticaUtil

LOG = logging.getLogger(__name__)

def create_app(port: int) -> FastMCP:
    handshake_key = os.getenv("KINETICA_OAUTH_HANDSHAKE_KEY")
    auth = None

    if handshake_key is not None:
        # truncate key to 255 becuause that is the max for django
        handshake_key = handshake_key[:255]

        # If we have a handshake key then assume we are using OAUTH2
        oauth_url = os.environ["KINETICA_OAUTH_URL"]
        base_url = os.environ["KINETICA_OAUTH_BASE_URL"]
        auth = WorkOSProvider(
            client_id="kinetica-mcp-id",
            client_secret=handshake_key,
            authkit_domain=oauth_url,
            base_url=base_url,
            required_scopes=["openid"]
        )

    tts_mode = os.getenv("KINETICA_TTS_MODE", "LOCAL").upper()

    mcp = FastMCP(f"mcp-sqlgpt-{tts_mode.lower()}", 
                        mask_error_details=True, # required in 2.12.3 because of a bug
                        auth=auth
                    )
    #dependencies=["gpudb", "python-dotenv"])
    k_util = KineticaUtil(mcp)

    if(tts_mode == "SERVER"):
        LOG.info("KINETICA_TTS_MODE = 'server': SQL will be generated using Kinetica server-side LLM.")
        from mcp_kinetica.features.generate_sql import GenerateSqlProvider
        from mcp_kinetica.features.sql_tools import SqlToolsProvider
        GenerateSqlProvider(mcp, k_util)
        SqlToolsProvider(mcp, k_util)

    elif(tts_mode == "LOCAL"):
        LOG.info("KINETICA_TTS_MODE = 'local': SQL will be generated using the local LLM.")
        from mcp_kinetica.features.sql_context import SqlContextProvider
        from mcp_kinetica.features.sql_tools import SqlToolsProvider
        from mcp_kinetica.features.table_monitor import TableMonitorProvider
        from mcp_kinetica.features.table_tools import TableToolsProvider
        TableMonitorProvider(mcp, k_util)
        SqlContextProvider(mcp, k_util)
        TableToolsProvider(mcp, k_util)
        SqlToolsProvider(mcp, k_util)

    else:
        raise ValueError(f"Invalid KINETICA_TTS_MODE: {tts_mode}. Must be 'server' or 'local'")
    
    return mcp
