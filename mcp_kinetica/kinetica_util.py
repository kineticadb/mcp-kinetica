##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
import os

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.dependencies import get_access_token
from gpudb import GPUdb

LOG = logging.getLogger(__name__)

class KineticaUtil:
    
    def __init__(self, mcp_instance: FastMCP):
        self.schema =  os.getenv("KINETICA_SCHEMA")
        self._gpudb_url = os.getenv('KINETICA_URL')
        self._gpudb_options = self._get_gpudb_options(mcp_instance)
        LOG.info(f"KINETICA_URL is: {self._gpudb_url}")


    def get_gpudb(self, ctx: Context) -> GPUdb:
        '''
        Set the GPUdb connection using the authenticated user from the FastMCP context.
        This will set the KINETICA_USER from the authenticated user.
        '''

        if "KINETICA_HANDSHAKE_KEY" in self._gpudb_options.http_headers:
            token = get_access_token()
            if token is None:
                raise ValueError("KINETICA_HANDSHAKE_KEY is provided but access token is not available.")

            username = token.client_id

            # if ctx.fastmcp.auth is None:
            #     raise ValueError("We need to get KINETICA_USER but oauth is not enabled.")

            # starlette_request: Request = ctx.request_context.request
            # auth_user: AuthenticatedUser  = starlette_request.user
            # username = auth_user.username

            self._gpudb_options.add_http_header("KINETICA_USER", username)
            LOG.info(f"Using kinetica handshake key with user: {username}")
        else:
            LOG.info(f"Connecting to kinetica with user: {self._gpudb_options.username}")

        return GPUdb( host = self._gpudb_url, 
                     options = self._gpudb_options )


    @classmethod
    async def get_user_info(cls) -> dict:
        """Returns information about the authenticated user."""
        from fastmcp.server.dependencies import get_access_token
        token = get_access_token()
        return {
            "token": token.token,
            "client_id": token.client_id,
            "scopes": token.scopes,
            "sub": token.claims.get("sub")
        }


    @classmethod
    def query_sql_sub(cls, dbc: GPUdb, sql: str, limit: int = 10) -> list[dict]:
        """ Execute a query and return as a list of dict encoded records."""
        response = dbc.execute_sql_and_decode(statement=sql, limit=limit,
                                                    get_column_major=False)
        status_info = response.status_info
        if(status_info['status'] != 'OK'):
            raise ToolError(f"SQL execution failed: {status_info.get('message', 'Unknown error')}")

        records = [ rec.as_dict() for rec in response.records]
        return records


    @classmethod
    def _get_gpudb_options(cls, mcp_instance: FastMCP) -> GPUdb.Options:
        ''' 
        Get a GPUdb connection using environment variables. 

        You must set the KINETICA_URL environment variable. There are 2 options for authentication: 
            1. Username and password. You must provide KINETICA_USER and KINETICA_PASSWD environment variables.
            2. Handshake key. You must provide KINETICA_HANDSHAKE_KEY environment variable.
        '''
        opts = GPUdb.Options()
        opts.disable_failover = True
        opts.disable_auto_discovery = True
        opts.timeout = 2000
        opts.logging_level = os.getenv("KINETICA_LOGLEVEL", "WARNING")
        opts.skip_ssl_cert_verification = True

        handshake_key = os.getenv('KINETICA_OAUTH_HANDSHAKE_KEY')
        if(handshake_key is not None):
            if mcp_instance.auth is None:
                raise ValueError("KINETICA_HANDSHAKE_KEY was provided but oauth is not enabled.")
            
            LOG.info("Using KINETICA_HANDSHAKE_KEY for authentication.")
            opts.add_http_header("KINETICA_HANDSHAKE_KEY", handshake_key)

            # Add a protected tool to tet user authentication
            mcp_instance.tool(cls.get_user_info)

        else:
            LOG.info("Using KINETICA_USER and KINETICA_PASSWD for authentication.")
            opts.username = os.getenv('KINETICA_USER')
            opts.password = os.getenv('KINETICA_PASSWD')

        return opts



