import json
import logging
import os

import pytest
import pytest_asyncio
from fastmcp import Client
from fastmcp.client.auth import OAuth

LOG = logging.getLogger(__name__)

# Edit this to point to your MCP instance
#MCP_URL = "https://172.31.33.26:8447/mcp"
MCP_URL = "http://localhost:8390/mcp"

# Path to SSL certificate file. This is required for https with self-signed certs.
CERT_FILE = "/opt/mcp-kinetica/tests/cert.pem"

@pytest_asyncio.fixture
async def auth_client():
    if os.path.isfile(CERT_FILE):
        os.environ["SSL_CERT_FILE"] = CERT_FILE
    else:
        LOG.warning(f"SSL certificate file not found: {CERT_FILE}")

    oauth = OAuth(mcp_url=MCP_URL, client_name="Kinetica MCP")
    async with Client(MCP_URL, auth=oauth) as mcp_client:
        LOG.info(f"Connected: {mcp_client.is_connected()}")
        await mcp_client.ping()
        LOG.info("Ping successful")
        yield mcp_client


@pytest.mark.asyncio
async def test_query_sql_success(auth_client: Client):
    ''' Verify that a valid SQL query returns expected results. '''

    query_result = await auth_client.call_tool(name="query_sql",
        arguments={ "sql": "SELECT CURRENT_USER() as user" })
    records = query_result.structured_content['result']
    LOG.info(f"Result records: {records}")
    first_rec = records[0]

    assert len(records) == 1
    assert "user" in first_rec


@pytest.mark.asyncio
async def test_get_user_info(auth_client: Client):
    ''' Verify that we can get user info from the protected tool.'''

    result = await auth_client.call_tool("get_user_info")
    records = result.structured_content
    LOG.info(f"Token claims: {json.dumps(records, indent=2)}")
    assert "sub" in records


def test_ssl_cert_file() -> None:
    ''' Verify that we can establish an SSL connection using the specified cert file.'''

    import socket
    import ssl
    from urllib.parse import urlparse

    url = urlparse(MCP_URL)
    host = url.hostname
    port = url.port if url.port else 443

    LOG.info(f"Testing SSL connection to {host}:{port}")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(CERT_FILE)

    with socket.create_connection((host, port)) as sock, context.wrap_socket(sock, server_hostname=host) as ssock:
        LOG.info(f"SSL established. Peer: {ssock.getpeercert()}")
