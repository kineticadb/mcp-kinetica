
import logging
import os

from gpudb import GPUdb

LOG = logging.getLogger(__name__)

def test_gpudb_connection() -> None:
    KINETCA_URL= os.getenv('KINETICA_URL')

    opts = GPUdb.Options()
    opts.disable_failover = True
    opts.disable_auto_discovery = True
    opts.logging_level = logging.DEBUG
    opts.skip_ssl_cert_verification = True
    opts.username = 'epresley'
    opts.password = 'elvis'

    LOG.info(f"Connecting to {KINETCA_URL}")
    dbc = GPUdb( host = KINETCA_URL, options = opts )

    result = dbc.query_one("SELECT CURRENT_USER()")
    LOG.info(f"Current user: {result[0]}")

    result = dbc.query("select schema_name from ki_catalog.ki_schemas")
    result_list = list(result)
    schemas = [item[0] for item in result_list]
    LOG.info(f"Schemas: {schemas}")


def test_gpudb_headers() -> None:
    KINETICA_HANDSHAKE_KEY = os.getenv('KINETICA_OAUTH_HANDSHAKE_KEY')
    REMOTE_USER =  '@epresley'
    KINETCA_URL= os.getenv('KINETICA_URL')

    opts = GPUdb.Options()
    opts.disable_failover = True
    opts.disable_auto_discovery = True
    opts.logging_level = logging.DEBUG
    opts.skip_ssl_cert_verification = True
    opts.add_http_header("KINETICA_HANDSHAKE_KEY", KINETICA_HANDSHAKE_KEY)
    #opts.add_http_header("REMOTE_USER", REMOTE_USER)
    opts.add_http_header("KINETICA_USER", REMOTE_USER)

    LOG.info(f"Connecting to {KINETCA_URL} with headers: {opts.http_headers}")
    dbc = GPUdb( host = KINETCA_URL, options = opts )

    result = dbc.query_one("SELECT CURRENT_USER()")
    LOG.info(f"Current user: {result[0]}")

    result = dbc.query("select schema_name from ki_catalog.ki_schemas")
    result_list = list(result)
    schemas = [item[0] for item in result_list]
    LOG.info(f"Schemas: {schemas}")
