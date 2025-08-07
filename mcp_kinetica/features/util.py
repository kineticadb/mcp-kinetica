##
# Copyright (c) 2025, Kinetica DB Inc.
##

from gpudb import GPUdb
import logging
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)

def create_kinetica_connection() -> GPUdb:
    """Create and return a GPUdb client instance using env variables."""
    return GPUdb.get_connection(logging_level=logger.level)


def query_sql_sub(dbc: GPUdb, sql: str, limit: int = 10) -> list[dict]:
    response = dbc.execute_sql_and_decode(statement=sql, limit=limit, 
                                                get_column_major=False)
    status_info = response.status_info
    if(status_info['status'] != 'OK'):
        raise ToolError(f"SQL execution failed: {status_info.get('message', 'Unknown error')}")

    records = [ rec.as_dict() for rec in response.records]
    return records
