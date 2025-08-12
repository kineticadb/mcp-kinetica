##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
from gpudb import ( 
    GPUdb,
    GPUdbTableMonitor as Monitor
)
from collections import deque
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .util import DBC

logger = logging.getLogger(__name__)

mcp = FastMCP("mcp-kinetica-monitor")

class MCPTableMonitor(Monitor.Client):
    def __init__(self, dbc: GPUdb, table_name: str):
        self._logger = logging.getLogger("TableMonitor")
        self._logger.setLevel(logger.level)
        self.recent_inserts = deque(maxlen=50)  # Stores last 50 inserts

        callbacks = [
            Monitor.Callback(
                Monitor.Callback.Type.INSERT_DECODED,
                self.on_insert,
                self.on_error,
                Monitor.Callback.InsertDecodedOptions(
                    Monitor.Callback.InsertDecodedOptions.DecodeFailureMode.SKIP
                )
            ),
            Monitor.Callback(
                Monitor.Callback.Type.UPDATED,
                self.on_update,
                self.on_error
            ),
            Monitor.Callback(
                Monitor.Callback.Type.DELETED,
                self.on_delete,
                self.on_error
            )
        ]

        super().__init__(dbc, table_name, callback_list=callbacks)

    def on_insert(self, record: dict):
        self.recent_inserts.appendleft(record)
        self._logger.info(f"[INSERT] New record: {record}")

    def on_update(self, count: int):
        self._logger.info(f"[UPDATE] {count} rows updated")

    def on_delete(self, count: int):
        self._logger.info(f"[DELETE] {count} rows deleted")

    def on_error(self, message: str):
        self._logger.error(f"[ERROR] {message}")

 
# A global registry of active table monitors
active_monitors = {}

    
@mcp.tool()
def start_table_monitor(table: str) -> str:
    """
    Starts a table monitor on the given Kinetica table and logs insert/update/delete events.
    """
    if table in active_monitors:
        return f"Monitor already running for table '{table}'"

    monitor = MCPTableMonitor(DBC, table)
    monitor.start_monitor()

    active_monitors[table] = monitor
    return f"Monitoring started on table '{table}'"

@mcp.resource("table-monitor://{table}")
def get_recent_inserts(table: str) -> list[dict]:
    """
    Returns the most recent inserts from a monitored table.
    This resource is generic and does not assume a specific schema or use case.
    """
    monitor = active_monitors.get(table)
    if monitor is None:
        raise ToolError(f"No monitor found for table '{table}'.")

    return list(monitor.recent_inserts)
