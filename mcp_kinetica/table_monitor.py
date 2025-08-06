##
# Copyright (c) 2025, Kinetica DB Inc.
##

import logging
from gpudb import ( 
    GPUdb,
    GPUdbTableMonitor as Monitor
)
from collections import deque

logger = logging.getLogger(__name__)

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
