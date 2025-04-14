from gpudb import GPUdb, GPUdbTableMonitor
from gpudb import GPUdbTableMonitor as Monitor
import logging
from .client import create_kinetica_client


class MCPTableMonitor(Monitor.Client):
    def __init__(self, db: GPUdb, table_name: str):
        self._logger = logging.getLogger("TableMonitor")
        self._logger.setLevel(logging.INFO)

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

        super().__init__(db, table_name, callback_list=callbacks)

    def on_insert(self, record: dict):
        self._logger.info(f"[INSERT] New record: {record}")

    def on_update(self, count: int):
        self._logger.info(f"[UPDATE] {count} rows updated")

    def on_delete(self, count: int):
        self._logger.info(f"[DELETE] {count} rows deleted")

    def on_error(self, message: str):
        self._logger.error(f"[ERROR] {message}")


# Function to start the monitor
def start_table_monitor(table: str) -> str:
    """
    Starts a table monitor on the specified Kinetica table
    and logs insert, update, and delete events.
    """
    db = create_kinetica_client()
    monitor = MCPTableMonitor(db, table)
    monitor.start_monitor()

    return f"Monitoring started on table '{table}' (insert, update, delete)"
