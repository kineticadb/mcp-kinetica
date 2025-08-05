from dotenv import load_dotenv
from typing import Union
import logging
import os
from importlib import resources as impresources
from collections import deque

from fastmcp import FastMCP
import fastmcp.settings as fastmcp_settings
from fastmcp.exceptions import ToolError

from gpudb import ( 
    GPUdb,
    GPUdbTableMonitor as Monitor,
    GPUdbTable
)

# Load environment variables
load_dotenv()

DEFAULT_LOG_LEVEL = "WARNING"

# Text-based log level
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", DEFAULT_LOG_LEVEL)

# Set MCP server log level
fastmcp_settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("mcp-kinetica")

mcp = FastMCP("mcp-kinetica", dependencies=["gpudb", "python-dotenv"])

# A global registry of active table monitors
active_monitors = {}


@mcp.prompt(name="kinetica-sql-agent")
def kinetica_sql_prompt() -> str:
    """
    System prompt to help Claude generate valid, performant Kinetica SQL queries.
    Loaded from markdown file for easier editing and versioning.
    """

    # Note: this may not work with a fastmcp install, depending on environment.
    #       It will work for fastmcp dev and PyPI-based installs
    with (impresources.files("mcp_kinetica") / 'kinetica_sql_system_prompt.md').open("r") as f:
        return f.read()


def _create_kinetica_connection() -> GPUdb:
    """Create and return a GPUdb client instance using env variables."""
    return GPUdb.get_connection(logging_level=logger.level)


@mcp.tool()
def list_tables(schema = "*") -> list[str]:
    """List all available tables, views, and schemas in the database."""
    logger.info("Fetching all tables, views, and schemas")
    dbc = _create_kinetica_connection()

    try:
        response = dbc.show_table(schema, options={"show_children": "true"})
        return sorted(response.get("table_names", []))
    
    except Exception as e:
        raise ToolError(f"Failed to list tables: {str(e)}")


@mcp.tool()
def describe_table(table_name: str) -> dict[str, str]:
    """Return a dictionary of column name to column type."""
    logger.info(f"Describing table: {table_name}")
    dbc = _create_kinetica_connection()

    try:
        result_rows = dbc.query(f"describe {table_name}")
        result_dict = {}
        for row in result_rows:
            result_dict[row[1]] = row[3]
        return result_dict
    
    except Exception as e:
        raise ToolError(f"Failed to describe table '{table_name}': {str(e)}")


def _query_sql_sub(dbc: GPUdb, sql: str, limit: int = 10) -> list[dict]:
    response = dbc.execute_sql_and_decode(statement=sql, limit=limit, 
                                                get_column_major=False)
    status_info = response.status_info
    if(status_info['status'] != 'OK'):
        raise ToolError(f"SQL execution failed: {status_info.get('message', 'Unknown error')}")

    records = [ rec.as_dict() for rec in response.records]
    return records


@mcp.tool()
def query_sql(sql: str, limit: int = 10) -> list[dict]:
    """Run a safe SQL query on the Kinetica database."""
    logger.info(f"Executing SQL: {sql}")
    dbc = _create_kinetica_connection()    
    return _query_sql_sub(dbc=dbc, sql=sql, limit=limit)


@mcp.tool()
def get_records(table_name: str, limit: int = 10) -> list[dict]:
    """Fetch raw JSON records from a given table."""
    logger.info(f"Getting records from {table_name}")
    dbc = _create_kinetica_connection()
    return _query_sql_sub(dbc=dbc, sql=f"SELECT * FROM {table_name}", limit=limit)


@mcp.tool()
def insert_records(table_name: str, records: list[dict]) -> int:
    """Insert records into a specified table."""
    logger.info(f"Inserting into table {table_name}")
    dbc = _create_kinetica_connection()

    try:
        result_table = GPUdbTable(name=table_name, db=dbc)
        orig_size = result_table.size()
        result_table.insert_records(records)
        new_size = result_table.size() - orig_size
        return new_size

    except Exception as e:
        raise ToolError(f"Insertion failed: {str(e)}")


class _MCPTableMonitor(Monitor.Client):
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


@mcp.tool()
def start_table_monitor(table: str) -> str:
    """
    Starts a table monitor on the given Kinetica table and logs insert/update/delete events.
    """
    if table in active_monitors:
        return f"Monitor already running for table '{table}'"

    dbc = _create_kinetica_connection()

    monitor = _MCPTableMonitor(dbc, table)
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


def _unquote(text: str) -> str:
    """Remove surrounding single quotes and unescape internal quotes."""
    result = text.strip()
    result = result.strip("'")
    result = result.replace("''", "'")
    return result


def _parse_list(text: str) -> list[str]:
    """Parse rules from a RULES string, handling escaped single quotes."""
    rules_list = []

    for rule in text.split(','):
        rule = _unquote(rule)
        rules_list.append(rule)

    return rules_list


def _parse_dict(text: str) -> dict[str, str]:
    """Parse a dictionary-like string of key=value pairs, handling escaped single quotes."""
    result = {}
    for pair in text.split(','):
        if '=' in pair:
            key, value = pair.split('=', 1)
            key = _unquote(key)
            value = _unquote(value)
            result[key] = value
    return result


@mcp.resource("sql-context://{context_name}")
def get_sql_context(context_name: str) -> dict[str, Union[str, list, dict]]:
    """
    Returns a structured, AI-readable summary of a Kinetica SQL-GPT context.
    Extracts the table, comment, rules, and comments block (if any) from the context definition.
    """

    dbc = _create_kinetica_connection()
    sql = f'DESCRIBE CONTEXT {context_name}'
    records = _query_sql_sub(dbc=dbc, sql=sql, limit=100)

    tables_list = []
    samples_dict = []
    rules_list = []

    for row in records:
        object_name = row['OBJECT_NAME']
        object_name = object_name.replace('"', '')

        if(object_name == 'samples'):
            samples_dict = _parse_dict(row['OBJECT_SAMPLES'])

        elif(object_name == 'rules'):
            rules_text = row['OBJECT_RULES']
            rules_list.append(_parse_list(rules_text))

        else:
            # object is a table
            table_rules_list = _parse_list(row['OBJECT_RULES'])
            comments_dict = _parse_dict(row['OBJECT_COMMENTS'])

            tables_list.append({
                'name': object_name,
                'description': row['OBJECT_DESCRIPTION'],
                'rules': table_rules_list,
                'column_comments': comments_dict
            })

    return {
        'context_name': context_name,
        'tables': tables_list,
        'samples': samples_dict,
        'rules': rules_list
    }


def main():
    mcp.run()

if __name__ == "__main__":
    main()
