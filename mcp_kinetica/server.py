import json
from dotenv import load_dotenv
from fastmcp import FastMCP, settings as fastmcp_settings
from fastmcp import prompts
import gpudb 
from typing import Dict, List, Union
import re
from gpudb import GPUdb
from gpudb import GPUdbTableMonitor as Monitor
import logging
import os
from importlib import resources as impresources
from collections import deque

DEFAULT_LOG_LEVEL = "WARNING"


def create_kinetica_client():
    """Create and return a GPUdb client instance using env variables."""
    options = gpudb.GPUdb.Options()
    options.username = os.getenv("KINETICA_USER")
    options.password = os.getenv("KINETICA_PASSWORD")
    options.logging_level = logger.level
    return gpudb.GPUdb(
        host=[os.getenv("KINETICA_URL")],
        options=options
    )




class MCPTableMonitor(Monitor.Client):
    def __init__(self, db: GPUdb, table_name: str):
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

        super().__init__(db, table_name, callback_list=callbacks)

    def on_insert(self, record: dict):
        self.recent_inserts.appendleft(record)
        self._logger.info(f"[INSERT] New record: {record}")

    def on_update(self, count: int):
        self._logger.info(f"[UPDATE] {count} rows updated")

    def on_delete(self, count: int):
        self._logger.info(f"[DELETE] {count} rows deleted")

    def on_error(self, message: str):
        self._logger.error(f"[ERROR] {message}")


# Load environment variables
load_dotenv()

# Text-based log level
LOG_LEVEL = os.getenv("KINETICA_LOGLEVEL", DEFAULT_LOG_LEVEL)

# Set MCP server log level
fastmcp_settings.log_level = LOG_LEVEL

# Initialize MCP client logger
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("mcp-kinetica")


mcp = FastMCP("mcp-kinetica", dependencies=["gpudb", "python-dotenv"])


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


# A global registry of active table monitors
active_monitors = {}



@mcp.tool()
def list_tables() -> list[str]:
    """List all available tables, views, and schemas in the database."""
    logger.info("Fetching all tables, views, and schemas")
    client = create_kinetica_client()
    response = client.show_table("*", options={"show_children": "true"})
    return sorted(response.get("table_names", []))


@mcp.tool()
def describe_table(table_name: str) -> dict:
    """Describe a specific table including type schema and properties."""
    logger.info(f"Describing table: {table_name}")
    client = create_kinetica_client()

    try:
        # Ensure we only fetch info about the table, not the schema's children
        table_info = client.show_table(table_name, options={"show_children": "false"})

        type_ids = table_info.get("type_ids")
        if not type_ids:
            return {
                "table_info": table_info,
                "type_info": {},
                "warning": "No type_ids found — possibly a schema or unsupported table type."
            }

        type_id = type_ids[0]
        type_detail = client.show_types(type_id=type_id, label="")

        return {
            "table_info": table_info,
            "type_info": type_detail
        }
    except Exception as e:
        logger.error(f"Failed to describe table: {str(e)}")
        return {"error": str(e)}

@mcp.tool()
def query_sql(sql: str) -> dict:
    """Run a safe SQL query on the Kinetica database."""
    logger.info(f"Executing SQL: {sql}")
    client = create_kinetica_client()
    try:
        response = client.execute_sql(statement=sql, encoding="json", options={})
        return json.loads(response["json_encoded_response"])
    except Exception as e:
        logger.error(f"SQL execution failed: {str(e)}")
        return {"error": str(e)}



@mcp.tool()
def get_records(table_name: str, limit: int = 100) -> list[dict]:
    """Fetch raw JSON records from a given table."""
    logger.info(f"Getting records from {table_name}")
    client = create_kinetica_client()
    try:
        json_str = client.get_records_json(table_name, limit=limit)
        data = json.loads(json_str)
        return data.get("data", {}).get("records", [])
    except Exception as e:
        logger.error(f"Record fetch failed: {str(e)}")
        return [{"error": str(e)}]

@mcp.tool()
def insert_json(table_name: str, records: list[dict]) -> dict:
    """Insert JSON records into a specified table."""
    logger.info(f"Inserting into table {table_name}")
    client = create_kinetica_client()

    try:
        # Ensure valid JSON string (raises if invalid)
        json_data = json.dumps(records)
        json.loads(json_data)

        # and pass table_name into query params
        combined_options = gpudb.GPUdb.merge_dicts(
            {"table_name": table_name},
            {"truncate_table": "false"},
        )

        response = client.insert_records_from_json(
            json_records=json_data,
            table_name=table_name,
            json_options={"validate": False},
            create_table_options=None,   
            options=combined_options     # <-- ensure table name is not lost
        )

        parsed = json.loads(response)
        logger.info(f"Insert response: {parsed}")
        return parsed

    except Exception as e:
        logger.error(f"Insertion failed: {str(e)}")
        return {"error": str(e)}

@mcp.tool()
def start_table_monitor(table: str) -> str:
    """
    Starts a table monitor on the given Kinetica table and logs insert/update/delete events.
    """
    if table in active_monitors:
        return f"Monitor already running for table '{table}'"

    db = create_kinetica_client()

    monitor = MCPTableMonitor(db, table)
    monitor.start_monitor()

    active_monitors[table] = monitor
    return f"Monitoring started on table '{table}'"

@mcp.resource("table-monitor://{table}")
def get_recent_inserts(table: str) -> List[dict]:
    """
    Returns the most recent inserts from a monitored table.
    This resource is generic and does not assume a specific schema or use case.
    """
    monitor = active_monitors.get(table)
    if monitor is None:
        return [{"error": f"No monitor found for table '{table}'."}]

    return list(monitor.recent_inserts)

@mcp.resource("sql-context://{context_name}")
def get_sql_context(context_name: str) -> Dict[str, Union[str, List[str], Dict[str, str]]]:
    """
    Returns a structured, AI-readable summary of a Kinetica SQL-GPT context.
    Extracts the table, comment, rules, and comments block (if any) from the context definition.
    """
    db = create_kinetica_client()
    try:
        sql = f'SHOW CONTEXT "{context_name}"'
        result = db.execute_sql(sql, encoding='json')
        raw_json = result.get("json_encoded_response", "{}")
        records = json.loads(raw_json)
        context_sql = records.get("column_1", [""])[0]

        parsed = {
            "context_name": context_name,
            "table": None,
            "comment": None,
            "rules": [],
            "column_comments": {}
        }

        # TABLE = "schema"."table"
        table_match = re.search(r'TABLE\s*=\s*"([^"]+)"\."([^"]+)"', context_sql)
        if table_match:
            parsed["table"] = f'{table_match.group(1)}.{table_match.group(2)}'

        # COMMENT = '...'
        comment_match = re.search(r'COMMENT\s*=\s*\'((?:[^\']|\\\')*)\'', context_sql, re.DOTALL)
        if comment_match:
            parsed["comment"] = comment_match.group(1).strip()

        # RULES = ('...', '...')
        rules_match = re.search(r'RULES\s*=\s*\((.*?)\)', context_sql, re.DOTALL)
        if rules_match:
            raw_rules = rules_match.group(1)
            rules = re.findall(r"'(.*?)'", raw_rules, re.DOTALL)
            parsed["rules"] = [r.strip() for r in rules]

        # COMMENTS = ('col' = 'desc', ...)
        comments_match = re.search(r'COMMENTS\s*=\s*\((.*?)\)\s*\)', context_sql, re.DOTALL)
        if comments_match:
            comments_block = comments_match.group(1)
            comment_pairs = re.findall(r"'([^']+)'\s*=\s*'([^']+)'", comments_block)
            parsed["column_comments"] = {k: v.strip() for k, v in comment_pairs}

        return parsed
    except Exception as e:
        return {"error": str(e), "context_name": context_name}


def main():
    mcp.run()

if __name__ == "__main__":
    main()
