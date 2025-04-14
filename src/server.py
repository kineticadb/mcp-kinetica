import logging
import os
import json
from dotenv import load_dotenv
from fastmcp import FastMCP
import gpudb 
from typing import Dict, List, Union
import re
import json
from .mcp_table_monitor import MCPTableMonitor
from .client import create_kinetica_client

# Load environment variables
load_dotenv()

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-kinetica")


mcp = FastMCP("mcp-kinetica", dependencies=["gpudb", "python-dotenv"])



# A global registry of active table monitors
active_monitors = {}



@mcp.tool()
def list_tables() -> list[str]:
    """List all available tables, views, and schemas in the database."""
    logger.info("Fetching all tables, views, and schemas")
    client = create_kinetica_client()
    response = client.show_table("*", options={"show_children": "true"})
    return response.get("table_names", [])


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