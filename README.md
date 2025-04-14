# Kinetica MCP Server

An MCP server for Kinetica, exposing tools and resources for interacting with Kinetica's database, SQL-GPT contexts, and real-time monitoring.

---

## Features

### Tools

#### `list_tables()`
List all available tables, views, and schemas in the Kinetica instance.

#### `describe_table(table_name: str)`
Show metadata and type schema for a specific table.

#### `query_sql(sql: str)`
Run a read-only SQL query on the database. Returns results as JSON.

#### `get_records(table_name: str, limit: int = 100)`
Fetch raw records from a table as a list of dictionaries.

#### `insert_json(table_name: str, records: list[dict])`
Insert a list of JSON records into the specified table.

#### `start_table_monitor(table: str)`
Start a real-time monitor for inserts, updates, and deletes on a table.

### Resources

#### `sql-context://{context_name}`
Return a structured view of a SQL-GPT context, including:
- `table`: Fully qualified table name
- `comment`: Context description
- `rules`: List of defined semantic rules
- `column_comments`: Optional inline column comment block

---

## Environment Setup

Create a `.env` file with the following:

```env
KINETICA_URL= {url}
KINETICA_USER= {user}
KINETICA_PASSWORD= {pwd}
``` 

## MCP Inspector

Use fastmcp dev for an interactive testing environment with the MCP Inspector.

