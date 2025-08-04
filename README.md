<h3 align="center" style="margin:0px">
	<img width="200" src="https://www.kinetica.com/wp-content/uploads/2018/08/kinetica_logo.svg" alt="Kinetica Logo"/>
</h3>
<h5 align="center" style="margin:0px">
	<a href="https://www.kinetica.com/">Website</a>
	|
	<a href="https://docs.kinetica.com/latest/">Docs</a>
	|
	<a href="https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg">Community Slack</a>
</h5>


# Kinetica MCP Server

- [Overview](#overview)
- [Features](#features)
  - [Tools](#tools)
  - [Resources](#resources)
- [Installation](#installation)
  - [MCP](#mcp)
    - [MCP via PIP](#mcp-via-pip)
    - [MCP via UV](#mcp-via-uv)
- [Setup and Configuration](#setup-and-configuration)
  - [Claude Desktop Configuration](#claude-desktop-configuration)
  - [MCP Inspector (For Testing)](#mcp-inspector-for-testing)
- [Support](#support)
- [Contact Us](#contact-us)


## Overview

This project contains the source code for the Kinetica Model Context Protocol
(MCP) server, as well as examples of how to configure and run the server.

The Kinetica MCP server exposes tools and resources for interacting with
Kinetica's database, SQL-GPT contexts, and real-time monitoring.


## Features

### Tools

- `list_tables()`

    List all available tables, views, and schemas in the Kinetica instance.

- `describe_table(table_name: str)`

    Return a dictionary of column name to column type.

- `query_sql(sql: str)`

    Run a read-only SQL query on the database, returns results as JSON.

- `get_records(table_name: str, limit: int = 100)`

    Fetch raw records from a table as a list of dictionaries.

- `insert_records(table_name: str, records: list[dict])`

    Insert a list of records into the specified table.

- `start_table_monitor(table: str)`

    Start a real-time monitor for inserts, updates, and deletes on a table.

### Resources

- `sql-context://{context_name}`

    Return a structured view of a SQL-GPT context, including:

    - `table`: Fully qualified table name
    - `comment`: Context description
    - `rules`: List of defined semantic rules
    - `column_comments`: Optional inline column comment block


## Installation

The Kinetica MCP server requires the following component versions:

- Python 3.10
- Node.js 18

### MCP

The Kinetica MCP server can be installed with one of the following:

- [Python PIP](#mcp-via-pip)
- [UV](#mcp-via-uv)

#### MCP via PIP

```env
pip3 install mcp-kinetica
```

#### MCP via UV

```env
uv add mcp-kinetica
```

## Setup and Configuration 

The MCP server uses environment variables to connect securely to your Kinetica
instance. You can define these in a `.env` file, export them in your shell, or
specify them in the Claude Desktop config.

You can integrate the Kinetica MCP server in two ways:

- [Claude Desktop](#claude-desktop-configuration)
- [Test Configuration](#mcp-inspector-for-testing)

### Claude Desktop Configuration 

1. Open your Claude Desktop configuration file:

    - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
    - **Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

2. Add an `mcp-kinetica` entry to the `mcpServers` block:

    ```json
    {
      "mcpServers": {
        "mcp-kinetica": {
          "command": "uv",
          "args": [
            "run",
            "--with",
            "setuptools",
            "--with",
            "mcp-kinetica",
            "mcp-kinetica"
          ],
          "env": {
            "KINETICA_URL": "<http://your-kinetica-host:9191>",
            "KINETICA_USER": "<your_username>",
            "KINETICA_PASSWD": "<your_password>",
            "KINETICA_LOGLEVEL": "INFO"
          }
        }
      }
    }
    ```

3. Update the environment variable values as needed for your Kinetica instance.

4. Restart Claude Desktop to apply the changes.


### MCP Inspector (For Testing)

1. Clone the GitHub project:

    ```bash
    git clone git@github.com:kineticadb/mcp-kinetica.git
    cd mcp-kinetica
    ```

2. Create a `.env` file in your project root with the following keys:

    ```env
    KINETICA_URL=http://<your-kinetica-host>:9191
    KINETICA_USER=<your_username>
    KINETICA_PASSWD=<your_password>
    ```

3. Update Python environment:

    ```bash
    uv sync
    ```

4. Activate Python environment:

   - Windows:

       ```bash
       .venv\Scripts\activate.bat
       ```

   - Linux:

       ```bash
       source .venv/bin/activate
       ```

5. Use `fastmcp dev` for an interactive testing environment with the MCP Inspector:

    ```bash
    fastmcp dev mcp_kinetica/server.py 
    ```

    To create a local package in editable mode:

    ```bash
    fastmcp dev mcp_kinetica/server.py --with-editable .
    ```

6. Launch MCP Inspector in a browser, pointing at the URL output by the
   `fastmcp` command; for instance `http://127.0.0.1:6274`, given this output:

    ```env
    Starting MCP inspector...
    Proxy server listening on port 6277
    MCP Inspector is up and running at http://127.0.0.1:6274
    ```

**Note:** MCP inspector will default to `uv` as the command to run.  If not
using `uv` for package management, the MCP Inspector parameters can be updated
as follows:
          
- *Command*:  `python3`
- *Arguments*:  `mcp_kinetica/server.py`


## Support

For bugs, please submit an
[issue on Github](https://github.com/kineticadb/mcp-kinetica/issues).

For support, you can post on
[stackoverflow](https://stackoverflow.com/questions/tagged/kinetica) under the
``kinetica`` tag or
[Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg).


## Contact Us

- Ask a question on Slack:
  [Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg)
- Follow on GitHub:
  [Follow @kineticadb](https://github.com/kineticadb)
- Email us:  <support@kinetica.com>
- Visit:  <https://www.kinetica.com/contact/>
