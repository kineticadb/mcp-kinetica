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
  - [Tools](#tools)
  - [Resources](#resources)
  - [Environment Variables](#environment-variables)
- [Integrate with Claude Desktop](#integrate-with-claude-desktop)
- [Test with MCP Inspector](#test-with-mcp-inspector)
- [Test with Pytest](#test-with-pytest)
- [Support](#support)
- [Contact Us](#contact-us)
- [References](#references)

## Overview

This project contains the source code for the Kinetica Model Context Protocol
(MCP) server, as well as examples of how to configure and run the server.

The Kinetica MCP server exposes tools and resources for interacting with
Kinetica's database, SQL-GPT contexts, and real-time monitoring.

### Tools

- `list_tables()`

    List all available tables, views, and schemas in the Kinetica instance.
    Results will be filtered by the KINETICA_SCHEMA env variable.

- `describe_table(table_name: str)`

    Return a dictionary of column name to column type.

- `query_sql(sql: str, limit: int = 10)`

    Run a read-only SQL query on the database, returns results as JSON.

- `get_records(table_name: str, limit: int = 10)`

    Fetch raw records from a table as a list of dictionaries.

- `insert_records(table_name: str, records: list[dict])`

    Insert a list of records into the specified table.

- `start_table_monitor(table: str)`

    Start a real-time monitor for inserts, updates, and deletes on a table.

### Resources

- `sql-context://{context_name}`

    Return a structured view of a SQL-GPT context, including:

  - `context_name`: Fully qualified table name.
  - `tables`: Table descriptions containing description, table rules, and column comments.
  - `rules`: List of defined semantic rules.
  - `samples`: One shot training examples.

### Environment Variables

The server should be configured with these environment variables.

- `KINETICA_URL`: The Kinetica API URL (e.g. `http://your-kinetica-host:9191`)
- `KINETICA_USER`: Kientica username
- `KINETICA_PASSWD`: Kinetica password
- `KINETICA_SCHEMA`: Filter tables by schema (optional, default=*)
- `KINETICA_LOGLEVEL`: Server Loglevel (optional, default=warning)

## Integrate with Claude Desktop

In this example we will invoke the `uv run` command to install the `mcp-kinetica` package automatically when
Claude desktop starts. For this to work we will use `uv` to create a virtual environment with `python` >=3.10 that
will be used by the MCP runtime.

If you have not already downloaded Claude desktop you can get it at <https://claude.ai/download>.

> Note: As an alternative you could install the `mcp-kinetica` with pip and avoid using UV but it is recommended
> in the fastmcp documentation.

1. Make sure you have UV installed.

    ```bash
    pip install --upgrade uv 
    ```

2. Create the python virtual environment.

    You must choose a directory `<your_venv_path>` for the python runtime.

    ```bash
    uv venv --python 3.12 <your_venv_path>
    ```

3. Make a note of the `python` and `uv` paths.

    UV and your VENV could be using different python interpreters. Make a note of these paths and save them for
    the claude config file.

    > Note: Windows users should activate with `<your_venv_path>/bin/activate.bat`

    ```bash
    $ source <your_venv_path>/bin/activate
    $ which uv
    <uv_exe_path>
    $ which python
    <python_exe_path>
    ```

4. Open your Claude Desktop configuration file:

    - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
    - **Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

5. Add an `mcp-kinetica` entry to the `mcpServers` block:

    You will need to edit the `<uv_exe_path>`, `<python_exe_path>`, and Kinetica connection info.

    ```json
    {
      "mcpServers": {
        "mcp-kinetica": {
          "command": "<uv_exe_path>",
          "args": [
            "run",
            "--python", "<python_exe_path>",
            "--with", "setuptools",
            "--with", "mcp-kinetica",
            "mcp-kinetica"
          ],
          "env": {
            "KINETICA_URL": "<http://your-kinetica-host:9191>",
            "KINETICA_USER": "<your_username>",
            "KINETICA_PASSWD": "<your_password>",
            "KINETICA_LOGLEVEL": "INFO",
            "KINETICA_SCHEMA": "*"
          }
        }
      }
    }
    ```

6. Restart Claude Desktop to apply the changes.

    In Claude Desktop open *Settings->Connectors* and look for an entry named mcp-kinetica.

## Test with MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) is a web UI used for exploring the features of
an MCP Service and simulating the activities of an LLM model. You will need Node.js >= 18 for the inspector.

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

3. Update Python environment with uv:

    ```bash
    [~/mcp-kinetica]$ pip install uv
    [~/mcp-kinetica]$ uv sync
    ```

4. Activate Python environment:

   - Windows:

       ```bash
       .venv\Scripts\activate.bat
       ```

   - Linux:

       ```bash
       [~/mcp-kinetica]$ source .venv/bin/activate
       ```

5. Use `fastmcp dev` for an interactive testing environment with the MCP Inspector:

    ```bash
    [~/mcp-kinetica]$ fastmcp dev mcp_kinetica/server.py 
    ```

    To create a local package in editable mode:

    ```bash
    [~/mcp-kinetica]$ fastmcp dev mcp_kinetica/server.py --with-editable .
    ```

6. Launch MCP Inspector in a browser, pointing at the URL output by the
   `fastmcp` command; for instance `http://127.0.0.1:6274`, given this output:

    ```env
    Starting MCP inspector...
    Proxy server listening on port 6277
    MCP Inspector is up and running at http://127.0.0.1:6274
    ```

> **Note:** MCP inspector will default to `uv` as the command to run.  If not
> using `uv` for package management, the MCP Inspector parameters can be updated
> as follows:
>
> - *Command*:  `python3`
> - *Arguments*:  `mcp_kinetica/server.py`

## Test with Pytest

This section describes how to run the test suite under `tests/test_server.py`.

> **Note:** The `uv` utility is not required.

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

3. Install the test dependencies:

    ```bash
    [~/mcp-kinetica]$ pip install --group test
    ```

4. Run pytest:

    ```bash
    [~/mcp-kinetica]$ pytest -rA
    [...]
    PASSED tests/test_server.py::test_create_test_table
    PASSED tests/test_server.py::test_list_tables
    PASSED tests/test_server.py::test_describe_table
    PASSED tests/test_server.py::test_get_records
    PASSED tests/test_server.py::test_insert_records
    PASSED tests/test_server.py::test_query_sql_success
    PASSED tests/test_server.py::test_query_sql_failure
    PASSED tests/test_server.py::test_create_context
    PASSED tests/test_server.py::test_get_sql_context
    ```

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

## References

- [Real-Time Geospatial Queries with MCP + Kinetica](https://www.kinetica.com/blog/real-time-geospatial-queries-with-mcp-kinetica/)
- [Access FSQ OS Places](https://docs.foursquare.com/data-products/docs/access-fsq-os-places)
- [UV Introduction](https://docs.astral.sh/uv/)
- [FastMCP Documentation](https://gofastmcp.com/getting-started/welcome)
- [Kinetica Python DEV Guide](https://github.com/kineticadb/examples/tree/master/python_dev_guide)
