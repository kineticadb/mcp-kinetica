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
  - [Text-to-SQL Modes](#text-to-sql-modes)
  - [Tools](#tools)
  - [Resources](#resources)
- [Configuration](#configuration)
  - [No Authentication](#no-authentication)
  - [OAUTH](#oauth)
- [Integrate with Claude Desktop](#integrate-with-claude-desktop)
- [Testing](#testing)
  - [Launch MCP Inspector](#launch-mcp-inspector)
  - [Run Unit tests with Pytest](#run-unit-tests-with-pytest)
  - [Unit testing OAUTH2](#unit-testing-oauth2)
- [Support](#support)
- [Contact Us](#contact-us)
- [References](#references)

## Overview

This project contains the source code for the Kinetica Model Context Protocol
(MCP) server, as well as examples of how to configure and run the server.

The Kinetica MCP server exposes tools and resources for interacting with
Kinetica's database, SQL-GPT contexts, and real-time monitoring.

## Features

### Text-to-SQL Modes

The MCP server has separate modes depending on how you want the LLM to generate SQL. Each mode contains
a different set tools to facilitate the workflow. This functionality is controlled by the environment
variable KINETICA_TTS_MODE.

- Server-side Inference Mode (KINETICA_TTS_MODE=server)

    The LLM will choose a SQL context and use Kinetica's native text-to-sql capabilities via the `generate_sql()` tool.
    This requires that you have appropriate SQL contexts configured in Kinetica.
    (see [SQL-GPT](https://docs.kinetica.com/7.2/sql-gpt/)). Available tools are:

  - `list_sql_contexts`
  - `generate_sql`
  - `query_sql`
  - `describe_table`

- Local Inference Mode (KINETICA_TTS_MODE=local, default)

    The LLM will retrieve table descriptions generate its own SQL. This mode will result in more tokens being consumed
    from table descriptions but it does not require the use of SQL contexts. Available tools are:
  
  - resource `sql-context://{context_name}`
  - resource `table-monitor://{table}`
  - `query_sql`
  - `describe_table`
  - `start_table_monitor`
  - `kinetica_sql_prompt`
  - `list_tables`
  - `get_records`
  - `insert_records`

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

- `list_sql_contexts()`

    List available SQL contexts and their corresponding tables.

- `generate_sql(context_name: str, question: str)`

    Generate SQL queries using Kinetica's text-to-SQL capabilities.

### Resources

- `sql-context://{context_name}`

    Return a structured view of a SQL-GPT context, including:

  - `context_name`: Fully qualified table name.
  - `tables`: Table descriptions containing description, table rules, and column comments.
  - `rules`: List of defined semantic rules.
  - `samples`: One shot training examples.

## Configuration

The server can optionally be configured to support the OAUTH `authorization-code` workflow. Common variables are:

- `KINETICA_URL`: The Kinetica API URL (e.g. `http://your-kinetica-host:9191`)
- `KINETICA_SCHEMA`: Filter tables by schema (optional, default=`*`)
- `KINETICA_LOGLEVEL`: Server Loglevel (optional, default=`warning`)
- `KINETICA_TTS_MODE`: Indicates the tex-to-sql mode ( `server` or `local` )

> See [conf_tmpl.sh](bin/conf_tmpl.sh) for an example configuration.

### No Authentication

If the MCP will allow access from any user you must specify a username/password that it should use when connecting
to Kinetica.

- `KINETICA_USER`: Kientica username
- `KINETICA_PASSWD`: Kinetica password

### OAUTH

When OAUTH is enabled users will be redirected to the authentication server where they will enter their Kinetica
credentials. The authentication server will then redirected back to the MCP server where they will be given an
authentication token. This token can be perpetually cached to avoid the need for future authentications. Additionally
the MCP server will authenticate with Kinetica using a handshake key that will allow it to impersonate the
authenticated user and their permissions.

To enable this you will need:

1. An Authentication server capable of providing an
[Authorization Grant](https://aaronparecki.com/oauth-2-simplified/#authorization).
2. The kinetica handshake key.

The required parameters to enable set these variables:

- `KINETICA_OAUTH_HANDSHAKE_KEY`: The unencrypted handshake key. This can be found in `httpd/etc/gpudb_httpd.conf`.
- `KINETICA_OAUTH_EXTERNAL_HOST`: The external of the MCP and OAUTH servers.

> Note: It is recommended that you not use `KINETICA_SCHEMA` only when you are using OAUTH2

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

    The app provides a shortcut in *Settings->Developer->Edit Config*.

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
            "KINETICA_SCHEMA": "*",
            "KINETICA_TTS_MODE": "server"
          }
        }
      }
    }
    ```

6. Restart Claude Desktop to apply the changes.

    In Claude Desktop open *Settings->Connectors* and look for an entry named mcp-kinetica.

## Testing

### Launch MCP Inspector

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
    [~/mcp-kinetica]$ fastmcp dev mcp_kinetica/mcp_main.py 
    ```

    To create a local package in editable mode:

    ```bash
    [~/mcp-kinetica]$ fastmcp dev mcp_kinetica/mcp_main.py --with-editable .
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
> - *Arguments*:  `mcp_kinetica/mcp_main.py`

### Run Unit tests with Pytest

This section describes how to run unauthenticated test cases under `tests/`.

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
    [~/mcp-kinetica]$ pip install --group test .
    ```

4. Run pytest:

    ```bash
    [~/mcp-kinetica]$ pytest -rA
    [...]
    PASSED tests/test_server_ki.py::test_list_contexts
    PASSED tests/test_server_ki.py::test_generate_sql
    PASSED tests/test_server_li.py::test_create_test_table
    PASSED tests/test_server_li.py::test_list_tables
    PASSED tests/test_server_li.py::test_describe_table
    PASSED tests/test_server_li.py::test_get_records
    PASSED tests/test_server_li.py::test_insert_records
    PASSED tests/test_server_li.py::test_query_sql_success
    PASSED tests/test_server_li.py::test_query_sql_failure
    PASSED tests/test_server_li.py::test_create_context
    PASSED tests/test_server_li.py::test_get_sql_context
    PASSED tests/test_server_li.py::test_get_prompt
    ```

### Unit testing OAUTH2

The fastmcp library allows for authenticated testing on localhost without the need for SSL. This means we can
configure the MCP and auth servers with unencrypted ports and connections from localhost will work.

1. Configure the auth server to use unencrypted ports. For example:

    ```sh
    KINETICA_URL=https://172.31.72.27:8082/gpudb
    KINETICA_EXTERNAL_HOST=localhost
    KINETICA_MCP_URI=http://localhost:8390
    KINETICA_HANDSHAKE_KEY='NDMxMTQ5MjAyNS0wOS0xOCAxMjozMDo1MS40MzExNTc='
    ```

    Start the auth server.

    ```sh
    [kinetica-auth/bin]$ ./start_auth.sh
    ```

2. Configure the MCP server to use unencrypted ports. For example:

    ```sh
    KINETICA_URL=http://172.31.72.27:9191
    KINETICA_LOGLEVEL=INFO
    KINETICA_OAUTH_URL=http://localhost:8380
    KINETICA_OAUTH_HANDSHAKE_KEY='NDMxMTQ5MjAyNS0wOS0xOCAxMjozMDo1MS40MzExNTc='
    KINETICA_OAUTH_EXTERNAL_HOST=localhost
    KINETICA_OAUTH_BASE_URL=http://localhost:8390
    ```

    Start the MCP server.

    ```sh
    [kinetica-mcp/bin]$ ./start_mcp.sh
    ```

3. Run the authentication test:

    Your browser should open to a login page. After logging in with a Kinetica user you will be redirected back
    to the MCP server.

    ```sh
    [kinetica-mcp]$ pytest tests/test_oauth.py::test_query_sql_success
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
