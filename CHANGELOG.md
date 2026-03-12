# Kinetica MCP Server Changelog

## Version 7.2

### Version 7.2.3.2 - 2026.03.03

-   Support lazy initialization of database connections.
-   Fix handshake_key greater than 255 bytes.
-   Fix handling of quotes in questions.
-   Set default `KINETICA_TTS_MODE` to `LOCAL`.

### Version 7.2.3.1 - 2025.08.28

-   Adding `server_ki` for support of Kinetica context based queries and
    `server_li` for support of local SQL generation.


### Version 7.2.3.0 - 2025.07.01

-   Initial release of the MCP server for Kinetica, exposing tools and resources
    for interacting with tables, SQL-GPT contexts, and real-time monitoring.
