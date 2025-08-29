
##
# Copyright (c) 2025, Kinetica DB Inc.
##

from typing import Union
from fastmcp import FastMCP

from .util import DBC, query_sql_sub

mcp = FastMCP("mcp-kinetica-context")

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

    sql = f'DESCRIBE CONTEXT {context_name}'
    records = query_sql_sub(dbc=DBC, sql=sql, limit=100)

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
