
##
# Copyright (c) 2025, Kinetica DB Inc.
##


from fastmcp import Context, FastMCP
from gpudb import GPUdb

from ..kinetica_util import KineticaUtil


class SqlContextProvider:
    
    def __init__(self, mcp_instance: FastMCP, k_util: KineticaUtil) -> None:
        mcp_instance.resource("sql-context://{context_name}")(self.get_sql_context)
        self.k_util = k_util


    def get_sql_context(self, ctx: Context, context_name: str) -> dict[str, str | list | dict]:
        """
        Returns a structured, AI-readable summary of a Kinetica SQL-GPT context.
        Extracts the table, comment, rules, and comments block (if any) from the context definition.
        """
        dbc: GPUdb = self.k_util.get_gpudb(ctx)

        sql = f'DESCRIBE CONTEXT {context_name}'
        records = self.k_util.query_sql_sub(dbc=dbc, sql=sql, limit=100)

        tables_list = []
        samples_dict = []
        rules_list = []

        for row in records:
            object_name = row['OBJECT_NAME']
            object_name = object_name.replace('"', '')

            if(object_name == 'samples'):
                samples_dict = self._parse_dict(row['OBJECT_SAMPLES'])

            elif(object_name == 'rules'):
                rules_text = row['OBJECT_RULES']
                rules_list.append(self._parse_list(rules_text))

            else:
                # object is a table
                table_rules_list = self._parse_list(row['OBJECT_RULES'])
                comments_dict = self._parse_dict(row['OBJECT_COMMENTS'])

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


    @classmethod
    def _unquote(cls, text: str) -> str:
        """Remove surrounding single quotes and unescape internal quotes."""
        result = text.strip()
        result = result.strip("'")
        result = result.replace("''", "'")
        return result


    @classmethod
    def _parse_list(cls, text: str) -> list[str]:
        """Parse rules from a RULES string, handling escaped single quotes."""
        rules_list = []

        for rule in text.split(','):
            _rule = cls._unquote(rule)
            rules_list.append(_rule)

        return rules_list


    @classmethod
    def _parse_dict(cls, text: str) -> dict[str, str]:
        """Parse a dictionary-like string of key=value pairs, handling escaped single quotes."""
        result = {}
        for pair in text.split(','):
            if '=' in pair:
                key, value = pair.split('=', 1)
                key = cls._unquote(key)
                value = cls._unquote(value)
                result[key] = value
        return result
