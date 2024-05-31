try:
    from pydruid.db import connect

    enabled = True
except ImportError:
    enabled = False

import ast
from redash.query_runner import TYPE_STRING, TYPE_INTEGER, TYPE_BOOLEAN
from redash.query_runner import register, BaseSQLQueryRunner
from redash.utils import json_dumps, json_loads

TYPES_MAP = {1: TYPE_STRING, 2: TYPE_INTEGER, 3: TYPE_BOOLEAN}


class Druid(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string", "default": "localhost"},
                "port": {"type": "number", "default": 8082},
                "scheme": {"type": "string", "default": "http"},
                "user": {"type": "string"},
                "password": {"type": "string"},
                "sql_max_rows_limit": {
                    "type": "number",
                    "default": 100000
                },
                "should_enforce_limit": {
                    "type": "boolean",
                    "default": False
                },
                "query_context_params": {
                    "type": "string",
                    "default": "{}"
                },
            },
            "order": ["scheme", "host", "port", "user", "password", "sql_max_rows_limit", "should_enforce_limit", "query_context_params"],
            "required": ["host"],
            "secret": ["password"],
        }

    @classmethod
    def enabled(cls):
        return enabled

    def run_query(self, query, user):
        context_params = ast.literal_eval(self.configuration['query_context_params'])
        connection = connect(
            host=self.configuration["host"],
            port=self.configuration["port"],
            path="/druid/v2/sql/",
            scheme=(self.configuration.get("scheme") or "http"),
            user=(self.configuration.get("user") or None),
            password=(self.configuration.get("password") or None),
            context=context_params,
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            columns = self.fetch_columns(
                [(i[0], TYPES_MAP.get(i[1], None)) for i in cursor.description]
            )
            rows = [
                dict(zip((column["name"] for column in columns), row)) for row in cursor
            ]

            data = {"columns": columns, "rows": rows}
            error = None
            json_data = json_dumps(data)
            print(json_data)
        finally:
            connection.close()

        return json_data, error

    def get_schema(self, get_stats=False):
        query = """
        SELECT TABLE_SCHEMA,
               TABLE_NAME,
               COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        schema = {}
        results = json_loads(results)

        for row in results["rows"]:
            table_name = "{}.{}".format(row["TABLE_SCHEMA"], row["TABLE_NAME"])

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["COLUMN_NAME"])

        return list(schema.values())


register(Druid)
