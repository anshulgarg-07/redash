import sys
from collections import defaultdict
from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from redash import settings

import logging

logger = logging.getLogger(__name__)


try:
    from pyhive import presto
    from pyhive.exc import DatabaseError

    enabled = True

except ImportError:
    enabled = False

PRESTO_TYPES_MAPPING = {
    "integer": TYPE_INTEGER,
    "tinyint": TYPE_INTEGER,
    "smallint": TYPE_INTEGER,
    "long": TYPE_INTEGER,
    "bigint": TYPE_INTEGER,
    "float": TYPE_FLOAT,
    "double": TYPE_FLOAT,
    "boolean": TYPE_BOOLEAN,
    "string": TYPE_STRING,
    "varchar": TYPE_STRING,
    "date": TYPE_DATE,
}


class Presto(BaseSQLQueryRunner):
    noop_query = "SHOW TABLES"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "protocol": {"type": "string", "default": "http"},
                "port": {"type": "number"},
                "schema": {"type": "string"},
                "catalog": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"},
                "source": {
                    "type": "string",
                    "title": "Source to be passed to presto",
                    "default": "pyhive"
                },
                "information_schema_query": {
                    "type": "string",
                    "title": "Custom information schema query"
                },
                "sql_max_rows_limit": {
                    "type": "number",
                    "default": 100000
                },
                "should_enforce_limit": {
                    "type": "boolean",
                    "default": False
                },
                'user_impersonation': {
                    'type': 'boolean',
                    'title': 'Allows passing logged-in users email address as username to presto, Instead of the default username being sent',
                    'default': False
                },
                'sql_character_limit': {
                    'type': 'number',
                    'default': settings.QUERY_CHARACTER_LIMIT
                }
            },
            "order": [
                "host",
                "protocol",
                "port",
                "username",
                "password",
                "source",
                "schema",
                "catalog",
                "information_schema_query",
                "sql_max_rows_limit",
                "should_enforce_limit",
                "user_impersonation"
            ],
            "required": ["host"],
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "presto"

    def get_schema(self, get_stats=False):
        schema = {}
        default_information_schema_query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        information_schema_query = self.configuration.get('information_schema_query',
                                                          default_information_schema_query)
        results, error = self.run_query(information_schema_query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results["rows"]:
            table_name = "{}.{}".format(row["table_schema"], row["table_name"])

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["column_name"])

        return list(schema.values())

    def run_query(self, query, user):
        should_impersonate_user = self.configuration.get('user_impersonation', False)
        if not should_impersonate_user or user is None:
            username = self.configuration.get('username', 'redash')
        else:
            username = user.email
        query_character_limit = self.configuration.get('sql_character_limit', settings.QUERY_CHARACTER_LIMIT)
        if settings.FEATURE_ENFORCE_QUERY_CHARACTER_LIMIT and len(query) >= query_character_limit:
            json_data = None
            error = "Query text length ({}) exceeds the maximum length ({})".format(len(query),
                                                                                    query_character_limit)
            return json_data, error
        connection = presto.connect(
            host=self.configuration.get("host", ""),
            port=self.configuration.get("port", 8080),
            protocol=self.configuration.get("protocol", "http"),
            username=username,
            password=(self.configuration.get("password") or None),
            catalog=self.configuration.get("catalog", "hive"),
            schema=self.configuration.get("schema", "default"),
            source=self.configuration.get("source", "pyhive"),
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            column_tuples = [
                (i[0], PRESTO_TYPES_MAPPING.get(i[1], None)) for i in cursor.description
            ]
            columns = self.fetch_columns(column_tuples)
            rows = [
                dict(zip(([column["name"] for column in columns]), r))
                for i, r in enumerate(cursor.fetchall())
            ]
            query_result_bytes = self.get_total_size(rows)
            logger.info('Query result size {0}'.format(query_result_bytes))
            if query_result_bytes > settings.QUERY_RESULT_MAX_BYTES_LIMIT:
                json_data = None
                error = "Query result too large. Data size > {0} bytes".format(settings.QUERY_RESULT_MAX_BYTES_LIMIT)
            else:
                data = {'columns': columns, 'rows': rows}
                json_data = json_dumps(data)
                error = None
        except DatabaseError as db:
            json_data = None
            default_message = "Unspecified DatabaseError: {0}".format(str(db))
            if isinstance(db.args[0], dict):
                message = db.args[0].get("failureInfo", {"message", None}).get(
                    "message"
                )
            else:
                message = None
            error = default_message if message is None else message
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            cursor.cancel()
            raise

        return json_data, error
    
    def get_total_size(self, obj):
        """Recursively finds the total size of an object, including nested objects."""
        size = sys.getsizeof(obj)
        if isinstance(obj, dict):
            size += sum([self.get_total_size(v) for v in obj.values()])
            size += sum([self.get_total_size(k) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_total_size(obj.__dict__)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_total_size(i) for i in obj])
        return size


register(Presto)
