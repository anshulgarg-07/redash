import logging

from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from redash import settings

logger = logging.getLogger(__name__)

try:
    import trino
    from trino.exceptions import DatabaseError

    enabled = True
except ImportError:
    enabled = False

TRINO_TYPES_MAPPING = {
    "boolean": TYPE_BOOLEAN,

    "tinyint": TYPE_INTEGER,
    "smallint": TYPE_INTEGER,
    "integer": TYPE_INTEGER,
    "long": TYPE_INTEGER,
    "bigint": TYPE_INTEGER,

    "float": TYPE_FLOAT,
    "real": TYPE_FLOAT,
    "double": TYPE_FLOAT,

    "decimal": TYPE_INTEGER,

    "varchar": TYPE_STRING,
    "char": TYPE_STRING,
    "string": TYPE_STRING,
    "json": TYPE_STRING,

    "date": TYPE_DATE,
    "timestamp": TYPE_DATETIME,
}


class Trino(BaseQueryRunner):
    noop_query = "SELECT 1"
    should_annotate_query = True

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "protocol": {"type": "string", "default": "http"},
                "host": {"type": "string"},
                "port": {"type": "number"},
                "username": {"type": "string"},
                "password": {"type": "string"},
                "catalog": {"type": "string"},
                "schema": {"type": "string"},
                "source": {
                    "type": "string",
                    "title": "Source to be passed to trino",
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
                    'title': 'Allows passing logged-in users email address as username to trino, Instead of the default username being sent',
                    'default': False
                },
                'sql_character_limit': {
                    'type': 'number',
                    'default': settings.QUERY_CHARACTER_LIMIT
                }
            },
            "order": [
                "protocol",
                "host",
                "port",
                "username",
                "password",
                "catalog",
                "schema",
                "source",
                "information_schema_query",
                "sql_max_rows_limit",
                "should_enforce_limit",
                "user_impersonation",
                "sql_character_limit"
            ],
            "required": ["host", "username"],
            "secret": ["password"]
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "trino"

    def get_schema(self, get_stats=False):
        default_information_schema_query = """
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                """
        information_schema_query = self.configuration.get('information_schema_query',
                                                          default_information_schema_query)
        results, error = self.run_query(information_schema_query, None)

        if error is not None:
            raise Exception(f"Failed getting schema. with error {error}")

        results = json_loads(results)
        schema = {}
        for row in results["rows"]:
            table_name = f'{row["table_schema"]}.{row["table_name"]}'

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["column_name"])

        return list(schema.values())

    def run_query(self, query, user):
        should_impersonate_user = self.configuration.get('user_impersonation', False)
        query_character_limit = self.configuration.get('sql_character_limit', settings.QUERY_CHARACTER_LIMIT)
        if settings.FEATURE_ENFORCE_QUERY_CHARACTER_LIMIT and len(query) >= query_character_limit:
            json_data = None
            error = "Query text length ({}) exceeds the maximum length ({})".format(len(query),
                                                                                    query_character_limit)
            return json_data, error

        if not should_impersonate_user or user is None:
            username = self.configuration.get('username', 'redash')
        else:
            username = user.email
        if self.configuration.get("password"):
            auth = trino.auth.BasicAuthentication(
                username=username,
                password=self.configuration.get("password")
            )
        else:
            auth = trino.constants.DEFAULT_AUTH
        connection = trino.dbapi.connect(
            http_scheme=self.configuration.get("protocol", "http"),
            host=self.configuration.get("host", ""),
            port=self.configuration.get("port", 8080),
            catalog=self.configuration.get("catalog", "hive"),
            schema=self.configuration.get("schema", "default"),
            user=username,
            auth=auth,
            source=self.configuration.get("source", "pyhive")
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            results = cursor.fetchall()
            description = cursor.description
            columns = self.fetch_columns([
                (c[0], TRINO_TYPES_MAPPING.get(c[1], None)) for c in description
            ])
            rows = [
                dict(zip([c["name"] for c in columns], r))
                for r in results
            ]

            query_result_bytes = self.get_total_size(rows)
            logger.info('Query result size {0}'.format(query_result_bytes))
            if query_result_bytes > settings.QUERY_RESULT_MAX_BYTES_LIMIT:
                json_data = None
                error = "Query result too large. Data size {1} > {0} bytes".format(settings.QUERY_RESULT_MAX_BYTES_LIMIT, query_result_bytes)
                return json_data, error
            else:
                data = {'columns': columns, 'rows': rows}
                json_data = json_dumps(data)
                error = None
        except DatabaseError as db:
            json_data = None
            default_message = "Unspecified DatabaseError: {0}".format(str(db))
            if isinstance(db.args[0], dict):
                message = db.args[0].get("failureInfo", {"message", None}).get("message")
            else:
                message = None
            error = default_message if message is None else message
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            cursor.cancel()
            raise

        return json_data, error


register(Trino)
