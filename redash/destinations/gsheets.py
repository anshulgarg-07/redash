import json
import logging
import signal

from datetime import timedelta
from gspread.exceptions import APIError, WorksheetNotFound
from redash.destinations import BaseDestination, register
from redash.tasks.destinations import signal_handler
from redash.utils.gsheets import get_gsheet
from redash import settings, utils

class Gsheets(BaseDestination):
    visualization_enabled = True
    alert_enabled = False

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "spreadsheet_id": {
                    "type": "string"
                },
                "spreadsheet_name": {
                    "type": "string"
                },
                "sheet_name": {
                    "type": "string",
                    "default": "Sheet1"
                },
                "row": {
                    "type": "integer",
                    "default": 0
                },
                "column": {
                    "type": "string",
                    "default": "string"
                },
                "last_sync_rows": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"}
                    ]
                },
                "last_sync_columns": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"}
                    ]
                }
            },
            # add ["last_sync_rows", "last_sync_columns"] in required once every destination has value of last_sync_rows and last_sync_columns in options
            "required": ["spreadsheet_id", "sheet_name", "row", "column"]
        }

    @classmethod
    def icon(cls):
        return 'file-spreadsheet'

    def sync_visualization(self, query_result, options, user_email, query_id, allowed_emails):
        signal.signal(signal.SIGINT, signal_handler)
        try:
            sh = get_gsheet(
                user_email=user_email,
                sheet_id=options.get("spreadsheet_id"),
                sheet_name=options.get("sheet_name"),
                allowed_emails=allowed_emails,
                clear_cache=True
            )
            data = [[column['friendly_name'] for column in query_result["columns"]]]
            for result in query_result["rows"]:
                row = []
                for column in query_result["columns"]:
                    val = result[column['friendly_name']]
                    # If data is NULL destination cell shall be empty
                    val = '' if val is None else val
                    row.append(val.encode('utf-8') if isinstance(val, str) else str(val))
                data.append(row)

            error = None
            # We need only else part once every destination has value of last_sync_rows and last_sync_columns in options
            if options.get("last_sync_rows") is None:
                sh.clear()
            else:
                decreased_rows = (options.get("last_sync_rows") - len(query_result['rows']))
                decreased_columns = (options.get("last_sync_columns") - len(query_result['columns']))
                if decreased_rows > 0:
                    data += decreased_rows * [len(query_result["columns"]) * ['']]
                if decreased_columns > 0:
                    for row in data:
                        row.extend(decreased_columns * [''])
            sh.update(
                '{column}{row}'.format(
                    row=options.get("row"),
                    column=options.get("column")
                ),
                data,
                raw=False
            )
            sh.insert_note(
                cell='{column}{row}'.format(
                    row=options.get("row"),
                    column=options.get("column")
                ),
                content='Source: https://{url}/queries/{id}\nUpdated by: {email}\nLast updated at: {sync_time}'.format(
                    url=settings.HOST,
                    id=query_id,
                    email=user_email,
                    sync_time=(utils.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
                )
            )
        except WorksheetNotFound as e:
            logging.warning("Error: {e}".format(e=str(e)))
            error = (u"No worksheet with name: {name} exists. Please make sure you input the correct sheet name"
                     .format(name=e.message))
        except APIError as e:
            logging.warning("Error: {e}".format(e=str(e)))
            error = str(e.message["message"])
        except Exception as e:
            logging.warning("Error: {e}".format(e=str(e)))
            error = str(e)

        return error


register(Gsheets)
