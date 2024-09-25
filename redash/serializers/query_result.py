import io
import csv
import xlsxwriter
import uuid
import logging
from funcy import rpartial, project
from dateutil.parser import isoparse as parse_date
from redash.query_runner import TYPE_BOOLEAN, TYPE_DATE, TYPE_DATETIME
from redash.authentication.org_resolving import current_org
from redash.permissions import can_override_download_limit
from redash import settings
from datetime import datetime, timedelta, timezone
from redash.tasks.audit_downloads import enqueue_download_audit
from redash.settings import ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING

IST_OFFSET = timedelta(hours=5, minutes=30)

def _convert_format(fmt):
    return (
        fmt.replace("DD", "%d")
        .replace("MM", "%m")
        .replace("YYYY", "%Y")
        .replace("YY", "%y")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
        .replace("SSS", "%f")
    )


def _convert_bool(value):
    if value is True:
        return "true"
    elif value is False:
        return "false"

    return value


def _convert_datetime(value, fmt):
    if not value:
        return value

    try:
        parsed = parse_date(value)
        ret = parsed.strftime(fmt)
    except Exception:
        return value

    return ret


def _get_column_lists(columns):
    date_format = _convert_format(current_org.get_setting("date_format"))
    datetime_format = _convert_format(
        "{} {}".format(
            current_org.get_setting("date_format"),
            current_org.get_setting("time_format"),
        )
    )

    special_types = {
        TYPE_BOOLEAN: _convert_bool,
        TYPE_DATE: rpartial(_convert_datetime, date_format),
        TYPE_DATETIME: rpartial(_convert_datetime, datetime_format),
    }

    fieldnames = []
    special_columns = dict()

    for col in columns:
        fieldnames.append(col["name"])

        for col_type in special_types.keys():
            if col["type"] == col_type:
                special_columns[col["name"]] = special_types[col_type]

    return fieldnames, special_columns


def serialize_query_result(query_result, is_api_user):
    if is_api_user:
        publicly_needed_keys = ["data", "retrieved_at"]
        return project(query_result.to_dict(), publicly_needed_keys)
    else:
        return query_result.to_dict()


def serialize_query_result_to_dsv(query_result, delimiter, current_user, format, query):
    s = io.StringIO()

    query_data = query_result.data

    fieldnames, special_columns = _get_column_lists(query_data["columns"] or [])

    writer = csv.DictWriter(s, extrasaction="ignore", fieldnames=fieldnames, delimiter=delimiter)
    writer.writeheader()
    
    download_limit = len(query_data['rows']) if can_override_download_limit() else settings.QUERY_RESULT_DATA_DOWNLOAD_ROW_LIMIT
    download_data = query_data["rows"][:download_limit]
    current_ist_time = datetime.now(timezone.utc) + IST_OFFSET

    if ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING:
        enqueue_download_audit(push_id=uuid.uuid4(), data=query_data, user=current_user.email, query=query, time=current_ist_time, format=format, limit=len(download_data))
    
    for row in download_data:
        for col_name, converter in special_columns.items():
            if col_name in row:
                row[col_name] = converter(row[col_name])

        writer.writerow(row)

    return s.getvalue()

def serialize_query_result_to_xlsx(query_result, current_user, format, query):
    output = io.BytesIO()

    query_data = query_result.data
    book = xlsxwriter.Workbook(output, {"constant_memory": True})
    sheet = book.add_worksheet("result")

    column_names = []
    for c, col in enumerate(query_data["columns"]):
        sheet.write(0, c, col["name"])
        column_names.append(col["name"])

    download_limit = len(query_data['rows']) if can_override_download_limit() else settings.QUERY_RESULT_DATA_DOWNLOAD_ROW_LIMIT
    download_data = query_data["rows"][:download_limit]
    current_ist_time = datetime.now(timezone.utc) + IST_OFFSET

    if ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING:
        enqueue_download_audit(push_id=uuid.uuid4(), data=query_data, user=current_user.email, query=query, time=current_ist_time, format=format, limit=len(download_data))

    for r, row in enumerate(download_data):
        for c, name in enumerate(column_names):
            v = row.get(name)
            if isinstance(v, (dict, list)):
                v = str(v)
            sheet.write(r + 1, c, v)

    book.close()

    return output.getvalue()
