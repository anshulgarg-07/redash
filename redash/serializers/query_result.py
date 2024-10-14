import io
import csv
import xlsxwriter
import uuid
import logging
import time
from funcy import rpartial, project
from dateutil.parser import isoparse as parse_date
from redash.query_runner import TYPE_BOOLEAN, TYPE_DATE, TYPE_DATETIME
from redash.authentication.org_resolving import current_org
from redash.permissions import can_override_download_limit
from redash import settings
from redash import models
from datetime import datetime, timedelta, timezone
from redash.tasks.audit_downloads import enqueue_download_audit
from redash.settings import ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING
from redash.utils.gsheets import ClientFactory, DelegatedGspreadClient
from redash import settings
from redash.utils import extract_company
import json

IST_OFFSET = timedelta(hours=5, minutes=30)
OWNER_EMAIL = settings.REDASH_GOOGLE_SHEETS_OWNER_EMAIL

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

def sheet_url_from_id(sheet_id):
    return {"sheet_link": f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid=0"}

def get_worksheet_name(query_name):
    return (f"{query_name}-" if query_name else "") + str(datetime.now(timezone.utc) + IST_OFFSET)

def fetch_gsheets_client(sheet_id: str = None, current_user=OWNER_EMAIL) -> DelegatedGspreadClient:
    google_apps_domains = models.Organization.query.filter(models.Organization.id == 1).first().settings.get("google_apps_domains", None)
    client = ClientFactory(option=settings.REDASH_GOOGLE_SHEET_CLIENT_MAPPING.get(extract_company(current_user)))(
        user_email=current_user,
        sheet_id=sheet_id,
        sheet_name=sheet_id,
        allowed_emails=google_apps_domains,
        clear_cache=True
    )
    return client

def export_serialized_results_to_gsheet(query_result, current_user, query, query_result_id, current_org_id):
    gsheet_id = ""
    sheet_name = f"[{current_user.email}] Redash-download-{str(datetime.now())}"
    try:
        client = fetch_gsheets_client(sheet_id=gsheet_id)
    except Exception as e:
        logging.warn("[Gsheets Export] Error fetching client: {e}".format(e=str(e)))
    try:
        client.get()
        gsheet_id = client.create_new_spreadsheet(sheet_name=sheet_name)
        client.set_permissions(gsheet_id, current_user.email)
        client.add_drive_labels(gsheet_id, labels=["Lvjzj2dSK24gZxerKnm5LeQhciVxhCAv8XpSNNEbbFcb", "sKqKqy2zyrPKOH5El085hgnHjQQhI89fQnhSNNEbbFcb"])
        wb = client.gc.open_by_key(gsheet_id)
        time.sleep(1)
        upload_data_to_gsheet(wb, query_result, sheet_name, current_user.email, query, query_result_id, current_org_id)
        return sheet_url_from_id(gsheet_id)
    except Exception as e:
        logging.error(f"[Gsheets Export] Failed: The export of data to gsheet {gsheet_id} failed because of error: {str(e)}")
        return {"error": f"Export to Google Sheet failed: {str(e)}"}


def upload_data_to_gsheet(wb, query_result, sheet_name, user, query, query_result_id, current_org_id):
    try:
        query_data = query_result.data
        column_names = [col.get("name") for col in query_data["columns"]]
        data = [column_names]
        current_ist_time = datetime.now(timezone.utc) + IST_OFFSET
        export_data = query_data["rows"]

        for row in query_data["rows"]:
            row_data = []
            for name in column_names:
                val = row.get(name, "")
                val = "" if val is None else val
                if isinstance(val, (dict, list)):
                    val = str(val)
                row_data.append(val.encode('utf-8') if isinstance(val, str) else str(val))
            data.append(row_data)
        sh = wb.worksheet(sheet_name)
        sh.clear
        res = sh.batch_update([{
            "range": "A1",
            "values": data,
        }],
        value_input_option="USER_ENTERED")
        if ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING:
            enqueue_download_audit(push_id=uuid.uuid4(), user=user, query=query, time=current_ist_time, format="gsheets", limit=len(export_data), query_result_id=query_result_id, current_org_id=current_org_id, source="export")
        logging.info(f"[Gsheets Export] Response: {json.dumps(res)} while pushing to sheet {sheet_name}")
        logging.info(f"[Gsheets Export] Successfully uploaded query results to Google Sheet.")

    except Exception as e:
        logging.warn(f"[Gsheets Export] Error while uploading data to Gsheet: {str(e)}")
        raise e


def serialize_query_result_to_dsv(query_result, delimiter, current_user, format, query, query_result_id, current_org_id):
    s = io.StringIO()

    query_data = query_result.data

    fieldnames, special_columns = _get_column_lists(query_data["columns"] or [])

    writer = csv.DictWriter(s, extrasaction="ignore", fieldnames=fieldnames, delimiter=delimiter)
    writer.writeheader()
    
    download_limit = len(query_data['rows']) if can_override_download_limit() else settings.QUERY_RESULT_DATA_DOWNLOAD_ROW_LIMIT
    download_data = query_data["rows"][:download_limit]
    current_ist_time = datetime.now(timezone.utc) + IST_OFFSET

    if ENABLE_DOWNLOAD_DATA_AUDIT_LOGGING:
        enqueue_download_audit(push_id=uuid.uuid4(), user=current_user.email, query=query, time=current_ist_time, format=format, limit=len(download_data), query_result_id=query_result_id, current_org_id=current_org_id, source="download")
    
    for row in download_data:
        for col_name, converter in special_columns.items():
            if col_name in row:
                row[col_name] = converter(row[col_name])

        writer.writerow(row)

    return s.getvalue()

def serialize_query_result_to_xlsx(query_result, current_user, format, query, query_result_id, current_org_id):
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
        enqueue_download_audit(push_id=uuid.uuid4(), user=current_user.email, query=query, time=current_ist_time, format=format, limit=len(download_data), query_result_id=query_result_id, current_org_id=current_org_id, source="download")

    for r, row in enumerate(download_data):
        for c, name in enumerate(column_names):
            v = row.get(name)
            if isinstance(v, (dict, list)):
                v = str(v)
            sheet.write(r + 1, c, v)

    book.close()

    return output.getvalue()
