import gspread
import logging
from requests import Session
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as rq
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from redash import settings
from redash.utils import extract_company

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

_delegated_gc_client={}
_gc_client = ""


class TimeoutSession(Session):
    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", 300)
        return super(TimeoutSession, self).request(*args, **kwargs)

class GspreadClient(object):
    def __init__(self, user_email, sheet_id, sheet_name, allowed_emails, clear_cache=False):
        self.user_email = user_email
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.allowed_emails = allowed_emails
        self.clear_cache = clear_cache

    def get_editors(self, wb = None):
        editors = []
        permissions = wb.list_permissions() if wb else None
        for permission in permissions or self.all_permissions:
            if permission.get("role", "") in ("writer", "owner"):
                # Cases
                # Individual permission object have emailAddress in them
                # Organization level permission object have domain in them
                # Access to anyone permission object have type = anyone in them
                editors.append(permission.get("emailAddress", permission.get("domain", permission.get("type"))))

        return editors

    def get_gsheet(self, sheet_name: str = None):
        wb = self.gc.open_by_key(self.sheet_id)
        self.all_permissions = wb.list_permissions()
        editors = self.get_editors()
        if "anyone" in editors or self.user_email.split("@")[1] in editors or self.user_email in editors:
            sh = wb.worksheet(sheet_name if sheet_name is not None else self.sheet_name)
        else:
            raise Exception("Please make sure you have edit access to the sheet or ask "
            "someone with edit access to send data to the sheet")
        return sh

    def handle_errors(self, e):
        raise e


class DelegatedGspreadClient(GspreadClient):
    def __init__(self, *args, **kwargs):
        super(DelegatedGspreadClient, self).__init__(*args, **kwargs)
        self.drive_service_api = None
        self.sheets_service_api = None

    def _cached_client(self):
        if self.clear_cache is False:
            self.gc = _delegated_gc_client[self.user_email]
        else:
            raise KeyError("Invalidate Cache")

    def get(self):
        try:
            self.gc = self._cached_client()
        except KeyError:
            cred_dict = settings.REDASH_GOOGLE_SHEET_DELEGATED_CONFIGS.get(extract_company(self.user_email))
            credentials = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
            delegated_credentials = credentials.with_subject(self.user_email)
            delegated_credentials.refresh(rq())
            timeout_session = Session()
            timeout_session.requests_session = TimeoutSession()
            self.gc = gspread.Client(auth=delegated_credentials, session=timeout_session)
            self.gc.login()
            _delegated_gc_client[self.user_email] = self.gc

    def drive_service(self):
        if self.drive_service_api is None:
            self.drive_service_api = build('drive', 'v3', credentials=self.gc.auth, cache_discovery=False)
        return self.drive_service_api

    def sheets_service(self):
        if self.sheets_service_api is None:
            self.sheets_service_api = build('sheets', 'v4', credentials=self.gc.auth, cache_discovery=False)
        return self.sheets_service_api

    def create_new_spreadsheet(self, sheet_name):
        try:
            sheet = self.gc.create(sheet_name)
            self.sheet_id = sheet.id
            requests = [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": 0,
                        "title": sheet_name
                    },
                    "fields": "title"
                }
            }]
            body = {
                'requests': requests
            }
            self.sheets_service().spreadsheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body
            ).execute()
            logging.info(f"[Gsheets Export] New Gsheet created with ID: {self.sheet_id} and title: {sheet_name}")
            return self.sheet_id
        except HttpError as error:
            raise error

    def set_permissions(self, sheet_id, user):
        try:
            self.drive_service().permissions().create(
                fileId=sheet_id,
                body={'type': 'user', 'role': 'writer', 'emailAddress': user}
            ).execute()
            logging.info(f"[Gsheets Export] Editor access granted to {user} for sheet {sheet_id}.")
        except HttpError as error:
            raise error

    def add_drive_labels(self, sheet_id, labels):
        try:
            body = {
                "labelModifications": [
                    {
                        "labelId": id
                    } for id in labels
                ]
            }
            self.drive_service().files().modifyLabels(
                fileId=sheet_id,
                body=body
            ).execute()
            logging.info(f"[Gsheets Export] Security labels added to spreadsheet with ID: {sheet_id}")
        except HttpError as error:
            raise error

    def handle_errors(self, e):
        if e.args[0]["code"] == 404:
            raise Exception("No spreadsheet with id: {id} exists. Please make sure you input the correct sheet id and "
                "have editor access to the sheet"
                    .format(id=self.sheet_id))
        else:
            raise e


class ServiceAccountGspreadClient(GspreadClient):
    def __init__(self, *args, **kwargs):
        super(ServiceAccountGspreadClient, self).__init__(*args, **kwargs)

    def _cached_client(self):
        if self.clear_cache is False:
            self.gc = _gc_client
        else:
            raise KeyError("Invalidate Cache")

    def get(self):
        try:
            self.gc = self._cached_client()
        except KeyError:
            secret = settings.REDASH_GOOGLE_SHEET_API_CONFIG
            credentials = Credentials.from_service_account_info(secret, scopes=SCOPES)
            self.gc = gspread.authorize(credentials)
            _gc_client = self.gc

    def handle_errors(self, e):
        if e.args[0]["code"] == 404:
            raise Exception("No spreadsheet with id: {id} exists. Please make sure you input the correct sheet id and "
                "have given editor access to redash-destination-sync@dse-tools-production.iam.gserviceaccount.com"
                    .format(id=self.sheet_id))
        else:
            raise e



def ClientFactory(option="service_account"):
    options={
        "delegation": DelegatedGspreadClient,
        "service_account": ServiceAccountGspreadClient
    }

    if option not in options.keys():
        raise Exception("{option} is not a supported client creation option. Supported options: {options}. Please reach out to the Administrator!"
            .format(option=option, options=options.keys()))
    return options[option]


def get_gsheet(user_email, sheet_id, sheet_name, allowed_emails, clear_cache=False):
    try:
        client = ClientFactory(option=settings.REDASH_GOOGLE_SHEET_CLIENT_MAPPING.get(extract_company(user_email)))(
            user_email=user_email,
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            allowed_emails=allowed_emails,
            clear_cache=clear_cache
        )
        logging.info(f"the client in get_gsheet is {client}")
        gc = client.get()
    except Exception as e:
        logging.warn("Error: {e}".format(e=str(e)))
        raise Exception("There was some issue while creating the gspread client. Please try again or reach out to the administrator")
    try:
        sh = client.get_gsheet()
    except APIError as e:
        if e.args[0]["code"] == 429:
            client.clear_cache = True
            gc = client.get()
            wb = gc.open_by_key(sheet_id)
            sh = wb.worksheet(sheet_name)
        else:
            logging.warn("Error: {e}".format(e=str(e)))
            client.handle_errors(e)
    return sh
