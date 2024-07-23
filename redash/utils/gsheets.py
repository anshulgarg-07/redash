import gspread
import logging
import json
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from redash import settings
from redash.utils import extract_company

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_delegated_gc_client={}
_gc_client = ""


class GspreadClient(object):
    def __init__(self, user_email, sheet_id, sheet_name, allowed_emails, clear_cache=False):
        self.user_email = user_email
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.allowed_emails = allowed_emails
        self.clear_cache = clear_cache

    def owner_domain(self):
        for permission in self.all_permissions:
            if permission["role"] == "owner":
                return permission["domain"]

    def get_editors(self):
        editors = []
        for permission in self.all_permissions:
            if permission.get("role", "") in ("writer", "owner"):
                # Cases
                # Individual permission object have emailAddress in them
                # Organization level permission object have domain in them
                # Access to anyone permission object have type = anyone in them
                editors.append(permission.get("emailAddress", permission.get("domain", permission.get("type"))))

        return editors

    def get_gsheet(self):
        wb = self.gc.open_by_key(self.sheet_id)
        self.all_permissions = wb.list_permissions()
        editors = self.get_editors()
        if self.owner_domain() not in self.allowed_emails:
            raise Exception("You are trying to sync data to a sheet outside the organization. Please make sure your sheet is owned by an email owned by the organization")
        if "anyone" in editors or self.user_email.split("@")[1] in editors or self.user_email in editors:
            sh = wb.worksheet(self.sheet_name)
        else:
            raise Exception("Please make sure you have edit access to the sheet or ask "
            "someone with edit access to send data to the sheet")
        return sh

    def handle_errors(self, e):
        raise e


class DelegatedGspreadClient(GspreadClient):
    def __init__(self, *args, **kwargs):
        super(DelegatedGspreadClient, self).__init__(*args, **kwargs)

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
            self.gc = gspread.Client(auth=delegated_credentials)
            _delegated_gc_client[self.user_email] = self.gc

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
