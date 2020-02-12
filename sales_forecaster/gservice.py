import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def authenticate_google_sheets():
    creds = None
    if os.path.exists('../token.pickle'):
        with open('../token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                './credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('../token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_data_from_spreadsheet(spreadsheet_id, sheet_name):
    creds = authenticate_google_sheets()
    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=sheet_name).execute()
    values = result.get('values', [])
    headers = values.pop(0)

    if not values:
        print('No data found in ', sheet_name)
        return pd.DataFrame()
    else:
        print('Data successfully got from sheet: ', sheet_name)
        return pd.DataFrame(values, columns=headers)


def upload_data_to_sheet(values, spreadsheet_id, sheet_name):
    creds = authenticate_google_sheets()
    service = build('sheets', 'v4', credentials=creds)

    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=sheet_name).execute()

    body = {
        'values': values
    }
    value_input_option = 'RAW'

    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=sheet_name,
        valueInputOption=value_input_option, body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))

    if result.get('updatedCells') < 1:
        print('No cells were updated')
        return False
    else:
        print('Some cells were successfully updated!')
        return True


def format_for_google_sheet_upload(df):
    headers = [list(df.columns.values)]
    values = df.values.tolist()
    return headers + values
