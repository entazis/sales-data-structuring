from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
from dotenv import load_dotenv

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def get_data_from_spreadsheet(spreadsheet_id, sheet_name, range):
    creds = None
    sheet_range = sheet_name + '!' + range

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=sheet_range).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return pd.DataFrame()
    else:
        print('Data successfully got from spreadsheet')
        return pd.DataFrame.from_records(values)


def main():
    load_dotenv()

    liquidataion_limit_df = get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'FT-Std. Price', 'A:F')
    print(liquidataion_limit_df)


if __name__ == '__main__':
    main()