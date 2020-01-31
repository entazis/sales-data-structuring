from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import numpy as np
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
    headers = values.pop(0)

    if not values:
        print('No data found.')
        return pd.DataFrame()
    else:
        print('Data successfully got from spreadsheet')
        return pd.DataFrame(values, columns=headers)


def parse_liquidation_limits(df):
    df = df.astype({'Standard Price': 'float'})
    df = df.astype({'Year': 'int'})
    df['Liquidation Limit'] = \
        df['Liquidation Limit'].replace('[\%,]', '', regex=True).astype(float) / 100
    df['Price Limit'] = \
        df['Standard Price'] * (1-df['Liquidation Limit'])
    return df


def parse_orders(df):
    df[['Price', 'Customer Pays']] = df[['Price', 'Customer Pays']]\
        .replace('[\$,]', '', regex=True)\
        .replace('', np.nan)
    df.dropna(subset=['Price', 'Customer Pays'], inplace=True)
    df[['Price', 'Customer Pays']] = df[['Price', 'Customer Pays']].astype(float)

    df['Year'] = pd.DatetimeIndex(df['Order Date']).year.astype(int)
    df['Month'] = pd.DatetimeIndex(df['Order Date']).strftime('%B')
    df['Day'] = pd.DatetimeIndex(df['Order Date']).day.astype(int)

    return df


def get_liquidation_sales(orders_df, liquidataion_limit_df):
    orders_with_liquidation_limit = pd.merge(orders_df, liquidataion_limit_df,
                                 how='left',
                                 on=['Year', 'Month', 'Product Group'])
    orders_with_liquidation_limit.dropna(subset=['Price Limit'], inplace=True)
    orders_liquidation = orders_with_liquidation_limit[
        orders_with_liquidation_limit['Customer Pays'] <= orders_with_liquidation_limit['Price Limit']
        ]
    return orders_liquidation


def main():
    load_dotenv()

    liquidataion_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'FT-Std. Price', 'A:F')
    )
    orders = parse_orders(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-Historical Orders', 'A:CJ')
    )

    # orders_amazon = orders[orders['Sales Channel'] == 'Amazon.com']
    # orders_non_amazon = orders[orders['Sales Channel'] == 'Non-Amazon']

    liquidation_sales = get_liquidation_sales(orders, liquidataion_limit)


if __name__ == '__main__':
    main()