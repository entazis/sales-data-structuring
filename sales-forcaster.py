from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import numpy as np
from dotenv import load_dotenv

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def authenticate_google_sheets():
    creds = None
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
        print('No data found.')
        return pd.DataFrame()
    else:
        print('Data successfully got from spreadsheet!')
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


def parse_liquidation_limits(df):
    df = df.astype({'Standard Price': 'float'})
    df = df.astype({'Year': 'int'})
    df['Liquidation Limit'] = \
        df['Liquidation Limit'].replace('[\%,]', '', regex=True).astype(float) / 100
    df['Price Limit'] = \
        df['Standard Price'] * (1-df['Liquidation Limit'])
    df.drop('SKU', 1, inplace=True)
    return df


def parse_orders(df):
    df[['Price', 'Customer Pays']] = df[['Price', 'Customer Pays']]\
        .replace('[\$,]', '', regex=True)\
        .replace('', np.nan)
    df.dropna(subset=['Price', 'Customer Pays'], inplace=True)
    df[['Qty', 'Price', 'Customer Pays']] = df[['Qty', 'Price', 'Customer Pays']].astype(float)

    df['Year'] = pd.DatetimeIndex(df['Order Date']).year.astype(int)
    df['Month'] = pd.DatetimeIndex(df['Order Date']).strftime('%B')
    df['Day'] = pd.DatetimeIndex(df['Order Date']).day.astype(int)

    return df


def parse_out_of_stock_days(df):
    df['Year'] = pd.DatetimeIndex(df['End']).year.astype(int)
    df['Month'] = pd.DatetimeIndex(df['End']).strftime('%B')
    df['Day'] = pd.DatetimeIndex(df['End']).day.astype(int)

    return df


def parse_sku_mapping(df):
    return df


def update_product_group_using_sku_mapping(df, sku_mapping):
    cols_to_use = df.columns.difference(sku_mapping.columns)
    df_sku_mapped = pd.merge(df[cols_to_use], sku_mapping,
                             how='left',
                             left_on='SKU',
                             right_on='Amazon-Sku')
    df_sku_mapped.dropna(subset=['Product Group'], inplace=True)

    return df_sku_mapped


def get_liquidation_orders(orders_df, liquidataion_limit_df):
    orders_with_liquidation_limit = pd.merge(orders_df, liquidataion_limit_df,
                                 how='left',
                                 on=['Year', 'Month', 'Product Group'])
    orders_with_liquidation_limit.dropna(subset=['Price Limit'], inplace=True)
    orders_liquidation = orders_with_liquidation_limit[
        ((orders_with_liquidation_limit['Customer Pays'] / orders_with_liquidation_limit['Qty'])
         <= orders_with_liquidation_limit['Price Limit'])
        & (orders_with_liquidation_limit['Sales Channel'] != 'Non-Amazon')
    ]
    return orders_liquidation


def add_out_of_stock_days(orders_df, out_of_stock_df):
    orders_with_out_of_stock_days = pd.merge(orders_df, out_of_stock_df,
                                             how='left',
                                             on=['ASIN', 'Year', 'Month', 'Product Group', 'Market Place'])
    return orders_with_out_of_stock_days


def format_for_google_sheet_upload(df):
    headers = [list(df.columns.values)]
    values = df.values.tolist()
    return headers + values


def main():
    load_dotenv()

    sku_mapping = parse_sku_mapping(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'FT-Mapping-Sku.Asin.Group')
    )
    liquidation_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'FT-Std. Price')
    )
    out_of_stock_days = parse_out_of_stock_days(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-Stockout Days')
    )
    orders = update_product_group_using_sku_mapping(
        parse_orders(
            get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-Historical Orders')
        ), sku_mapping
    )

    liquidation_orders = get_liquidation_orders(orders, liquidation_limit)
    liquidation_orders_with_out_of_stock = add_out_of_stock_days(liquidation_orders, out_of_stock_days)
    liquidation_orders_with_out_of_stock['Sales Type'] = 'Liquidation'
    liquidation_orders_with_out_of_stock['Fulfillment Type'] = ''
    liquidation_orders_with_out_of_stock['Promotion Notes'] = ''

    qty_sum = liquidation_orders_with_out_of_stock.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Fulfillment Type', 'Product Group', 'SKU', 'Sales Type',
        'Promotion Ids', 'Promotion Notes', 'Year', 'Month', 'Out of stock days'
    ])['Qty'].sum()
    customer_pays_mean = liquidation_orders_with_out_of_stock.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Fulfillment Type', 'Product Group', 'SKU', 'Sales Type',
        'Promotion Ids', 'Promotion Notes', 'Year', 'Month', 'Out of stock days'
    ])['Customer Pays'].mean()

    calc_historical_liquidation = pd.concat([qty_sum, customer_pays_mean], axis=1).reset_index()
    calc_historical_liquidation['Revenue'] = \
        calc_historical_liquidation['Qty'] * calc_historical_liquidation['Customer Pays']
    calc_historical_liquidation.rename(columns={'Qty': 'Sales QTY', 'Customer Pays': 'Avg Sale Price'}, inplace=True)

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_liquidation),
        os.getenv('SPREADSHEET_ID'),
        'python-liquidation'
    )

    orders_without_liquidation = pd.concat([orders, liquidation_orders]).drop_duplicates()

    amazon_orders_without_liquidation_and_promotions = orders_without_liquidation[
        (orders_without_liquidation['Sales Channel'] != 'Non-Amazon')
        & (orders_without_liquidation['Promotion Ids'] == '')
    ]


if __name__ == '__main__':
    main()
