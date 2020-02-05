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
    df.drop(columns=['Product Group'], inplace=True)
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

    df['Price/Qty'] = df['Price'] / df['Qty']

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
    df_sku_mapped = add_cin7_sku_map_for_asin(df[cols_to_use], sku_mapping)

    return df_sku_mapped


def get_liquidation_orders(orders_df, liquidataion_limit_df):
    orders_with_liquidation_limit = pd.merge(orders_df, liquidataion_limit_df,
                                 how='left',
                                 on=['Cin7', 'Year', 'Month'])
    orders_with_liquidation_limit.dropna(subset=['Price Limit'], inplace=True)
    orders_liquidation = orders_with_liquidation_limit[
        (orders_with_liquidation_limit['Price/Qty'] <= orders_with_liquidation_limit['Price Limit'])
        & (orders_with_liquidation_limit['Sales Channel'] != 'Non-Amazon')
    ]
    return orders_liquidation


def add_out_of_stock_days(orders_df, out_of_stock_df):
    orders_with_out_of_stock_days = pd.merge(
        orders_df,
        out_of_stock_df[['Cin7', 'Year', 'Month', 'Market Place', 'Out of stock days']],
        how='left',
        on=['Cin7', 'Year', 'Month', 'Market Place'])
    orders_with_out_of_stock_days.fillna('-', inplace=True)

    return orders_with_out_of_stock_days


def add_cin7_sku_map_for_asin(df, sku_map):
    cols_to_use = df.columns.difference(sku_map.columns)
    sku_mapped = pd.merge(df[cols_to_use], sku_map,
                          how='left',
                          left_on='ASIN',
                          right_on='Amazon-ASIN')
    sku_mapped.dropna(subset=['Cin7'], inplace=True)
    return sku_mapped


def format_for_google_sheet_upload(df):
    headers = [list(df.columns.values)]
    values = df.values.tolist()
    return headers + values


def calculate_historical_table(df, group_by):
    qty_sum = df.groupby(group_by)['Qty'].sum()
    customer_pays_mean = df.groupby(group_by)['Price/Qty'].mean()

    calc_historical = pd.concat([qty_sum, customer_pays_mean], axis=1).reset_index()
    calc_historical['Revenue'] = \
        calc_historical['Qty'] * calc_historical['Price/Qty']
    calc_historical.rename(columns={'Qty': 'Sales QTY',
                                    'Price/Qty': 'Avg Sale Price'}, inplace=True)

    return calc_historical


def main():
    load_dotenv()

    sku_mapping = parse_sku_mapping(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Sku Dump')
    )
    liquidation_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'FT-Std. Price')
    )
    out_of_stock_days = add_cin7_sku_map_for_asin(
        parse_out_of_stock_days(
            get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-Stockout Days')
        ), sku_mapping
    )
    orders = update_product_group_using_sku_mapping(
            parse_orders(
                get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-Historical Orders')
            ), sku_mapping
    )

    liquidation_orders = get_liquidation_orders(orders, liquidation_limit)
    liquidation_orders['Sales Type'] = 'Liquidation'
    liquidation_orders['Fulfillment Type'] = ''
    liquidation_orders['Promotion Notes'] = ''

    calc_historical_liquidation = calculate_historical_table(liquidation_orders, [
        'Brand', 'Market Place', 'Sales Channel', 'Fulfillment Type', 'Product Group', 'Cin7', 'Sales Type',
        'Promotion Ids', 'Promotion Notes', 'Year', 'Month'
    ])
    calc_historical_liquidation = add_out_of_stock_days(calc_historical_liquidation, out_of_stock_days)

    calc_historical_total_sales = calculate_historical_table(orders, [
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Promotion Ids', 'Year', 'Month'
    ])
    calc_historical_total_sales = add_out_of_stock_days(calc_historical_total_sales, out_of_stock_days)

    calc_historical_non_amazon = calculate_historical_table(orders[orders['Sales Channel'] == 'Non-Amazon'], [
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Promotion Ids', 'Year', 'Month'
    ])
    calc_historical_non_amazon = add_out_of_stock_days(calc_historical_non_amazon, out_of_stock_days)

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_liquidation),
        os.getenv('SPREADSHEET_ID'),
        'python-liquidation'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_total_sales),
        os.getenv('SPREADSHEET_ID'),
        'python-total-sales'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_non_amazon),
        os.getenv('SPREADSHEET_ID'),
        'python-non-amazon'
    )

    orders_without_liquidation = pd.concat([orders, liquidation_orders], sort=True).drop_duplicates()

    amazon_orders_without_liquidation_and_promotions = orders_without_liquidation[
        (orders_without_liquidation['Sales Channel'] != 'Non-Amazon')
        & (orders_without_liquidation['Promotion Ids'] == '')
    ]


if __name__ == '__main__':
    main()
