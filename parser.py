import pandas as pd
import numpy as np


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


def read_xlsx(filename):
    df = pd.read_excel(filename)
    df.drop(df.columns[0], axis=1, inplace=True)

    # df['Year'] = pd.DatetimeIndex(df['Date']).year.astype(int)
    # df['Month'] = pd.DatetimeIndex(df['Date']).strftime('%B')
    # df['Day'] = pd.DatetimeIndex(df['Date']).day.astype(int)
    # df = df[['Year', 'Month', 'Day', 'Market Place', 'ASIN', 'Units', 'Refunded', 'PPC Orders']]
    # df.to_excel('sales.xlsx', sheet_name='sales')

    return df


def read_csv(filename):
    df = pd.read_csv(filename)
    df = df[['Market Place', 'ASIN', 'Out of stock days']]
    df['Year'] = 2019
    df['Month'] = 'January'
    # FIXME

    return df
