import pandas as pd
import numpy as np


def parse_liquidation_limits(df):
    df = df.astype({'Liquidation Limit': 'float'})
    df = df.astype({'Normal Price': 'float'})
    df = df.astype({'Year': 'int'})

    df['Price Limit'] = df['Normal Price'] * (1 - df['Liquidation Limit'])
    return df


def parse_orders(df):
    df = df.loc[:, ['Order Date', 'Market Place', 'ASIN', 'Price', 'Qty', 'Refunded', 'Sales Channel', 'Customer Pays']]
    df.loc[:, ['Price', 'Customer Pays']] = df.loc[:, ['Price', 'Customer Pays']]\
        .replace('[\$,]', '', regex=True)\
        .replace('', np.nan)
    df.dropna(subset=['Price', 'Customer Pays'], inplace=True)
    df.loc[:, ['Qty', 'Price', 'Customer Pays']] = df.loc[:, ['Qty', 'Price', 'Customer Pays']].astype(float)

    df.loc[:, 'Year'] = pd.DatetimeIndex(df['Order Date']).year.astype(int)
    df.loc[:, 'Month'] = pd.DatetimeIndex(df['Order Date']).strftime('%B')
    df.loc[:, 'Day'] = pd.DatetimeIndex(df['Order Date']).day.astype(int)
    df.drop(['Order Date'], axis=1, inplace=True)

    df['Price/Qty'] = df['Price'] / df['Qty']

    return df


def parse_out_of_stock_days(df):
    df['Year'] = pd.DatetimeIndex(df['End']).year.astype(int)
    df['Month'] = pd.DatetimeIndex(df['End']).strftime('%B')
    df['Day'] = pd.DatetimeIndex(df['End']).day.astype(int)

    return df


def read_sales_xlsx(filename):
    df = pd.read_excel(filename)
    df.drop(df.columns[0], axis=1, inplace=True)

    # df['Year'] = pd.DatetimeIndex(df['Date']).year.astype(int)
    # df['Month'] = pd.DatetimeIndex(df['Date']).strftime('%B')
    # df['Day'] = pd.DatetimeIndex(df['Date']).day.astype(int)
    # df = df[['Year', 'Month', 'Day', 'Market Place', 'ASIN', 'Units', 'Refunded', 'PPC Orders']]
    # df.to_excel('sales.xlsx', sheet_name='sales')

    return df


def read_out_of_stock_csv(filename):
    df = pd.read_csv(filename)
    df = df[['Market Place', 'ASIN', 'Out of stock days']]
    df['Year'] = 2018
    df['Month'] = 'October'
    # FIXME

    return df


def read_orders_csv(filename):
    df = pd.read_csv(filename, encoding="ISO-8859-1")
    df = parse_orders(df)
    return df
