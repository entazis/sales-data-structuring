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
