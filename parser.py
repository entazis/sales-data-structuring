import pandas as pd
import numpy as np
import re
from calendar import month_name


def parse_liquidation_limits(df):
    df = df.astype({'Liquidation Limit': 'float'})
    df = df.astype({'Normal Price': 'float'})
    df = df.astype({'Year': 'int'})

    df['Price Limit'] = df['Normal Price'] * (1 - df['Liquidation Limit'])
    return df


def parse_orders(df):
    df = df.loc[:, ['Order Date', 'Market Place', 'ASIN', 'Price', 'Qty', 'Refunded', 'Sales Channel', 'Customer Pays']]
    df.loc[:, ['Price', 'Customer Pays']] = df.loc[:, ['Price', 'Customer Pays']]\
        .replace('[\$,]', '', regex=True) \
        .replace('[\£,]', '', regex=True) \
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


def read_out_of_stock_csv(filenames):
    df = pd.DataFrame(columns=['Market Place', 'ASIN', 'Out of stock days', 'Year', 'Month'])
    month_pattern = '|'.join(month_name[1:])
    year_list = ["{0}".format(year) for year in range(2017, 2021)]
    year_pattern = ' | '.join(year_list)

    for filename in filenames:
        stock_out = pd.read_csv(filename)
        stock_out = stock_out[['Market Place', 'ASIN', 'Out of stock days']]

        month = re.search(month_pattern, filename, re.IGNORECASE).group(0).capitalize()
        year = re.search(year_pattern, filename, re.IGNORECASE).group(0)

        stock_out['Year'] = int(year)
        stock_out['Month'] = month

        df = df.append(stock_out, ignore_index=True)

    return df


def read_orders_csv(filenames):
    df = pd.DataFrame(columns=['Market Place', 'Year', 'Month', 'Day', 'ASIN',
                               'Price', 'Qty', 'Price/Qty', 'Refunded', 'Sales Channel', 'Customer Pays'])
    for filename in filenames:
        orders = pd.read_csv(filename, encoding="ISO-8859-1", low_memory=False)
        orders = parse_orders(orders)
        df = df.append(orders, ignore_index=True, sort=False)

    return df
