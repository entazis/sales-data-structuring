from __future__ import print_function
import os.path
import pandas as pd
from dotenv import load_dotenv

from parser import *
from gservice import *


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
        & (orders_with_liquidation_limit['Customer Pays'] != 0)
    ]
    return orders_liquidation


def add_out_of_stock_days(orders_df, out_of_stock_df):
    orders_with_out_of_stock_days = pd.merge(
        orders_df,
        out_of_stock_df[['Cin7', 'Year', 'Month', 'Market Place', 'Out of stock days']],
        how='left',
        on=['Cin7', 'Year', 'Month', 'Market Place'])
    orders_with_out_of_stock_days.fillna(0, inplace=True)

    return orders_with_out_of_stock_days


def add_cin7_sku_map_for_asin(df, sku_map):
    cols_to_use = df.columns.difference(sku_map.columns)
    sku_mapped = pd.merge(df[cols_to_use], sku_map,
                          how='left',
                          left_on='ASIN',
                          right_on='Amazon-ASIN')
    sku_mapped.dropna(subset=['Cin7'], inplace=True)
    return sku_mapped


def calculate_historical_table(df):
    qty_sum = df.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Promotion Ids', 'Year', 'Month'
    ])['Qty'].sum()
    customer_pays_mean = df.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Promotion Ids', 'Year', 'Month'
    ])['Price/Qty'].mean()

    calc_historical = pd.concat([qty_sum, customer_pays_mean], axis=1).reset_index()
    calc_historical['Revenue'] = \
        calc_historical['Qty'] * calc_historical['Price/Qty']
    calc_historical.rename(columns={'Qty': 'Sales QTY',
                                    'Price/Qty': 'Avg Sale Price'}, inplace=True)

    return calc_historical


def calculate_amazon_ppc_orders(orders, liquidation_orders, orders_non_amazon):
    amazon_ppc_orders = orders.merge(liquidation_orders, on=['Brand', 'Market Place', 'Sales Channel', 'Product Group',
                                                             'Cin7', 'Promotion Ids', 'Year', 'Month'],
                                     how='left', indicator=True)
    amazon_ppc_orders = amazon_ppc_orders[amazon_ppc_orders['_merge'] == 'left_only']
    amazon_ppc_orders.drop(['_merge'], axis=1, inplace=True)

    amazon_ppc_orders = amazon_ppc_orders.merge(orders_non_amazon, on=['Brand', 'Market Place', 'Sales Channel', 'Product Group',
                                                                       'Cin7', 'Promotion Ids', 'Year', 'Month'],
                                                how='left', indicator=True)
    amazon_ppc_orders = amazon_ppc_orders[amazon_ppc_orders['_merge'] == 'left_only']

    return amazon_ppc_orders


def main():
    load_dotenv()

    authenticate_google_sheets()

    sku_mapping = parse_sku_mapping(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Sku Dump')
    )
    liquidation_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('SPREADSHEET_ID'), 'Input-FT-Std. Price')
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
    orders_non_amazon = orders[orders['Sales Channel'] == 'Non-Amazon']

    liquidation_orders = get_liquidation_orders(orders, liquidation_limit)
    liquidation_orders['Sales Type'] = 'Liquidation'
    liquidation_orders['Fulfillment Type'] = ''
    liquidation_orders['Promotion Notes'] = ''
    calc_historical_liquidation = calculate_historical_table(liquidation_orders)
    calc_historical_liquidation = add_out_of_stock_days(calc_historical_liquidation, out_of_stock_days)

    calc_historical_total_sales = calculate_historical_table(orders)
    calc_historical_total_sales = add_out_of_stock_days(calc_historical_total_sales, out_of_stock_days)

    calc_historical_non_amazon = calculate_historical_table(orders_non_amazon)
    calc_historical_non_amazon = add_out_of_stock_days(calc_historical_non_amazon, out_of_stock_days)

    amazon_ppc_orders = calculate_amazon_ppc_orders(orders, liquidation_orders, orders_non_amazon)

    calc_historical_amazon_ppc = calculate_historical_table(amazon_ppc_orders)
    calc_historical_amazon_ppc = add_out_of_stock_days(calc_historical_amazon_ppc, out_of_stock_days)

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

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_amazon_ppc),
        os.getenv('SPREADSHEET_ID'),
        'python-amazon-sales-ppc'
    )


if __name__ == '__main__':
    main()
