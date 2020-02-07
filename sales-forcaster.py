from __future__ import print_function
import os.path
from dotenv import load_dotenv

from parser import *
from gservice import *


def update_product_group_using_sku_mapping(df, sku_mapping):
    cols_to_use = df.columns.difference(sku_mapping.columns)
    df_sku_mapped = match_asin_cin7(df[cols_to_use], sku_mapping)

    return df_sku_mapped


def get_liquidation_orders(orders_df, liquidataion_limit_df):
    orders_with_liquidation_limit = pd.merge(orders_df, liquidataion_limit_df,
                                 how='left',
                                 on=['Cin7', 'Year', 'Month'])
    # TODO check if needed
    # orders_with_liquidation_limit.dropna(subset=['Price Limit'], inplace=True)

    # TODO change back after cin7
    orders_liquidation = orders_with_liquidation_limit
    # orders_liquidation = orders_with_liquidation_limit[
    #     orders_with_liquidation_limit['Price/Qty'] <= orders_with_liquidation_limit['Price Limit']
    # ]

    orders_liquidation.drop(['Liquidation Limit', 'Normal Price', 'Price Limit'], axis=1, inplace=True)
    return orders_liquidation


def add_out_of_stock_days(orders_df, out_of_stock_df):
    orders_with_out_of_stock_days = pd.merge(
        orders_df,
        out_of_stock_df[['Cin7', 'Year', 'Month', 'Market Place', 'Out of stock days']],
        how='left',
        on=['Cin7', 'Year', 'Month', 'Market Place'])
    orders_with_out_of_stock_days.fillna(0, inplace=True)

    return orders_with_out_of_stock_days


def match_asin_cin7(df, asin_cin7_map):
    matched = pd.merge(df, asin_cin7_map,
                          how='left',
                          left_on='ASIN',
                          right_on='Amazon-ASIN')
    matched.drop(['Amazon-ASIN', 'Amazon-Sku'], axis=1, inplace=True)
    # TODO comment back after cin7
    # matched.dropna(subset=['Cin7'], inplace=True)
    return matched


def calculate_historical_table(df):
    qty_sum = df.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Year', 'Month'
    ])['Qty'].sum()
    customer_pays_mean = df.groupby([
        'Brand', 'Market Place', 'Sales Channel', 'Product Group', 'Cin7', 'Year', 'Month'
    ])['Price/Qty'].mean()

    calc_historical = pd.concat([qty_sum, customer_pays_mean], axis=1).reset_index()
    calc_historical['Revenue'] = \
        calc_historical['Qty'] * calc_historical['Price/Qty']
    calc_historical.rename(columns={'Qty': 'Sales QTY',
                                    'Price/Qty': 'Avg Sale Price'}, inplace=True)

    return calc_historical


def calculate_amazon_ppc_orders(orders, liquidation_orders, orders_non_amazon):
    amazon_ppc_orders = subtract_dataframes(orders, liquidation_orders)
    amazon_ppc_orders = subtract_dataframes(amazon_ppc_orders, orders_non_amazon)
    return amazon_ppc_orders


def subtract_dataframes(df1, df2):
    result = df1.merge(df2, on=['Brand', 'Market Place', 'Sales Channel', 'Product Group',
                                'Cin7', 'Year', 'Month'],
                       how='left', indicator=True)
    result = result[result['_merge'] == 'left_only']
    result.drop(['_merge'], axis=1, inplace=True)
    result.dropna(axis=1, how='all', inplace=True)
    drop_y(result)
    rename_x(result)
    return result


def drop_y(df):
    to_drop = [x for x in df if x.endswith('_y')]
    df.drop(to_drop, axis=1, inplace=True)


def rename_x(df):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)


def main():
    load_dotenv()

    sales = read_sales_xlsx('sales.xlsx')
    out_of_stock = read_out_of_stock_csv('Input Stock Out Days/INVENTORY-20200205-194353 october 2018 stock outdays.csv')
    orders = read_orders_csv('ORDERS-20200206-141613-Bear-Butt.csv')

    authenticate_google_sheets()

    cin7_product = get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Cin7-Product-Map')
    liquidation_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Liquidation-Limits')
    )
    asin_cin7 = get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-ASIN-Cin7-Map')

    orders = match_asin_cin7(orders, asin_cin7)

    orders = orders[['Cin7', 'Year', 'Month', 'Day', 'Market Place', 'Sales Channel',
                     'Qty', 'Price', 'Price/Qty', 'Customer Pays']]
    orders_amazon = orders[orders['Sales Channel'] != 'Non-Amazon']
    orders_non_amazon = orders[orders['Sales Channel'] == 'Non-Amazon']

    out_of_stock = match_asin_cin7(out_of_stock, asin_cin7)

    liquidation_orders = get_liquidation_orders(orders_amazon, liquidation_limit)

    calc_historical_liquidation = calculate_historical_table(liquidation_orders)
    # calc_historical_liquidation = add_out_of_stock_days(calc_historical_liquidation, out_of_stock)

    calc_historical_total_sales = calculate_historical_table(orders)
    # calc_historical_total_sales = add_out_of_stock_days(calc_historical_total_sales, out_of_stock)

    calc_historical_non_amazon = calculate_historical_table(orders_non_amazon)
    # calc_historical_non_amazon = add_out_of_stock_days(calc_historical_non_amazon, out_of_stock)

    amazon_ppc_orders = calculate_amazon_ppc_orders(orders, liquidation_orders, orders_non_amazon)

    calc_historical_amazon_ppc = calculate_historical_table(amazon_ppc_orders)
    # calc_historical_amazon_ppc = add_out_of_stock_days(calc_historical_amazon_ppc, out_of_stock)

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
