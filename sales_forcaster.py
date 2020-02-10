from __future__ import print_function
import os.path
import glob
from dotenv import load_dotenv

from parser import *
from gservice import *


def get_liquidation_orders(orders_df, liquidataion_limit_df):
    orders_with_liquidation_limit = pd.merge(orders_df, liquidataion_limit_df,
                                 how='left',
                                 on=['Cin7', 'Year', 'Month'])
    orders_with_liquidation_limit.dropna(subset=['Price Limit'], inplace=True)

    orders_liquidation = orders_with_liquidation_limit[
        orders_with_liquidation_limit['Price/Qty'] <= orders_with_liquidation_limit['Price Limit']
    ]

    orders_liquidation = orders_liquidation.drop(['Liquidation Limit', 'Normal Price', 'Price Limit'], axis=1)
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
    matched.drop(['Amazon-ASIN'], axis=1, inplace=True)
    matched.dropna(subset=['Cin7'], inplace=True)
    return matched


def match_cin7_product(df, cin7_product_map):
    matched = pd.merge(df, cin7_product_map,
                          how='left',
                          on='Cin7')
    return matched


def calculate_historical_table(df):
    qty_sum = df.groupby([
        'Year', 'Month', 'Day', 'Market Place', 'Cin7'
    ])['Qty'].sum()
    unit_price_mean = df.groupby([
        'Year', 'Month', 'Day', 'Market Place', 'Cin7'
    ])['Price/Qty'].mean()

    calc_historical = pd.concat([qty_sum, unit_price_mean], axis=1).reset_index()

    calc_historical = calc_historical[['Cin7', 'Market Place', 'Year', 'Month', 'Day', 'Qty', 'Price/Qty']]
    return calc_historical


def sum_ppc_orders_by_product_group(df):
    qty_sum = df.groupby([
        'Market Place', 'Year', 'Month', 'Day', 'Brand', 'Product Group'
    ])['PPC Orders'].sum()
    ppc_sums = qty_sum.reset_index()
    return ppc_sums


def calculate_ppc_portions(df):
    daily_brand_pg_sum = df.groupby([
        'Market Place', 'Year', 'Month', 'Day', 'Brand', 'Product Group'
    ])['Qty'].sum().reset_index().rename(columns={'Qty': 'Category Sum'})

    df_with_brand_pg_sum = pd.merge(df, daily_brand_pg_sum, how='left', on=['Market Place', 'Year', 'Month', 'Day', 'Brand', 'Product Group'])
    df_with_brand_pg_sum['Portion'] = df_with_brand_pg_sum['Qty'] / df_with_brand_pg_sum['Category Sum']

    # this is because the self-generated dummy data would break the code
    df_with_brand_pg_sum = df_with_brand_pg_sum.replace([np.inf, -np.inf], np.nan)

    df_with_brand_pg_sum.fillna(0, inplace=True)

    return df_with_brand_pg_sum[['Cin7', 'Market Place', 'Year', 'Month', 'Day', 'Portion']]


def reallocate_ppc_qty(ppc_organic, sales_ppc, portion):
    ppc_organic = pd.merge(ppc_organic, portion,
                           how='left',
                           on=['Cin7', 'Market Place', 'Year', 'Month', 'Day'])
    ppc_organic = pd.merge(ppc_organic, sales_ppc,
                           how='left',
                           on=['Market Place', 'Year', 'Month', 'Day', 'Brand', 'Product Group'])
    ppc_organic['PPC Orders'] = ppc_organic['PPC Orders'] * ppc_organic['Portion']
    ppc_organic.fillna(0, inplace=True)

    ppc_organic['Organic Orders'] = ppc_organic['Qty'] - ppc_organic['PPC Orders']
    ppc_organic = ppc_organic.round()

    return ppc_organic[['Cin7', 'Market Place', 'Year', 'Month', 'Day',
                        'Qty', 'Price/Qty', 'PPC Orders', 'Organic Orders']]


def summarize_by_sales_type(df, cin7_product_map, salesType):
    summarized = match_cin7_product(df, cin7_product_map)
    qty_sum = summarized.groupby([
        'Brand', 'Market Place', 'Product Group', 'Cin7', 'Year', 'Month'
    ])['Qty'].sum()
    price_avg = summarized.groupby([
        'Brand', 'Market Place', 'Product Group', 'Cin7', 'Year', 'Month'
    ])['Price/Qty'].mean()

    summarized = pd.concat([qty_sum, price_avg], axis=1).reset_index()\
        .rename(columns={'Qty': 'Sales QTY', 'Price/Qty': 'Avg Sale Price'})
    summarized['Revenue'] = summarized['Sales QTY'] * summarized['Avg Sale Price']

    summarized['Sales Type'] = salesType
    return summarized


def main():
    load_dotenv()
    authenticate_google_sheets()

    order_files = glob.glob('ORDERS*.csv')
    stock_out_files = glob.glob('INVENTORY*.csv')

    cin7_product = get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Cin7-Product-Map')
    asin_cin7 = get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-ASIN-Cin7-Map')
    liquidation_limit = parse_liquidation_limits(
        get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Liquidation-Limits')
    )
    promotions = parse_historical_table(
        get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Historical-Promotions')
    )
    shopify = parse_historical_table(
        get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Historical-Shopify')
    )
    wholesale = parse_historical_table(
        get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Historical-Wholesale')
    )

    out_of_stock = read_out_of_stock_csv(stock_out_files)
    out_of_stock = match_asin_cin7(out_of_stock, asin_cin7)

    orders = read_orders_csv(order_files)
    orders = match_asin_cin7(orders, asin_cin7)
    orders = orders[['Cin7', 'Year', 'Month', 'Day', 'Market Place', 'Sales Channel',
                     'Qty', 'Price', 'Price/Qty']]
    orders_amazon = orders[orders['Sales Channel'] != 'Non-Amazon']
    orders_non_amazon = orders[orders['Sales Channel'] == 'Non-Amazon']

    liquidation_orders = get_liquidation_orders(orders_amazon, liquidation_limit)

    calc_historical_total_sales = calculate_historical_table(orders)
    calc_historical_liquidation = calculate_historical_table(liquidation_orders)
    calc_historical_non_amazon = calculate_historical_table(orders_non_amazon)
    calc_historical_amazon = calculate_historical_table(orders_amazon)

    calc_historical_ppc_organic = pd.merge(calc_historical_amazon, calc_historical_liquidation,
                                           how='left',
                                           on=['Cin7', 'Market Place', 'Year', 'Month', 'Day'],
                                           suffixes=('_amazon', '_liquidation'))
    calc_historical_ppc_organic.fillna(0, inplace=True)
    calc_historical_ppc_organic = pd.merge(calc_historical_ppc_organic, promotions,
                                           how='left',
                                           on=['Cin7', 'Market Place', 'Year', 'Month', 'Day'],
                                           suffixes=('_amazon', '_promotion'))
    calc_historical_ppc_organic.rename(columns={'Qty': 'Qty_promotion', 'Price/Qty': 'Price/Qty_promotion'}, inplace=True)
    calc_historical_ppc_organic.fillna(0, inplace=True)
    calc_historical_ppc_organic['Qty'] = calc_historical_ppc_organic['Qty_amazon'] \
                                         - calc_historical_ppc_organic['Qty_liquidation'] \
                                         - calc_historical_ppc_organic['Qty_promotion']
    calc_historical_ppc_organic['Price/Qty'] = calc_historical_ppc_organic['Price/Qty_amazon']

    calc_historical_ppc_organic = calc_historical_ppc_organic.loc[:,
                                  ~calc_historical_ppc_organic.columns.str.endswith('_amazon')]
    calc_historical_ppc_organic = calc_historical_ppc_organic.loc[:,
                                  ~calc_historical_ppc_organic.columns.str.endswith('_liquidation')]
    calc_historical_ppc_organic = calc_historical_ppc_organic.loc[:,
                                  ~calc_historical_ppc_organic.columns.str.endswith('_promotion')]

    calc_historical_ppc_organic = match_cin7_product(calc_historical_ppc_organic, cin7_product)
    calc_orders_portion = calculate_ppc_portions(calc_historical_ppc_organic)

    sales = read_sales_xlsx('sales.xlsx')
    sales = match_asin_cin7(sales, asin_cin7)
    sales = match_cin7_product(sales, cin7_product)
    sales_ppc = sum_ppc_orders_by_product_group(sales)

    calc_historical_ppc_organic_reallocated = \
        reallocate_ppc_qty(calc_historical_ppc_organic, sales_ppc, calc_orders_portion)
    calc_historical_ppc_reallocated = calc_historical_ppc_organic_reallocated[[
        'Cin7', 'Market Place', 'Year', 'Month', 'Day', 'Price/Qty', 'PPC Orders']]
    calc_historical_ppc_reallocated = calc_historical_ppc_reallocated.rename(columns={'PPC Orders': 'Qty'})
    calc_historical_organic_reallocated = calc_historical_ppc_organic_reallocated[[
        'Cin7', 'Market Place', 'Year', 'Month', 'Day', 'Price/Qty', 'Organic Orders']]
    calc_historical_organic_reallocated = calc_historical_organic_reallocated.rename(columns={'Organic Orders': 'Qty'})

    sum_liq = summarize_by_sales_type(calc_historical_liquidation, cin7_product, 'Liquidations')
    sum_prom = summarize_by_sales_type(promotions, cin7_product, 'Promotions')
    sum_ppc = summarize_by_sales_type(calc_historical_ppc_reallocated, cin7_product, 'PPC')
    sum_org = summarize_by_sales_type(calc_historical_organic_reallocated, cin7_product, 'Organic')

    sum_shopify = summarize_by_sales_type(shopify, cin7_product, 'Shopify')
    sum_wholesale = summarize_by_sales_type(wholesale, cin7_product, 'Wholesale')

    summarized_output_file = pd.concat([sum_liq, sum_prom, sum_ppc, sum_org,
                                        sum_shopify, sum_wholesale], ignore_index=True)

    summarized_output_file = add_out_of_stock_days(summarized_output_file, out_of_stock)

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_total_sales),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Historical-Total'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_amazon),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Historical-Amazon'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_liquidation),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Historical-Liquidation'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_non_amazon),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Historical-Non-Amazon'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(sales_ppc),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-SUM-PPC-Orders'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_orders_portion),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Orders-portion'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(calc_historical_ppc_organic_reallocated),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Calc-Historical-PPC.Reallocated'
    )

    upload_data_to_sheet(
        format_for_google_sheet_upload(summarized_output_file),
        os.getenv('CALCULATIONS_SPREADSHEET_ID'),
        'Output File'
    )


if __name__ == '__main__':
    main()
