import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from parser import *
from gservice import *


def generate_liquidation_limits(cin7_df, start_date_string, end_date_string):
    liquidation_limits = pd.DataFrame(columns=['Cin7', 'Year', 'Month', 'Liquidation Limit'])
    iterator_date = datetime.strptime(start_date_string, '%Y.%m.%d.')
    end_date = datetime.strptime(end_date_string, '%Y.%m.%d.')

    while iterator_date < end_date:
        df = cin7_df[['Cin7']]
        df['Year'] = iterator_date.strftime('%Y')
        df['Month'] = iterator_date.strftime('%B')
        df['Liquidation Limit'] = 0.2
        df['Normal Price'] = 29.97
        liquidation_limits = liquidation_limits.append(df, ignore_index=True)
        iterator_date += relativedelta(months=1)

    return liquidation_limits[['Cin7', 'Year', 'Month', 'Normal Price', 'Liquidation Limit']]


def main():
    load_dotenv()
    cin7_product = get_data_from_spreadsheet(os.getenv('INPUT_SPREADSHEET_ID'), 'Input-Cin7-Product-Map')
    liquidation_limits = generate_liquidation_limits(cin7_product, '2017.01.01.', '2020.01.31.')

    upload_data_to_sheet(
        format_for_google_sheet_upload(liquidation_limits),
        os.getenv('INPUT_SPREADSHEET_ID'),
        'Input-Liquidation-Limits'
    )


if __name__ == '__main__':
    main()
