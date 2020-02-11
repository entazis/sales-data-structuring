# Sales data restructuring

Clean the raw data to a visualizable format.

## Getting Started

Clone this project to use.

### Prerequisites

You will need Python3.5 to run the project.

```
python3 sales_forecast.py
```

### Installing

Copy the local.env file and rename it to .env
Fill the ids for the input and calculations google sheets for development:

```
INPUT_SPREADSHEET_ID=''
CALCULATIONS_SPREADSHEET_ID=''
```

Get the following raw data files following this naming convention, put them next to the sales_forcaster.py file:

```
INVENTORY-...-... MM YYYY stock outdays.csv
ORDERS-...-... .csv
SALESPERDAY...-...-... .xlsx

```

## Authors

* **Bence Szabó** - *Initial work* - [Entazis](https://github.com/Entazis)
