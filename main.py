import argparse
import datetime as dt
import json
import os
import pandas as pd
import requests
import time
from urllib.parse import urlencode


# Command line parser
parser = argparse.ArgumentParser(description='Update historical exchange rates', exit_on_error=True)
parser.add_argument(
    '-s', 
    '--start_date', 
    action='store', 
    default=(dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d'),
    required=False,
    type=str, 
    help='Date from which this helper will start pulling historical data,m inclusive. Default is yesterday.'
)
parser.add_argument(
    '-e', 
    '--end_date', 
    action='store', 
    default=dt.datetime.now().strftime('%Y-%m-%d'),
    required=False,
    type=str, 
    help='Date from which this helper will stop pulling historical data,m inclusive. Default is today.'
)
parser.add_argument(
    '-t', 
    '--targets', 
    action='store', 
    default='CAD,EUR,HKD,PHP',
    required=False,
    type=str, 
    help='Currency codes to pull the historical data for. Default includes Canada, Euro, HK Dollar, and Phillipino Peso.'
)
args = parser.parse_args()


def validate_inputs(args: argparse.Namespace) -> argparse.Namespace:
    """Validate the inputs from the commmand line
    
    Args:
        args (argparse.Namespace): The arguments from the command line

    Returns:
        argparse.Namespace: The validated arguments
    """
    args = vars(args)
    # Validate date types
    if isinstance(args.get('start_date'), str):
        start_date: dt.datetime = dt.datetime.strptime(args.get('start_date'), '%Y-%m-%d')

    if isinstance(args.get('end_date'), str):
        end_date: dt.datetime = dt.datetime.strptime(args.get('end_date'), '%Y-%m-%d')
        
    # Validate the start date is before the end date. Set default otherwise
    if start_date >= end_date:
        print('The historical start date must be before the historical end date. Setting the start date to 1 day before the historical end date')
        start_date = dt.strptime(end_date - dt.timedelta(days=1), '%Y-%m-%d')

    # Reassign the validated dates
    args['start_date'] = start_date
    args['end_date'] = end_date
    return args
    
def create_set_of_dates(start_date: dt.datetime, end_date: dt.datetime) -> set[str]:
    """Create a set of dates between the start and end dates, inclusive, to iterate through
    while making the API calls.

    Args:
        start_date (dt.datetime): The start date
        end_date (dt.datetime): The end date

    Returns:
        set[str]: A set of dates between the start and end dates, inclusive
    """
    dates = []
    while start_date <= end_date:
        dates.append(start_date.strftime('%Y-%m-%d'))
        start_date += dt.timedelta(days=1)
    return set(dates)

def return_csv_indicies(filename: str) -> tuple[pd.Index, pd.Index]:
    """Read the row and column indicies to check for new currency targets and dates
    
    Args:
        filename (str): The name of the file to read
    
    Returns:
        tuple[pd.Index, pd.Index]: The row and column indicies
    """
    df = pd.read_csv(filename, 
                     sep=',',
                     header=0,
                     index_col=0,
                )
    return df.index, df.columns

def validate_targets(targets: set, row_index: set) -> set[str]:
    """Compare the default targets and/or input targets to what is in the CSV file.
    Return a warning if new targets have been found.
    
    Args:
        targets (set): The targets to validate
        row_index (pd.Index): The row index from the CSV file

    Returns:
        None. Print warning if new targets have been added
    """
    if new_targets := {t for t in targets if t not in row_index}:
        print(f"Warnihg: New targets have been added and will need to be backfilled: {new_targets}")
    return new_targets

def validate_dates(date_range: set, col_index: set) -> set[str]:
    """Compare the default dates and/or input dates to what is in the CSV file.
    Pull historical data only for the new dates.

    Args:
        date_range (set): The dates to validate
        col_index (pd.Index): The column index from the CSV file

    Returns:
        set: The new dates to pull historical data for
    """
    if new_dates := {d for d in date_range if d not in col_index}:
        print(f"Pulling historical data for the following dates: {new_dates}")
    return new_dates

def aggregate_historical_currency_data(base: str, date_range: set, *, targets: set) -> pd.DataFrame:
    """Create an iterator that controls the API calls to the endpoint. Since the API allows us to retrieve data for one day
    at a time, we're iterating through a date range and a generator function to combine all of the data.
    
    Args:
        base (str): The base currency
        date_range (set): The date range to pull historical data for
        targets (set): The targets to pull historical data for

    Returns:
        pd.DataFrame: The combined historical exchange rates
    """
    targets: str = ','.join(targets)
    df: pd.DataFrame = pd.DataFrame()
    for dr in sorted(list(date_range)):
        temp_df = next(get_historical_exchange_rates(base, dr, targets))
        df = pd.concat([df, temp_df], axis=1, ignore_index=False)
    return df

def get_historical_exchange_rates(base: str, date: dt.datetime, targets: str) -> pd.DataFrame:
    """Generator function to pull historical exchange rate for a given date and currency codes
    
    Args:
        base (str): The base currency
        date (dt.datetime): The date to pull historical data for
        targets (str): The currency codes to pull historical data for

    Raises:
        requests.ClientError: If there is an error with the request

    Yields:
        pd.DataFrame: The historical exchange rates for a single date
    """
    try:
        url = 'https://exchange-rates.abstractapi.com/v1/historical'
        params = dict(
                api_key=os.environ['ABSTRACTAPI_API_KEY'],
                base= base,
                date=date,
                target=targets
            )
        print(f"Sending request to {url}?{urlencode(params)}")
        response = requests.request(
            "GET", 
            url=url, 
            params=params, 
            timeout=30
        )
    except requests.ClientError as e:
        raise e
    else:
        response = json.loads(response.text)
        rates = response['exchange_rates']
        df = pd.DataFrame.from_dict(rates, orient='index', columns=[f"{date}"])
        time.sleep(3)
    yield df

def concatenate_dfs(existing_csv: str, new_df: pd.DataFrame):
    """Concatenate the existing CSV file with the new historical exchange rates
    
    Args:
        existing_csv (str): The existing CSV file
        new_df (pd.DataFrame): The new historical exchange rates

    Returns:
        pd.DataFrame: The concatenated historical exchange rates
    """
    existing = pd.read_csv(existing_csv,
                        sep=',',
                        header=0,
                        index_col=0,
                    )
    return pd.concat([existing, new_df], axis=1)


if __name__ == "__main__":
    args = validate_inputs(args)
    start = time.perf_counter()
    print(f"Starting to pull historical data for {args.get('targets')} between the dates {args.get('start_date')} and {args.get('end_date')}")

    filename: str = 'exchange_rates_table.csv'
    historical_start_date: str = args.get('start_date')
    historical_end_date:str = args.get('end_date')
    date_range: set = create_set_of_dates(historical_start_date, historical_end_date)

    targets: set[str] = set(args.get('targets').split(','))
    row_index, col_index = return_csv_indicies(filename)
    validate_targets(targets, row_index)
    valid_date_range: set = validate_dates(date_range, col_index)

    base_currency = 'USD'
    df_from_api: pd.DataFrame = aggregate_historical_currency_data(base_currency, valid_date_range, targets=targets)
    final_df: pd.DataFrame = concatenate_dfs('exchange_rates_table.csv', df_from_api)
    final_df.to_csv(filename, sep=',', header=True, index=True, encoding='utf-8')
    print("Job complete. CSV file updated.")
    print(f"Finished the job in {time.perf_counter() - start}s.")
