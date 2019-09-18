import numpy as np
import pandas as pd


def get_rollups():
    return pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=0, header=1, usecols=range(0, 53))


def get_transactions():
    transactions = pd.read_csv('datasets/transactions.csv').replace(np.nan, '', regex=True)
    if 'sale__eb_tax__epp__1' in transactions.columns:
        transactions.drop('sale__eb_tax__epp__1', axis=1, inplace=True)
    transactions['transaction_created_date'] = pd.to_datetime(
        transactions['transaction_created_date'],
        format="%m/%d/%Y"
    )
    return transactions


def get_organizer_sales():
    organizer_sales = pd.read_csv('datasets/organizer_sales.csv').replace(np.nan, '', regex=True)
    if 'organizer_email' in organizer_sales.columns:
        organizer_sales.rename(columns={'organizer_email': 'email'}, inplace=True)
    return organizer_sales


def get_organizer_list():
    return []

def get_organizer_event_list(organizer_id):
    transactions = get_transactions()
    return transactions[transactions['eventholder_user_id'] == int(organizer_id)]


def filter_transactions_by_date(dataframe, start_date, end_date=None):
    start_date = np.datetime64(start_date, 'D')
    filter_condition = (dataframe['transaction_created_date'] == start_date)
    if end_date:
        end_date = np.datetime64(end_date, 'D')
        filter_condition = (dataframe['transaction_created_date'] >= start_date) & \
            (dataframe['transaction_created_date'] <= end_date)
    return dataframe[filter_condition]
