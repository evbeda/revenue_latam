import numpy as np
import pandas as pd

FULL_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    # Vertical (not found yet)
    # Subvertical (not found yet)
    'event_id',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]

ORGANIZER_FILTER_COLUMNS = [
    'transaction_created_date',
    'payment_processor',
    'currency',
    'event_id',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
]

DATE_FILTER_COLUMNS = [
    'transaction_created_date',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    'event_id',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]


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


def merge_transactions(transactions, organizer_sales):
    merged = transactions.merge(
        organizer_sales[['email', 'sales_flag']].drop_duplicates(),
        on=['email'],
        how='left',
    )
    merged.replace(np.nan, 'unknown', regex=True, inplace=True)
    merged.sort_values(
        by=['transaction_created_date', 'eventholder_user_id', 'event_id'],
        inplace=True,
    )
    return merged


def calc_perc_take_rate(transactions):
    transactions['eb_perc_take_rate'] = \
        transactions['sale__gtf_esf__epp'] / transactions['sale__payment_amount__epp'] * 100
    transactions.eb_perc_take_rate.replace('unknown', 0, regex=True, inplace=True)
    return transactions


def get_organizers_transactions():
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    return merged[FULL_COLUMNS]


def get_organizer_transactions(eventholder_user_id):
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    organizer_transactions = merged[merged['eventholder_user_id'] == int(eventholder_user_id)]
    return organizer_transactions[ORGANIZER_FILTER_COLUMNS]


def get_transactions_by_date(start_date, end_date):
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    return filter_transactions_by_date(merged, start_date, end_date)


def get_dates():
    transactions = get_transactions()
    return np.datetime_as_string(
        pd.to_datetime(
            transactions['transaction_created_date'],
            format="%m/%d/%Y",
        ).sort_values().unique(),
        unit='D',
    ).tolist()
