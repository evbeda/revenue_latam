from functools import reduce
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


def filter_transactions(transactions=None, **kwargs):
    if transactions is None:
        transactions = get_transactions()
    conditions = []
    if kwargs.get('event_id'):
        conditions.insert(
            0,
            transactions['event_id'] == int(kwargs.get('event_id'))
        )
    if kwargs.get('email'):
        conditions.insert(
            0,
            transactions['email'] == kwargs.get('email')
        )
    if kwargs.get('eventholder_user_id') or kwargs.get('organizer_id'):
        conditions.insert(
            0,
            transactions['eventholder_user_id'] == int(
                kwargs.get('eventholder_user_id') or kwargs.get('organizer_id')
            )
        )
    if kwargs.get('start_date'):
        start_date = np.datetime64(kwargs['start_date'], 'D')
        if kwargs.get('end_date'):
            end_date = np.datetime64(kwargs['end_date'], 'D')
            conditions.insert(
                0,
                transactions['transaction_created_date'] >= start_date
            )
            conditions.insert(
                0,
                transactions['transaction_created_date'] <= end_date
            )
        else:
            conditions.insert(
                0,
                transactions['transaction_created_date'] == start_date
            )
    if not conditions:
        return transactions
    return transactions[reduce(np.logical_and, conditions)]


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


def transactions(**kwargs):
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    return filter_transactions(merged, **kwargs)


def get_transactions_event(event_id):
    transactions_event = transactions(event_id=event_id)
    organizers_sales = get_organizer_sales()
    paidtix = organizers_sales[organizers_sales['event_id'] == int(event_id)]['PaidTix']
    return (transactions_event, paidtix)


def get_dates():
    transactions = get_transactions()
    return np.datetime_as_string(
        pd.to_datetime(
            transactions['transaction_created_date'],
            format="%m/%d/%Y",
        ).sort_values().unique(),
        unit='D',
    ).tolist()


def get_top_organizers(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['eventholder_user_id', 'email'],
    ).agg({'sale__payment_amount__epp': sum}).sort_values(
        by='sale__payment_amount__epp',
        ascending=False,
    )
    top = ordered.head(10)
    return top.reset_index(level=[0, 1])

def get_top_events(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['event_id']
    ).agg({'sale__payment_amount__epp': sum}).sort_values(
    by='sale__payment_amount__epp', ascending=False)
    top = ordered.head(10)
    return top.reset_index(level=0)
