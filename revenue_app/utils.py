from functools import reduce
import numpy as np
import pandas as pd
from random import randint


NUMBER_COLUMNS = [
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
    # 'eb_perc_take_rate',
]


def get_rollups():
    return pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=0, header=1, usecols=range(0, 53))


def get_transactions():
    transactions = pd.read_csv('datasets/transactions.csv').replace(np.nan, '', regex=True)
    if 'sale__eb_tax__epp__1' in transactions.columns:
        transactions.drop('sale__eb_tax__epp__1', axis=1, inplace=True)
    transactions['transaction_created_date'] = pd.to_datetime(
        transactions['transaction_created_date'],
        format="%m/%d/%Y",
    )
    transactions['eventholder_user_id'] = transactions['eventholder_user_id'].apply(str)
    transactions['event_id'] = transactions['event_id'].apply(str)
    return transactions


def get_organizer_sales():
    organizer_sales = pd.read_csv('datasets/organizer_sales.csv').replace(np.nan, '', regex=True)
    if 'organizer_email' in organizer_sales.columns:
        organizer_sales.rename(columns={'organizer_email': 'email'}, inplace=True)
    organizer_sales['event_id'] = organizer_sales['event_id'].apply(str)
    return organizer_sales


def filter_transactions(transactions=None, **kwargs):
    if transactions is None:
        transactions = get_transactions()
    conditions = []
    if kwargs.get('event_id'):
        conditions.insert(
            0,
            transactions['event_id'] == kwargs.get('event_id').strip()
        )
    if kwargs.get('email'):
        conditions.insert(
            0,
            transactions['email'] == kwargs.get('email').strip()
        )
    if kwargs.get('eventholder_user_id') or kwargs.get('organizer_id'):
        conditions.insert(
            0,
            transactions['eventholder_user_id'] ==
            (kwargs.get('eventholder_user_id') or kwargs.get('organizer_id')).strip()
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


def group_transactions(transactions, by):
    time_groupby = {
        'day': 'D',
        'week': 'W',
        'semi-month': 'SMS',  # quincena del 1 al 14 y del 15 a fin de mes, consultar con finanzas
        'month': 'M',
        'quarter': 'Q',  # trimestre
        'year': 'Y',
    }
    custom_groupby = {
        'event_id': ['eventholder_user_id', 'email', 'event_id', 'currency'],
        'eventholder_user_id': ['eventholder_user_id', 'email', 'currency'],
        'email': ['eventholder_user_id', 'email', 'currency'],
        'payment_processor': ['payment_processor', 'currency'],
        'currency': ['currency'],
    }
    if isinstance(by, str):
        if by in time_groupby:
            grouped = transactions.set_index("transaction_created_date").resample(time_groupby[by]).sum().reset_index()
        elif by in custom_groupby:
            grouped = transactions.groupby(custom_groupby[by], as_index=False).sum()
    else:
        grouped = transactions.groupby(by, as_index=False).sum()
    return grouped


def calc_perc_take_rate(transactions):
    transactions['eb_perc_take_rate'] = (
        transactions['sale__gtf_esf__epp'] / transactions['sale__payment_amount__epp'] * 100
    ).round(2).apply(str)
    transactions.eb_perc_take_rate.replace(np.nan, 0.00, regex=True, inplace=True)
    return transactions


def transactions(**kwargs):
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    filtered = filter_transactions(merged, **kwargs)
    if kwargs.get('groupby'):
        filtered = group_transactions(filtered, kwargs.get('groupby'))
    return filtered.round(2)


def get_transactions_event(event_id, **kwargs):
    transactions_event = transactions(event_id=event_id, **kwargs)
    organizers_sales = get_organizer_sales()
    paidtix = organizers_sales[organizers_sales['event_id'] == event_id]['PaidTix']
    total = summarize_dataframe(transactions_event)
    return (transactions_event, paidtix, total)


def get_top_organizers(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['eventholder_user_id', 'email'],
    ).agg({'sale__gtf_esf__epp': sum}).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2)
    top = ordered.head(10)
    return top.reset_index(level=[0, 1])


def get_top_events(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['event_id']
    ).agg({'sale__payment_amount__epp': sum}).sort_values(
        by='sale__payment_amount__epp',
        ascending=False,
    ).round(2)
    top = ordered.head(10)
    return top.reset_index(level=0)


def random_color():
    return f"rgba({randint(0, 255)}, {randint(0, 255)}, {randint(0, 255)}, 0.2)"


def summarize_dataframe(dataframe):
    dic = {}
    for column in dataframe.columns.tolist():
        if column in NUMBER_COLUMNS:
            dic[column] = dataframe[column].sum().round(2)
    return dic


def organizer_details(organizer_id):
    details = {}
    organizer_transactions = transactions()
    organizer_transactions = organizer_transactions[
        organizer_transactions['eventholder_user_id'] == organizer_id
    ].head(1)
    details['email'] = organizer_transactions['email'].to_string(index=False).strip()
    organizer_sales = get_organizer_sales()
    organizer_sales = organizer_sales[organizer_sales['email'] == details['email']]
    details['name'] = organizer_sales.iloc[0]['organizer_name'] if len(organizer_sales) > 0 else ''
    return details
