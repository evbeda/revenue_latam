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
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    'refund__ap_organizer__royalty__epp',
    # 'eb_perc_take_rate',
]


def get_rollups():
    return pd.read_excel('datasets/Revenue Queries.xlsx', sheet_name=0, header=1, usecols=range(0, 53))


def get_transactions():
    transactions = pd.read_csv('datasets/transactions.csv').replace(np.nan, '', regex=True)
    transactions['transaction_created_date'] = pd.to_datetime(
        transactions['transaction_created_date'],
    )
    transactions['eventholder_user_id'] = transactions['eventholder_user_id'].apply(str)
    transactions['event_id'] = transactions['event_id'].apply(str)
    return transactions


def get_organizer_sales():
    organizer_sales = pd.read_csv('datasets/organizer_sales.csv').replace(np.nan, '', regex=True)
    if 'organizer_email' in organizer_sales.columns:
        organizer_sales.rename(columns={'organizer_email': 'email'}, inplace=True)
    if 'trx_date' in organizer_sales.columns:
        organizer_sales.rename(columns={'trx_date': 'transaction_created_date'}, inplace=True)
        organizer_sales['transaction_created_date'] = pd.to_datetime(
            organizer_sales['transaction_created_date'],
        )
    organizer_sales['event_id'] = organizer_sales['event_id'].apply(str)
    return organizer_sales


def filter_transactions(transactions=None, **kwargs):
    if transactions is None:
        transactions = get_transactions()
    conditions = [
        transactions[key] == kwargs.get(key).strip()
        for key in kwargs
        if key in ['event_id', 'email', 'currency', 'eventholder_user_id'] and kwargs.get(key)
    ]
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
        organizer_sales[[
            'transaction_created_date',
            'email',
            'event_id',
            'event_title',
            'sales_flag',
            'sales_vertical',
            'vertical',
            'sub_vertical',
            'PaidTix',
        ]].drop_duplicates(),
        on=['transaction_created_date', 'email', 'event_id'],
        how='left',
    )
    merged.PaidTix.replace(np.nan, 0, regex=True, inplace=True)
    merged['PaidTix'] = merged['PaidTix'].astype(int)
    merged.replace(np.nan, 'n/a', regex=True, inplace=True)
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
        'event_id': ['eventholder_user_id', 'email', 'event_id', 'event_title', 'currency'],
        'eventholder_user_id': ['eventholder_user_id', 'email', 'currency'],
        'email': ['eventholder_user_id', 'email', 'currency'],
        'payment_processor': ['payment_processor', 'currency'],
        'sales_flag': ['sales_flag', 'currency'],
        'sales_vertical': ['sales_vertical', 'currency'],
        'vertical': ['vertical', 'currency'],
        'sub_vertical': ['vertical', 'sub_vertical', 'currency'],
        'currency': ['currency'],
    }
    if isinstance(by, str):
        if by in time_groupby:
            grouped = transactions.set_index("transaction_created_date").groupby(
                ['currency', pd.Grouper(freq=time_groupby[by])]
            ).sum().reset_index()
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


def calc_gtv(transactions):
    transactions['gtv'] = transactions['sale__gtf_esf__epp'] + transactions['sale__ap_organizer__gts__epp']
    transactions.gtv.replace(np.nan, 0.00, regex=True, inplace=True)
    return transactions


def transactions(**kwargs):
    transactions = get_transactions()
    organizers_sales = get_organizer_sales()
    merged = merge_transactions(transactions, organizers_sales)
    merged = calc_perc_take_rate(merged)
    merged = calc_gtv(merged)
    filtered = filter_transactions(merged, **kwargs)
    if kwargs.get('groupby'):
        filtered = group_transactions(filtered, kwargs.get('groupby'))
    return filtered.round(2)


# TODO: refactor using the new query result
def get_transactions_event(event_id, **kwargs):
    transactions_event = transactions(event_id=event_id, **kwargs)
    organizers_sales = get_organizer_sales()
    paidtix = organizers_sales[organizers_sales['event_id'] == event_id]['PaidTix']
    total = summarize_dataframe(transactions_event)
    return (transactions_event, paidtix, total)


def get_top_organizers(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['eventholder_user_id', 'email', 'eb_perc_take_rate'],
    ).agg({
        'sale__gtf_esf__epp': sum,
        'gtv': sum,
    }).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2).reset_index()
    top = ordered.head(10).copy()
    top.loc[len(top), ['email', 'sale__gtf_esf__epp', 'gtv']] = [
        'Others',
        ordered[10:].sale__gtf_esf__epp.sum().round(2),
        ordered[10:].gtv.sum().round(2),
    ]
    return top

def get_top_organizers_refunds(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['eventholder_user_id', 'email'],
    ).agg({
        'refund__gtf_epp__gtf_esf__epp': sum,
    }).sort_values(
        by='refund__gtf_epp__gtf_esf__epp',
        ascending=True,
    ).round(2).reset_index()
    top = ordered.head(10).copy()
    top.loc[len(top), ['email', 'refund__gtf_epp__gtf_esf__epp']] = [
        'Others',
        ordered[10:].refund__gtf_epp__gtf_esf__epp.sum().round(2),
    ]
    return top


def get_top_events(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['event_id', 'event_title', 'eventholder_user_id', 'email', 'eb_perc_take_rate'],
    ).agg({
        'sale__gtf_esf__epp': sum,
        'gtv': sum,
    }).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2).reset_index()
    top = ordered.head(10).copy()
    top.loc[len(top), ['event_title', 'event_id', 'sale__gtf_esf__epp', 'gtv']] = [
        'Others',
        'Others',
        ordered[10:].sale__gtf_esf__epp.sum().round(2),
        ordered[10:].gtv.sum().round(2),
    ]
    return top


def random_color():
    return f"rgba({randint(0, 255)}, {randint(0, 255)}, {randint(0, 255)}, 0.2)"


def summarize_dataframe(dataframe):
    dic = {}
    for column in dataframe.columns.tolist():
        if column in NUMBER_COLUMNS:
            dic[column] = dataframe[column].sum().round(2)
    return dic


def organizer_details(eventholder_user_id):
    details = {}
    organizer_transactions = transactions()
    organizer_transactions = organizer_transactions[
        organizer_transactions['eventholder_user_id'] == eventholder_user_id
    ].head(1)
    details['email'] = organizer_transactions['email'].to_string(index=False).strip()
    organizer_sales = get_organizer_sales()
    organizer_sales = organizer_sales[organizer_sales['email'] == details['email']]
    details['name'] = organizer_sales.iloc[0]['organizer_name'] if len(organizer_sales) > 0 else ''
    return details


def get_summarized_data():
    trx = transactions()
    currencies = ['ARS', 'BRL']
    summarized_data = {}
    for currency in currencies:
        filtered = trx[trx['currency'] == currency]
        summarized_data[currency] = {
            'Total Organizers': filtered.eventholder_user_id.nunique(),
            'Total Events': filtered.event_id.nunique(),
            'Total PaidTix': filtered.PaidTix.sum(),
            'Total GTF': round(filtered.sale__gtf_esf__epp.sum(), 2),
            'Total GTV': round(filtered.gtv.sum(), 2),
            'ATV': round(filtered.sale__ap_organizer__gts__epp.sum() / filtered.PaidTix.sum(), 2),
            'Avg EB Perc Take Rate': round(filtered.eb_perc_take_rate.apply(float).mean(), 2),
        }
    return summarized_data
