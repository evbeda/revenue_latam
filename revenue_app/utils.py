from functools import reduce
import numpy as np
import pandas as pd
from random import randint


MONEY_COLUMNS = [
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

NUMBER_COLUMNS = [
    'PaidTix',
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


def clean_transactions(transactions):
    transactions = transactions.replace(np.nan, '', regex=True)
    transactions['transaction_created_date'] = pd.to_datetime(
        transactions['transaction_created_date'],
    )
    transactions['eventholder_user_id'] = transactions['eventholder_user_id'].apply(str)
    transactions['event_id'] = transactions['event_id'].apply(str)
    transactions[MONEY_COLUMNS] = transactions[MONEY_COLUMNS].astype(float)
    return transactions


def clean_corrections(corrections):
    corrections = corrections.replace(np.nan, '', regex=True)
    corrections['transaction_created_date'] = pd.to_datetime(
        corrections['transaction_created_date'],
    )
    corrections['eventholder_user_id'] = corrections['eventholder_user_id'].apply(str)
    corrections['event_id'] = corrections['event_id'].apply(str)
    corrections[MONEY_COLUMNS] = corrections[MONEY_COLUMNS].astype(float)
    return corrections


def clean_organizer_sales(organizer_sales):
    organizer_sales = organizer_sales.replace(np.nan, '', regex=True)
    if 'organizer_email' in organizer_sales.columns:
        organizer_sales.rename(columns={'organizer_email': 'email'}, inplace=True)
    if 'trx_date' in organizer_sales.columns:
        organizer_sales.rename(columns={'trx_date': 'transaction_created_date'}, inplace=True)
    organizer_sales['transaction_created_date'] = pd.to_datetime(
        organizer_sales['transaction_created_date'],
    )
    organizer_sales['event_id'] = organizer_sales['event_id'].apply(str)
    organizer_sales[['GTSntv', 'GTFntv']] = organizer_sales[['GTSntv', 'GTFntv']].astype(float)
    organizer_sales = organizer_sales[organizer_sales['PaidTix'] != 0]
    return organizer_sales


def clean_organizer_refunds(organizer_refunds):
    organizer_refunds = organizer_refunds.replace(np.nan, '', regex=True)
    if 'organizer_email' in organizer_refunds.columns:
        organizer_refunds.rename(columns={'organizer_email': 'email'}, inplace=True)
    if 'trx_date' in organizer_refunds.columns:
        organizer_refunds.rename(columns={'trx_date': 'transaction_created_date'}, inplace=True)
    organizer_refunds['transaction_created_date'] = pd.to_datetime(
        organizer_refunds['transaction_created_date'],
    )
    organizer_refunds['event_id'] = organizer_refunds['event_id'].apply(str)
    organizer_refunds[['GTSntv', 'GTFntv']] = organizer_refunds[['GTSntv', 'GTFntv']].astype(float)
    organizer_refunds = organizer_refunds[organizer_refunds['PaidTix'] != 0]
    return organizer_refunds


def merge_corrections(transactions, corrections):
    trx_total = pd.concat([transactions, corrections])
    trx_total = trx_total.groupby([
        'transaction_created_date',
        'eventholder_user_id',
        'email',
        'event_id',
        'currency',
        'payment_processor',
        'is_refund',
        'is_sale',
    ]).sum().reset_index()
    return trx_total


def filter_transactions(transactions, **kwargs):
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


def merge_transactions(transactions, organizer_sales, organizer_refunds):
    merged = transactions.merge(
        organizer_sales[[
            'email',
            'event_id',
            'event_title',
            'sales_flag',
            'sales_vertical',
            'vertical',
            'sub_vertical',
        ]].drop_duplicates(),
        on=['email', 'event_id'],
        how='left',
    )
    sales = merged[merged['is_sale'] == 1]
    refunds = merged[merged['is_refund'] == 1]
    merged_sales = sales.merge(
        organizer_sales[[
            'transaction_created_date',
            'email',
            'event_id',
            'PaidTix',
        ]].drop_duplicates(),
        on=['transaction_created_date', 'email', 'event_id'],
        how='left',
    )
    merged_refunds = refunds.merge(
        organizer_refunds[[
            'transaction_created_date',
            'email',
            'event_id',
            'PaidTix',
        ]].drop_duplicates(),
        on=['transaction_created_date', 'email', 'event_id'],
        how='left',
    )
    merged_final = pd.concat([merged_sales, merged_refunds])
    merged_final.sort_values(
        by=['transaction_created_date', 'eventholder_user_id', 'event_id'],
        inplace=True,
    )
    merged_final.PaidTix.replace(np.nan, 0, regex=True, inplace=True)
    merged_final['PaidTix'] = merged_final['PaidTix'].astype(int)
    merged_final.replace(np.nan, 'n/a', regex=True, inplace=True)
    return merged_final.drop(columns=['is_sale', 'is_refund'])


def group_transactions(transactions, by):
    time_groupby = {
        'day': 'D',
        'week': 'W',
        'semi_month': 'SMS',  # quincena del 1 al 14 y del 15 a fin de mes, consultar con finanzas
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
    ).round(2)
    transactions.eb_perc_take_rate.replace(np.nan, 0.00, regex=True, inplace=True)
    return transactions


def manage_transactions(transactions, corrections, organizer_sales, organizer_refunds, **kwargs):
    transactions = clean_transactions(transactions)
    corrections = clean_corrections(corrections)
    trx_total = merge_corrections(transactions, corrections)
    organizers_sales = clean_organizer_sales(organizer_sales)
    organizers_refunds = clean_organizer_refunds(organizer_refunds)
    merged = merge_transactions(trx_total, organizers_sales, organizers_refunds)
    merged = calc_perc_take_rate(merged)
    filtered = filter_transactions(merged, **kwargs)
    if kwargs.get('groupby'):
        filtered = group_transactions(filtered, kwargs.get('groupby'))
    return filtered.round(2)


def event_details(event_id, eventholder_user_id, organizer_sales):
    organizer_sales = clean_organizer_sales(organizer_sales)
    organizer_sales = organizer_sales[organizer_sales['event_id'] == event_id]
    details = {
        'Event ID': event_id,
        'Event Title': organizer_sales.iloc[0]['event_title'] if len(organizer_sales) > 0 else '',
        'Organizer ID': eventholder_user_id,
        'Organizer Name': organizer_sales.iloc[0]['organizer_name'] if len(organizer_sales) > 0 else '',
        'Email': organizer_sales.iloc[0]['email'] if len(organizer_sales) > 0 else '',
    }
    return details


def summarize_dataframe(dataframe):
    return {
        column: dataframe[column].sum().round(2)
        for column in dataframe.columns.tolist()
        if column in NUMBER_COLUMNS
    }


def get_event_transactions(transactions, corrections, organizer_sales, organizer_refunds, event_id, **kwargs):
    event_transactions = manage_transactions(
        transactions,
        corrections,
        organizer_sales,
        organizer_refunds,
        event_id=event_id
    )
    filtered = filter_transactions(event_transactions, **kwargs)
    event_total = summarize_dataframe(filtered)
    if len(event_transactions) > 0:
        details = event_details(
            event_id,
            event_transactions.iloc[0]['eventholder_user_id'],
            organizer_sales,
        )
        details['PaidTix'] = event_total['PaidTix']
        details['AVG Ticket Value'] = round(event_total['sale__payment_amount__epp'] / event_total['PaidTix'], 2) \
            if event_total['PaidTix'] > 0 else 0
        details['AVG PaidTix/Day'] = round(event_total['PaidTix'] / len(filtered), 2) \
            if len(filtered) > 0 else 0
    sales_refunds = {
        'Total Sales Detail': {
            k: v for k, v in event_total.items() if 'sale' in k
        },
        'Total Refunds Detail': {
            k: v for k, v in event_total.items() if 'refund' in k
        },
    }
    return filtered, details, sales_refunds


def get_organizer_transactions(
    transactions,
    corrections,
    organizer_sales,
    organizer_refunds,
    eventholder_user_id,
    **kwargs
):
    organizer_transactions = manage_transactions(
        transactions,
        corrections,
        organizer_sales,
        organizer_refunds,
        eventholder_user_id=eventholder_user_id,
    )
    filtered = filter_transactions(organizer_transactions, **kwargs)
    event_total = summarize_dataframe(filtered)
    organizer_sales = clean_organizer_sales(organizer_sales)
    if len(organizer_transactions) > 0:
        details = {
            'Organizer ID': eventholder_user_id,
            'Organizer Name': organizer_sales.iloc[0]['organizer_name'] if len(organizer_sales) > 0 else '',
            'Email': organizer_transactions.iloc[0]['email'],
            'PaidTix': event_total['PaidTix'],
            'AVG Ticket Value': round(event_total['sale__payment_amount__epp'] / event_total['PaidTix'], 2)
            if event_total['PaidTix'] > 0 else 0,
            'AVG PaidTix/Day':  round(event_total['PaidTix'] / len(group_transactions(filtered, 'day')), 2)
            if len(group_transactions(filtered, 'day')) > 0 else 0,
        }
    sales_refunds = {
        'Total Sales Detail': {
            k: v for k, v in event_total.items() if 'sale' in k
        },
        'Total Refunds Detail': {
            k: v for k, v in event_total.items() if 'refund' in k
        },
    }
    return filtered, details, sales_refunds


def get_top_organizers(filtered_transactions):
    ordered = filtered_transactions.groupby(
        ['eventholder_user_id', 'email'],
    ).agg({
        'sale__payment_amount__epp': sum,
        'sale__gtf_esf__epp': sum,
    }).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2).reset_index()
    ordered = calc_perc_take_rate(ordered)
    top = ordered.head(10).copy()
    top.loc[len(top), ['email', 'sale__gtf_esf__epp', 'sale__payment_amount__epp']] = [
        'Others',
        ordered[10:].sale__gtf_esf__epp.sum().round(2),
        ordered[10:].sale__payment_amount__epp.sum().round(2),
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
        ['event_id', 'event_title', 'eventholder_user_id', 'email'],
    ).agg({
        'sale__gtf_esf__epp': sum,
        'sale__payment_amount__epp': sum,
        'eb_perc_take_rate': 'mean',
    }).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2).reset_index()
    top = ordered.head(10).copy()
    top.loc[len(top), ['event_title', 'event_id', 'sale__gtf_esf__epp', 'sale__payment_amount__epp']] = [
        'Others',
        'Others',
        ordered[10:].sale__gtf_esf__epp.sum().round(2),
        ordered[10:].sale__payment_amount__epp.sum().round(2),
    ]
    return top


def random_color():
    return f"rgba({randint(0, 255)}, {randint(0, 255)}, {randint(0, 255)}, 0.2)"


def get_summarized_data(transactions, corrections, organizer_sales, organizer_refunds):
    trx = manage_transactions(transactions, corrections, organizer_sales, organizer_refunds)
    currencies = ['ARS', 'BRL']
    summarized_data = {}
    for currency in currencies:
        filtered = trx[trx['currency'] == currency]
        summarized_data[currency] = {
            'Total Organizers': filtered.eventholder_user_id.nunique(),
            'Total Events': filtered.event_id.nunique(),
            'Total PaidTix': filtered.PaidTix.sum(),
            'Total GTF': round(filtered.sale__gtf_esf__epp.sum(), 2),
            'Total GTV': round(filtered.sale__payment_amount__epp.sum(), 2),
            'ATV': round(
                (filtered.sale__payment_amount__epp.sum() - filtered.sale__gtf_esf__epp.sum()) / filtered.PaidTix.sum(),
                2,
            ),
            'Avg EB Perc Take Rate': round(
                filtered.sale__gtf_esf__epp.sum() / filtered.sale__payment_amount__epp.sum() * 100,
                2,
            ),
        }
    return summarized_data


def payment_processor_summary(transactions):
    transactions = transactions.copy()
    transactions.payment_processor.replace('', 'n/a', regex=True, inplace=True)
    pp_gtv = transactions.groupby(['payment_processor']).agg({'sale__payment_amount__epp': sum}).reset_index().round(2)
    pp_gtv = pp_gtv[pp_gtv['sale__payment_amount__epp'] != 0]
    pp_gtf = transactions.groupby(['payment_processor']).agg({'sale__gtf_esf__epp': sum}).reset_index().round(2)
    pp_gtf = pp_gtf[pp_gtf['sale__gtf_esf__epp'] != 0]
    return pp_gtv, pp_gtf


def sales_flag_summary(transactions):
    sf_organizer = transactions.groupby(
        ['eventholder_user_id', 'sales_flag']
    ).sum().reset_index().sales_flag.value_counts()
    sf_gtf = transactions.groupby(['sales_flag']).agg({'sale__gtf_esf__epp': sum}).reset_index().round(2)
    return sf_organizer, sf_gtf
