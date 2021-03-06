from functools import reduce
import numpy as np
import pandas as pd

from revenue_app.const import (
    ARS,
    BRL,
    USD,
)


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

NET_COLUMNS = [
    'net__payment_amount__epp',
    'net__gtf_esf__epp',
    'net__eb_tax__epp',
    'net__ap_organizer__gts__epp',
    'net__ap_organizer__royalty__epp',
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


def merge_transactions(transactions, organizer_sales, organizer_refunds):
    sales = transactions[transactions['is_sale'] == 1]
    refunds = transactions[transactions['is_refund'] == 1]

    sales = sales.merge(
        organizer_sales[[
            'email',
            'organizer_name',
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
    refunds = refunds.merge(
        organizer_refunds[[
            'email',
            'organizer_name',
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


def calc_perc_take_rate(transactions):
    transactions['eb_perc_take_rate'] = (
        transactions['sale__gtf_esf__epp'] / transactions['sale__payment_amount__epp'] * 100
    ).round(2)
    transactions.eb_perc_take_rate.replace(np.nan, 0.00, regex=True, inplace=True)
    return transactions


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


def group_transactions(transactions, by):
    time_groupby = {
        'day': 'D',
        'week': 'W',
        'semi_month': 'SMS',  # quincena del 1 al 14 y del 15 a fin de mes, consultar con finanzas
        'month': 'M',
        'quarter': 'Q',  # trimestre
    }
    custom_groupby = {
        'event_id': ['eventholder_user_id', 'email', 'event_id', 'event_title', 'currency'],
        'eventholder_user_id': ['eventholder_user_id', 'email', 'currency'],
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
    columns_to_drop = [column for column in grouped.columns if 'local_' in column]
    return grouped.drop(columns_to_drop, axis=1)


def generate_transactions_consolidation(transactions, corrections, organizer_sales, organizer_refunds):
    transactions = clean_transactions(transactions)
    corrections = clean_corrections(corrections)
    trx_total = merge_corrections(transactions, corrections)
    organizers_sales = clean_organizer_sales(organizer_sales)
    organizers_refunds = clean_organizer_refunds(organizer_refunds)
    merged = merge_transactions(trx_total, organizers_sales, organizers_refunds)
    merged = calc_perc_take_rate(merged)
    return merged.round(2)


def manage_transactions(transactions, **kwargs):
    filtered = filter_transactions(transactions, **kwargs)
    if kwargs.get('groupby'):
        filtered = group_transactions(filtered, kwargs.get('groupby'))
    return filtered.round(2)


def event_details(transactions, event_id, eventholder_user_id):
    transactions = transactions[transactions['event_id'] == event_id]
    details = {
        'Event ID': event_id,
        'Event Title': transactions.iloc[0]['event_title'] if len(transactions) > 0 else '',
        'Organizer ID': eventholder_user_id,
        'Organizer Name': transactions.iloc[0]['organizer_name'] if len(transactions) > 0 else '',
        'Email': transactions.iloc[0]['email'] if len(transactions) > 0 else '',
        'Sales Flag': transactions.iloc[0]['sales_flag'] if len(transactions) > 0 else '',
        'Sales Vertical': transactions.iloc[0]['sales_vertical'] if len(transactions) > 0 else '',
    }
    return details


def summarize_dataframe(dataframe):
    return {
        column: dataframe[column].sum().round(2)
        for column in dataframe.columns.tolist()
        if column in NUMBER_COLUMNS
    }


def get_event_transactions(transactions, event_id, **kwargs):
    event_transactions = manage_transactions(
        transactions,
        event_id=event_id,
    )
    filtered = filter_transactions(event_transactions, **kwargs)
    event_total = summarize_dataframe(filtered)
    if len(event_transactions) > 0:
        details = event_details(
            transactions,
            event_id,
            event_transactions.iloc[0]['eventholder_user_id'],
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
    net_sales_refunds = {
        'Total Net Detail': {
            net: sale + refund
            for net, sale, refund in zip(
                NET_COLUMNS,
                sales_refunds['Total Sales Detail'].values(),
                sales_refunds['Total Refunds Detail'].values(),
            )
        }
    }
    return filtered, details, sales_refunds, net_sales_refunds


def get_organizer_transactions(transactions, eventholder_user_id, **kwargs):
    organizer_transactions = manage_transactions(
        transactions,
        eventholder_user_id=eventholder_user_id,
    )
    filtered = filter_transactions(organizer_transactions, **kwargs)
    event_total = summarize_dataframe(filtered)
    if len(organizer_transactions) > 0:
        details = {
            'Organizer ID': eventholder_user_id,
            'Organizer Name': transactions.iloc[0]['organizer_name'] if len(transactions) > 0 else '',
            'Email': organizer_transactions.iloc[0]['email'],
            'Sales Flag': organizer_transactions.iloc[0]['sales_flag'],
            'Sales Vertical': organizer_transactions.iloc[0]['sales_vertical'],
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
    net_sales_refunds = {
        'Total Net Detail': {
            net: round(sale + refund, 2)
            for net, sale, refund in zip(
                NET_COLUMNS,
                sales_refunds['Total Sales Detail'].values(),
                sales_refunds['Total Refunds Detail'].values(),
            )
        }
    }
    return filtered, details, sales_refunds, net_sales_refunds


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
    }).sort_values(
        by='sale__gtf_esf__epp',
        ascending=False,
    ).round(2).reset_index()
    ordered = calc_perc_take_rate(ordered)
    top = ordered.head(10).copy()
    top.loc[len(top), ['event_title', 'event_id', 'sale__gtf_esf__epp', 'sale__payment_amount__epp']] = [
        'Others',
        '',
        ordered[10:].sale__gtf_esf__epp.sum().round(2),
        ordered[10:].sale__payment_amount__epp.sum().round(2),
    ]
    return top


def get_summarized_data(transactions):
    currencies = {'Argentina': ARS, 'Brazil': BRL}
    summarized_data = {}
    ref_currency = 'local_currency' if 'local_currency' in transactions.columns else 'currency'
    for country, currency in currencies.items():
        filtered = transactions[transactions[ref_currency] == currency]
        summarized_data[country] = {
            'currency': filtered.iloc[0]['currency'],
            'Totals': {
                'Organizers': filtered.eventholder_user_id.nunique(),
                'Events': filtered.event_id.nunique(),
                'PaidTix': filtered.PaidTix.sum(),
            },
            'Gross': {
                'GTF': round(filtered.sale__gtf_esf__epp.sum(), 2),
                'GTV': round(filtered.sale__payment_amount__epp.sum(), 2),
                'ATV': round(
                    (filtered.sale__payment_amount__epp.sum() - filtered.sale__gtf_esf__epp.sum()) / filtered.PaidTix.sum(),
                    2,
                ),
                'Avg EB Take Rate': round(
                    filtered.sale__gtf_esf__epp.sum() / filtered.sale__payment_amount__epp.sum() * 100,
                    2,
                ),
            },
            'Net': {
                'GTF': round(filtered.sale__gtf_esf__epp.sum() + filtered.refund__gtf_epp__gtf_esf__epp.sum(), 2),
                'GTV': round(filtered.sale__payment_amount__epp.sum() + filtered.refund__payment_amount__epp.sum(), 2),
                'ATV': round(
                    (filtered.sale__payment_amount__epp.sum() - filtered.sale__gtf_esf__epp.sum() +
                     filtered.refund__payment_amount__epp.sum() - filtered.refund__gtf_epp__gtf_esf__epp.sum()
                     ) / filtered.PaidTix.sum(),
                    2,
                ),
                'Avg EB Take Rate': round(
                    (filtered.sale__gtf_esf__epp.sum() + filtered.refund__gtf_epp__gtf_esf__epp.sum()) /
                    (filtered.sale__payment_amount__epp.sum() + filtered.refund__payment_amount__epp.sum()) * 100,
                    2,
                ),
            },
        }
    return summarized_data


def payment_processor_summary(trx_currencies, filter):
    json = {}
    filters = {
        'gtv': 'sale__payment_amount__epp',
        'gtf': 'sale__gtf_esf__epp',
    }
    if filter not in filters:
        return json
    column = filters[filter]
    for country, trx_currency in trx_currencies.items():
        currency = trx_currency.currency.iloc[0]
        trx_currency.payment_processor.replace('', 'n/a', regex=True, inplace=True)
        filtered = trx_currency.groupby(['payment_processor']).agg({column: sum}).reset_index().round(2)
        filtered = filtered[filtered[column] != 0]
        filtered_names = filtered.payment_processor.tolist()
        filtered_quantities = filtered[column].tolist()
        filtered_data, filtered_legend = get_chart_json_data(filtered_names, filtered_quantities)
        json[country] = {
            'unit': currency,
            'data': filtered_data,
            'legend': filtered_legend,
        }
    return json


def sales_flag_summary(trx_currencies, filter):
    json = {}
    if filter == 'organizers':
        for country, trx_currency in trx_currencies.items():
            org = trx_currency.groupby(
                ['eventholder_user_id', 'sales_flag']
            ).sum().reset_index().sales_flag.value_counts()
            org_names = org.index.to_list()
            org_quantities = org.values.tolist()
            org_data, org_legend = get_chart_json_data(org_names, org_quantities)
            json[country] = {
                'unit': 'organizers',
                'data': org_data,
                'legend': org_legend,
            }
    elif filter == 'gtf':
        for country, trx_currency in trx_currencies.items():
            currency = trx_currency.currency.iloc[0]
            gtf = trx_currency.groupby(['sales_flag']).agg({'sale__gtf_esf__epp': sum}).reset_index().round(2)
            gtf_names = gtf.sales_flag.to_list()
            gtf_quantities = gtf.sale__gtf_esf__epp.tolist()
            gtf_data, gtf_legend = get_chart_json_data(gtf_names, gtf_quantities)
            json[country] = {
                'unit': currency,
                'data': gtf_data,
                'legend': gtf_legend,
            }
    elif filter == 'gtv':
        for country, trx_currency in trx_currencies.items():
            currency = trx_currency.currency.iloc[0]
            gtv = trx_currency.groupby(['sales_flag']).agg({'sale__payment_amount__epp': sum}).reset_index().round(2)
            gtv_names = gtv.sales_flag.to_list()
            gtv_quantities = gtv.sale__payment_amount__epp.tolist()
            gtv_data, gtv_legend = get_chart_json_data(gtv_names, gtv_quantities)
            json[country] = {
                'unit': currency,
                'data': gtv_data,
                'legend': gtv_legend,
            }
    return json


def get_chart_json_data(names, quantities):
    percent = [str(round(qty/sum(quantities) * 100, 1)) + '% ' for qty in quantities]
    ids = list(range(0, 11))
    data = [
        {'name': name, 'id': id, 'quantity': abs(quantity)}
        for name, id, quantity in zip(names, ids, quantities)
    ]
    legend = [
        {'name': str(percent) + name, 'id': id, 'quantity': abs(quantity)}
        for name, id, quantity, percent in zip(names, ids, quantities, percent)
    ]
    return data, legend



def get_charts_data(transactions, type, filter):
    ref_currency = 'local_currency' if 'local_currency' in transactions.columns else 'currency'
    trx_currencies = {
        'Argentina': transactions[transactions[ref_currency] == ARS],
        'Brazil': transactions[transactions[ref_currency] == BRL],
    }
    json = {}
    if type == 'payment_processor':
        json = payment_processor_summary(trx_currencies, filter)
    elif type == 'sales_flag':
        json = sales_flag_summary(trx_currencies, filter)
    return json


def dataframe_to_usd(transactions, exchange_data):
    trx = []
    for month, values in exchange_data.items():
        trx_month = transactions[transactions['transaction_created_date'].dt.month_name() == month]
        ars = trx_month[trx_month['currency'] == ARS]
        brl = trx_month[trx_month['currency'] == BRL]
        ars['exchange_rate'] = values['ars_to_usd']
        brl['exchange_rate'] = values['brl_to_usd']
        trx.append(pd.concat([ars, brl]))
    converted = pd.concat(trx)
    converted.sort_values(
        by=['transaction_created_date', 'eventholder_user_id', 'event_id'],
        inplace=True,
    )
    renamed_columns = {column: f'local_{column}' for column in (MONEY_COLUMNS + ['currency'])}
    converted.rename(columns=renamed_columns, inplace=True)
    for column in MONEY_COLUMNS:
        converted[column] = converted[f'local_{column}'] / converted['exchange_rate']
    converted['currency'] = USD
    return converted

def restore_currency(transactions):
    deleted_columns = MONEY_COLUMNS + ['currency', 'exchange_rate']
    restored = transactions.drop(deleted_columns, axis=1)
    restored_columns = {f'local_{column}': column for column in (MONEY_COLUMNS + ['currency'])}
    restored.rename(columns=restored_columns, inplace=True)
    return restored
