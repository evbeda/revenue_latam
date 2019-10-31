from functools import reduce
import numpy as np
import pandas as pd


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


def generate_transactions_consolidation(transactions, corrections, organizer_sales, organizer_refunds):
    transactions = clean_transactions(transactions)
    corrections = clean_corrections(corrections)
    trx_total = merge_corrections(transactions, corrections)
    organizers_sales = clean_organizer_sales(organizer_sales)
    organizers_refunds = clean_organizer_refunds(organizer_refunds)
    merged = merge_transactions(trx_total, organizers_sales, organizers_refunds)
    merged = calc_perc_take_rate(merged)
    return merged.round(2)


def manage_transactions(transactions, usd=None, **kwargs):
    filtered = filter_transactions(transactions, **kwargs)
    filtered = convert_dataframe_to_usd(filtered, usd)
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


def get_event_transactions(transactions, usd, event_id, **kwargs):
    event_transactions = manage_transactions(
        transactions,
        usd=usd,
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


def get_organizer_transactions(transactions, eventholder_user_id, usd, **kwargs):
    organizer_transactions = manage_transactions(
        transactions,
        usd=usd,
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


def get_top_organizers(filtered_transactions, usd):
    filtered_transactions = convert_dataframe_to_usd(filtered_transactions, usd)
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


def get_top_organizers_refunds(filtered_transactions, usd):
    filtered_transactions = convert_dataframe_to_usd(filtered_transactions, usd)
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


def get_top_events(filtered_transactions, usd):
    filtered_transactions = convert_dataframe_to_usd(filtered_transactions, usd)
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


def get_summarized_data(transactions, usd):
    currencies = {'Argentina': 'ARS', 'Brazil': 'BRL'}
    summarized_data = {}
    for country, currency in currencies.items():
        filtered = transactions[transactions['currency'] == currency]
        filtered = convert_dataframe_to_usd(filtered, usd)
        summarized_data[country] = {
            'currency': currency if (not usd) or (None in usd.values()) else "USD",
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


def payment_processor_summary(trx_currencies, filter, usd=None):
    ids = list(range(0, 10))
    json = {}
    filters = {
        'gtv': 'sale__payment_amount__epp',
        'gtf': 'sale__gtf_esf__epp',
    }
    if filter not in filters:
        return json
    column = filters[filter]
    for country, trx_currency in trx_currencies.items():
        trx_currency = convert_dataframe_to_usd(trx_currency, usd)
        currency = trx_currency.currency.iloc[0]
        trx_currency.payment_processor.replace('', 'n/a', regex=True, inplace=True)
        filtered = trx_currency.groupby(['payment_processor']).agg({column: sum}).reset_index().round(2)
        filtered = filtered[filtered[column] != 0]
        filtered_names = filtered.payment_processor.tolist()
        filtered_quantities = filtered[column].tolist()
        filtered_percent = [str(round(qty/sum(filtered_quantities) * 100, 1)) + '% ' for qty in filtered_quantities]
        json[country] = {
            'unit': currency,
            'data': [
                {'name': name, 'id': id, 'quantity': quantity}
                for name, id, quantity in zip(filtered_names, ids, filtered_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(filtered_names, ids, filtered_quantities, filtered_percent)
            ]
        }
    return json


def sales_flag_summary(trx_currencies, filter, usd=None):
    ids = list(range(0, 10))
    json = {}
    if filter == 'organizers':
        for country, trx_currency in trx_currencies.items():
            trx_currency = convert_dataframe_to_usd(trx_currency, usd)
            org = trx_currency.groupby(
                ['eventholder_user_id', 'sales_flag']
            ).sum().reset_index().sales_flag.value_counts()
            org_names = org.index.to_list()
            org_quantities = org.values.tolist()
            org_percent = [str(round(qty/sum(org_quantities) * 100, 1)) + '% ' for qty in org_quantities]
            json[country] = {
                'unit': 'organizers',
                'data': [
                    {'name': name, 'id': id, 'quantity': quantity}
                    for name, id, quantity in zip(org_names, ids, org_quantities)
                ],
                'legend': [
                    {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                    for name, id, quantity, percent in zip(org_names, ids, org_quantities, org_percent)
                ]
            }
    elif filter == 'gtf':
        for country, trx_currency in trx_currencies.items():
            trx_currency = convert_dataframe_to_usd(trx_currency, usd)
            currency = trx_currency.currency.iloc[0]
            gtf = trx_currency.groupby(['sales_flag']).agg({'sale__gtf_esf__epp': sum}).reset_index().round(2)
            gtf_names = gtf.sales_flag.to_list()
            gtf_quantities = gtf.sale__gtf_esf__epp.tolist()
            gtf_percent = [str(round(qty/sum(gtf_quantities) * 100, 1)) + '% ' for qty in gtf_quantities]
            json[country] = {
                'unit': currency,
                'data': [
                    {'name': name, 'id': id, 'quantity': quantity}
                    for name, id, quantity in zip(gtf_names, ids, gtf_quantities)
                ],
                'legend': [
                    {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                    for name, id, quantity, percent in zip(gtf_names, ids, gtf_quantities, gtf_percent)
                ]
            }
    elif filter == 'gtv':
        for country, trx_currency in trx_currencies.items():
            trx_currency = convert_dataframe_to_usd(trx_currency, usd)
            currency = trx_currency.currency.iloc[0]
            gtv = trx_currency.groupby(['sales_flag']).agg({'sale__payment_amount__epp': sum}).reset_index().round(2)
            gtv_names = gtv.sales_flag.to_list()
            gtv_quantities = gtv.sale__payment_amount__epp.tolist()
            gtv_percent = [str(round(qty/sum(gtv_quantities) * 100, 1)) + '% ' for qty in gtv_quantities]
            json[country] = {
                'unit': currency,
                'data': [
                    {'name': name, 'id': id, 'quantity': quantity}
                    for name, id, quantity in zip(gtv_names, ids, gtv_quantities)
                ],
                'legend': [
                    {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                    for name, id, quantity, percent in zip(gtv_names, ids, gtv_quantities, gtv_percent)
                ]
            }
    return json


def get_charts_data(transactions, type, filter, usd=None):
    trx_currencies = {
        'Argentina': transactions[transactions['currency'] == 'ARS'],
        'Brazil': transactions[transactions['currency'] == 'BRL'],
    }
    json = {}
    if type == 'payment_processor':
        json = payment_processor_summary(trx_currencies, filter, usd)
    elif type == 'sales_flag':
        json = sales_flag_summary(trx_currencies, filter, usd)
    return json


def convert_dataframe_to_usd(dataframe, usd):
    if (not usd) or (None in usd.values()):
        return dataframe
    for currency in dataframe['currency'].unique():
        dataframe.loc[dataframe['currency'] == currency, MONEY_COLUMNS] = \
            dataframe[dataframe['currency'] == currency][MONEY_COLUMNS] / float(usd[currency])
    dataframe['currency'] = 'USD'
    return dataframe
