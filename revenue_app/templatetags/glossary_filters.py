from django import template

GLOSSARY = {
    'transaction_created_date': 'from all queries',
    'eventholder_user_id': 'from transactions query',
    'Organizer ID': 'from transactions query',
    'Organizer Name': 'from organizer sales/refunds queries',
    'Sales Flag': 'from organizer sales/refunds queries',
    'Sales Vertical': 'from organizer sales/refunds queries',
    'email': 'from all queries',
    'Email': 'from all queries',
    'sales_flag': 'from organizer sales/refunds queries',
    'payment_processor': 'from organizer sales/refunds queries',
    'currency': 'from transactions query',
    'PaidTix': 'from organizer sales/refunds queries',
    'sales_vertical': 'from organizer sales/refunds queries',
    'vertical': 'from organizer sales/refunds queries',
    'sub_vertical': 'from organizer sales/refunds queries',
    'event_id': 'from all queries',
    'Event ID': 'from all queries',
    'event_title': 'from organizer sales/refunds queries',
    'Event Title': 'from organizer sales/refunds queries',
    'eb_perc_take_rate': 'sale__gtf_esf__epp / sale__payment_amount__epp * 100',
    'Take Rate': 'sale__gtf_esf__epp / sale__payment_amount__epp * 100',
    'sale__payment_amount__epp': 'from transactions query',
    'sale__gtf_esf__epp': 'from transactions query',
    'sale__eb_tax__epp': 'from transactions query',
    'sale__ap_organizer__gts__epp': 'from transactions query',
    'sale__ap_organizer__royalty__epp': 'from transactions query',
    'refund__payment_amount__epp': 'from transactions query',
    'refund__gtf_epp__gtf_esf__epp': 'from transactions query',
    'RefundGTF': 'sum of refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp': 'from transactions query',
    'refund__ap_organizer__gts__epp': 'from transactions query',
    'refund__ap_organizer__royalty__epp': 'from transactions query',
    'net__payment_amount__epp': 'sale__payment_amount__epp + refund__payment_amount__epp',
    'net__gtf_esf__epp': 'sale__gtf_esf__epp + refund__gtf_epp__gtf_esf__epp',
    'net__eb_tax__epp': 'sale__eb_tax__epp + refund__eb_tax__epp',
    'net__ap_organizer__gts__epp': 'sale__ap_organizer__gts__epp + refund__ap_organizer__gts__epp',
    'net__ap_organizer__royalty__epp': 'sale__ap_organizer__royalty__epp + refund__ap_organizer__royalty__epp',
    'AVG Ticket Value': 'sale__payment_amount__epp / PaidTix',
    'AVG PaidTix/Day':  'PaidTix / days quantity',
    'TotalsOrganizers': 'number of unique eventholder_user_id',
    'TotalsEvents': 'number of unique event_id',
    'TotalsPaidTix': 'sum of PaidTix',
    'GrossGTF': 'sum of sale__gtf_esf__epp',
    'GrossGTV': 'sum of sale__payment_amount__epp',
    'GrossATV': 'sum of (sale__payment_amount__epp - sale__gtf_esf__epp) / sum of PaidTix',
    'GrossAvg EB Take Rate': 'sum of sale__gtf_esf__epp / sum of sale__payment_amount__epp * 100',
    'NetGTF': 'sum of sale__gtf_esf__epp + refund__gtf_epp__gtf_esf__epp',
    'NetGTV': 'sum of sale__payment_amount__epp + refund__payment_amount__epp',
    'NetATV': 'sum of (sale__payment_amount__epp - sale__gtf_esf__epp'
              ' + refund__payment_amount__epp - refund__gtf_epp__gtf_esf__epp) / sum of PaidTix',
    'NetAvg EB Take Rate': 'sum of (sale__gtf_esf__epp + refund__gtf_epp__gtf_esf__epp) / '
                           'sum of (sale__payment_amount__epp + refund__payment_amount__epp) * 100',
    'SalesGTF': 'sum of sale__gtf_esf__epp',
    'SalesGTV': 'sum of sale__payment_amount__epp',
    'SalesATV': 'sum of (sale__payment_amount__epp - sale__gtf_esf__epp) / sum of PaidTix',
    'SalesAvg EB Take Rate': 'sum of sale__gtf_esf__epp / sum of sale__payment_amount__epp * 100',
}

register = template.Library()


@register.simple_tag(name='glossary')
def get_key_from_glossary(key):
    return GLOSSARY.get(key)
