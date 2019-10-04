from io import BytesIO

from xhtml2pdf import pisa
import csv
from datetime import datetime
import json
import xlwt

from django.http import HttpResponse
from django.template.loader import get_template
from django.views.generic import TemplateView, View

from .utils import (
    get_event_transactions,
    get_organizer_transactions,
    get_summarized_data,
    get_top_events,
    get_top_organizers,
    get_top_organizers_refunds,
    random_color,
    transactions,
)

FULL_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'gtv',
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
]

TRANSACTIONS_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'gtv',
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
]

ORGANIZER_COLUMNS = [
    'transaction_created_date',
    'event_id',
    'event_title',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'eb_perc_take_rate',
    'gtv',
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
]

EVENT_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'eb_perc_take_rate',
    'gtv',
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
]

TOP_ORGANIZERS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'eb_perc_take_rate',
    'sale__gtf_esf__epp',
    'gtv',
]

TOP_ORGANIZERS_REFUNDS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'refund__gtf_epp__gtf_esf__epp',
]


TOP_EVENTS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'sale__gtf_esf__epp',
    'gtv',
]


class Dashboard(TemplateView):
    template_name = 'revenue_app/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['summarized_data'] = get_summarized_data()
        return context


class OrganizersTransactions(TemplateView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = transactions(**self.request.GET.dict())[TRANSACTIONS_COLUMNS].head(5000)
        return context


class OrganizerTransactions(TemplateView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        transactions, details, sales_refunds = get_organizer_transactions(
            self.kwargs['eventholder_user_id'],
            **self.request.GET.dict(),
        )
        context['details'] = details
        context['sales_refunds'] = sales_refunds
        context['transactions'] = transactions[ORGANIZER_COLUMNS]
        return context


class OrganizerTransactionsPdf(View):

    def get_context_data(self, *args, **kwargs):
        context = {}
        transactions, details, sales_refunds = get_organizer_transactions(
            self.kwargs['eventholder_user_id'],
            **self.request.GET.dict(),
        )
        context['details'] = details
        context['sales_refunds'] = sales_refunds
        context['transactions'] = transactions[ORGANIZER_COLUMNS]
        return context
class OrganizerTransactionsPdf(OrganizerTransactions):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/organizer_transactions_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopOrganizersLatam(TemplateView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_ars'] = get_top_organizers(trx[trx['currency'] == 'ARS'])[:10][TOP_ORGANIZERS_COLUMNS]
        context['top_brl'] = get_top_organizers(trx[trx['currency'] == 'BRL'])[:10][TOP_ORGANIZERS_COLUMNS]
        return context

class TopOrganizersLatamPdf(TopOrganizersLatam):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_organizers_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopOrganizersRefundsLatam(TemplateView):
    template_name = 'revenue_app/top_organizers_refunds.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_ars'] = get_top_organizers_refunds(trx[trx['currency'] == 'ARS'])[:10][TOP_ORGANIZERS_REFUNDS_COLUMNS]
        context['top_brl'] = get_top_organizers_refunds(trx[trx['currency'] == 'BRL'])[:10][TOP_ORGANIZERS_REFUNDS_COLUMNS]
        return context

class TopOrganizersRefundsLatamPdf(TopOrganizersRefundsLatam):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_organizers_refunds_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')

class TransactionsEvent(TemplateView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        transactions, details, sales_refunds = get_event_transactions(
            self.kwargs['event_id'],
            **self.request.GET.dict(),
        )
        context['details'] = details
        context['sales_refunds'] = sales_refunds
        context['transactions'] = transactions[EVENT_COLUMNS]
        return context

class TransactionsEventPdf(TransactionsEvent):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/event_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopEventsLatam(TemplateView):
    template_name = 'revenue_app/top_events.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_event_ars'] = get_top_events(trx[trx['currency'] == 'ARS'])[:10][TOP_EVENTS_COLUMNS]
        context['top_event_brl'] = get_top_events(trx[trx['currency'] == 'BRL'])[:10][TOP_EVENTS_COLUMNS]
        return context

class TopEventsLatamPdf(TopEventsLatam):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_events_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TransactionsGrouped(TemplateView):
    template_name = 'revenue_app/transactions_grouped.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = transactions(**self.request.GET.dict())
        return context


def top_organizers_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(11)]
    top_organizers_ars = get_top_organizers(trx[trx['currency'] == 'ARS'])
    top_organizers_brl = get_top_organizers(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': top_organizers_ars['email'].tolist(),
            'data': top_organizers_ars['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_organizers_brl['email'].tolist(),
            'data': top_organizers_brl['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")

def top_organizers_refunds_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(11)]
    top_organizers_ars = get_top_organizers_refunds(trx[trx['currency'] == 'ARS'])
    top_organizers_brl = get_top_organizers_refunds(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': top_organizers_ars['email'].tolist(),
            'data': top_organizers_ars['refund__gtf_epp__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_organizers_brl['email'].tolist(),
            'data': top_organizers_brl['refund__gtf_epp__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")



def top_events_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(11)]
    top_events_ars = get_top_events(trx[trx['currency'] == 'ARS'])
    top_events_brl = get_top_events(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': [event_title[:30] for event_title in top_events_ars['event_title']],
            'data': top_events_ars['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_events_brl['event_id'].tolist(),
            'data': top_events_brl['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")


def dashboard_summary(request):
    trx = transactions()
    colors_pp = [random_color() for _ in range(4)]
    colors_sf = [random_color() for _ in range(3)]
    colors_c = [random_color() for _ in range(2)]
    trx.payment_processor.replace('', 'n/a', regex=True, inplace=True)
    payment_processor = trx.payment_processor.value_counts()
    sales_flag = trx.sales_flag.value_counts()
    currency = trx.currency.value_counts()
    res = json.dumps({
        'payment_processor': {
            'labels': payment_processor.index.to_list(),
            'data': payment_processor.values.tolist(),
            'backgroundColor': colors_pp,
            'borderColor': [color.replace('0.2', '1') for color in colors_pp],
        },
        'sales_flag': {
            'labels': sales_flag.index.to_list(),
            'data': sales_flag.values.tolist(),
            'backgroundColor': colors_sf,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf],
        },
        'currency': {
            'labels': currency.index.tolist(),
            'data': currency.values.tolist(),
            'backgroundColor': colors_c,
            'borderColor': [color.replace('0.2', '1') for color in colors_c],
        },
    })
    return HttpResponse(res, content_type="application/json")


def download_excel(request):
    response = HttpResponse(content_type='application/ms-excel')
    response['Content-Disposition'] = 'attachment; filename="transactions{}.xls"'.format(datetime.now())
    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Transactions')
    organizers_transactions = transactions()
    transactions_list = organizers_transactions.values.tolist()
    row_num = 0
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    for col_num in range(len(FULL_COLUMNS)):
        worksheet.write(row_num, col_num, FULL_COLUMNS[col_num], font_style)
    for row_list in transactions_list:
        row_list[1] = row_list[1].strftime("%Y-%m-%d")
        row_num += 1
        for col_num in range(len(row_list)):
            worksheet.write(row_num, col_num, row_list[col_num])
    workbook.save(response)
    return response


def download_csv(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="transactions{}.csv"'.format(datetime.now())
    writer = csv.writer(response)
    organizer_transactions = transactions().values.tolist()
    writer.writerow(FULL_COLUMNS)
    for transaction in organizer_transactions:
        writer.writerow(transaction)
    return response

def render_to_pdf(template_src, context_dict={}):
    template = get_template(template_src)
    html  = template.render(context_dict)
    result = BytesIO()
    pdf = pisa.pisaDocument(BytesIO(html.encode("ISO-8859-1")), result)
    if not pdf.err:
        return HttpResponse(result.getvalue(), content_type='application/pdf')
    return None
