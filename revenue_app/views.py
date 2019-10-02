import csv
from datetime import datetime
import json
import xlwt

from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.views.generic import TemplateView

from .utils import (
    get_summarized_data,
    get_transactions_event,
    get_top_events,
    get_top_organizers,
    random_color,
    transactions,
    summarize_dataframe,
    organizer_details,
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

EVENT_COLUMNS = [
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

TOP_ORGANIZERS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'eb_perc_take_rate',
    'sale__gtf_esf__epp',
    'gtv',
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


class Dashboard(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['summarized_data'] = get_summarized_data()
        return context


class OrganizersTransactions(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = transactions(**self.request.GET.dict())[TRANSACTIONS_COLUMNS].head(5000)
        return context


class OrganizerTransactions(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        eventholder_user_id = self.kwargs['organizer_id']
        organizer_transactions = transactions(
            eventholder_user_id=eventholder_user_id,
            **self.request.GET.dict(),
        )[ORGANIZER_COLUMNS]
        context['transactions'] = organizer_transactions
        context['organizer_details'] = organizer_details(eventholder_user_id)
        context['organizer_total'] = summarize_dataframe(organizer_transactions)
        return context


class TransactionsSearch(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['transactions'] = transactions(
            **self.request.GET.dict(),
        )[ORGANIZER_COLUMNS]
        return context


class TopOrganizersLatam(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_ars'] = get_top_organizers(trx[trx['currency'] == 'ARS'])[:10][TOP_ORGANIZERS_COLUMNS]
        context['top_brl'] = get_top_organizers(trx[trx['currency'] == 'BRL'])[:10][TOP_ORGANIZERS_COLUMNS]
        return context


class TransactionsEvent(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['event_id'] = self.kwargs['event_id']
        transactions_event, event_paidtix, event_total = get_transactions_event(
            self.kwargs['event_id'],
            **self.request.GET.dict(),
        )
        if len(transactions_event) > 0:
            context['organizer_id'] = transactions_event.iloc[0]['eventholder_user_id']
            context['organizer_details'] = organizer_details(context['organizer_id'])
        context['transactions'] = transactions_event[EVENT_COLUMNS]
        context['event_total'] = event_total
        context['event_paidtix'] = event_paidtix.iloc[0] if len(event_paidtix) > 0 else ''
        return context


class TopEventsLatam(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_events.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_event_ars'] = get_top_events(trx[trx['currency'] == 'ARS'])[:10][TOP_EVENTS_COLUMNS]
        context['top_event_brl'] = get_top_events(trx[trx['currency'] == 'BRL'])[:10][TOP_EVENTS_COLUMNS]
        return context


class TransactionsGrouped(LoginRequiredMixin, TemplateView):
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
            'borderColor': [color.replace('0.2', '1') for color in colors_pp]
        },
        'sales_flag': {
            'labels': sales_flag.index.to_list(),
            'data': sales_flag.values.tolist(),
            'backgroundColor': colors_sf,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf]
        },
        'currency': {
            'labels': currency.index.tolist(),
            'data': currency.values.tolist(),
            'backgroundColor': colors_c,
            'borderColor': [color.replace('0.2', '1') for color in colors_c]
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
