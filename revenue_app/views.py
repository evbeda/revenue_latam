import csv
from datetime import datetime
import json
import xlwt

from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.views.generic import TemplateView

from .utils import (
    get_dates,
    get_transactions_event,
    get_top_events,
    get_top_organizers,
    random_color,
    transactions,
)

FULL_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'payment_processor',
    'currency',
    # Vertical (not found yet)
    # Subvertical (not found yet)
    'event_id',
    'email',
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
    'sales_flag',
    'eb_perc_take_rate',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]

TRANSACTIONS_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
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

ORGANIZER_COLUMNS = [
    'transaction_created_date',
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

EVENT_COLUMNS = [
    'transaction_created_date',
    'sales_flag',
    'payment_processor',
    'currency',
    # Vertical (not found yet)
    # Subvertical (not found yet)
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


class TransactionsView(TemplateView):
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['dates_list'] = get_dates()
        return context


class Dashboard(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/dashboard.html'


class OrganizersTransactions(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = transactions(**self.request.GET.dict())[TRANSACTIONS_COLUMNS].head(5000)
        return context


class OrganizerTransactions(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        eventholder_user_id = self.kwargs['organizer_id']
        context['transactions'] = transactions(
            eventholder_user_id=eventholder_user_id,
            **self.request.GET.dict(),
        )[ORGANIZER_COLUMNS]
        return context


class TransactionsSearch(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['transactions'] = transactions(
            **self.request.GET.dict(),
        )[ORGANIZER_COLUMNS]
        return context


class TopOrganizersLatam(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions(**(self.request.GET.dict()))
        context['top_ars'] = get_top_organizers(trx[trx['currency'] == 'ARS'])
        context['top_brl'] = get_top_organizers(trx[trx['currency'] == 'BRL'])
        return context


class TransactionsEvent(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['event_id'] = self.kwargs['event_id']
        transactions_event, event_paidtix = get_transactions_event(
            self.kwargs['event_id'],
            **self.request.GET.dict(),
        )
        if len(transactions_event) > 0:
            context['organizer_id'] = transactions_event.iloc[0]['eventholder_user_id']
            context['organizer_email'] = transactions_event.iloc[0]['email']
        context['transactions'] = transactions_event[EVENT_COLUMNS]
        context['event_paidtix'] = event_paidtix.iloc[0]
        return context


class TopEventsLatam(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/top_events.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions()
        context['top_event_ars'] = get_top_events(trx[trx['currency'] == 'ARS'])
        context['top_event_brl'] = get_top_events(trx[trx['currency'] == 'BRL'])
        return context


class TransactionsGrouped(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/transactions_grouped.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['transactions'] = transactions(**self.request.GET.dict())
        return context


def top_organizers_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(10)]
    res = json.dumps({
        'arg': {
            'labels': get_top_organizers(trx[trx['currency'] == 'ARS'])['email'].tolist(),
            'data': get_top_organizers(trx[trx['currency'] == 'ARS'])['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': get_top_organizers(trx[trx['currency'] == 'BRL'])['email'].tolist(),
            'data': get_top_organizers(trx[trx['currency'] == 'BRL'])['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")


def top_events_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(10)]
    res = json.dumps({
        'arg': {
            'labels': get_top_events(trx[trx['currency'] == 'ARS'])['event_id'].tolist(),
            'data': get_top_events(trx[trx['currency'] == 'ARS'])['sale__payment_amount__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': get_top_events(trx[trx['currency'] == 'BRL'])['event_id'].tolist(),
            'data': get_top_events(trx[trx['currency'] == 'BRL'])['sale__payment_amount__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
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
