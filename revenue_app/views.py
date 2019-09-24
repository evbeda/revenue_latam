import json
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.views.generic import TemplateView
from .utils import (
    FULL_COLUMNS,
    get_dates,
    get_transactions_event,
    get_top_events,
    get_top_organizers,
    random_color,
    transactions,
)
from datetime import datetime, date
import json
import csv
import xlwt


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
        context['organizers_transactions'] = transactions().head(5000)
        return context


class OrganizerTransactions(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        eventholder_user_id = self.kwargs['organizer_id']
        context['organizer_transactions'] = transactions(
            eventholder_user_id=eventholder_user_id,
        ).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context


class TransactionsSearch(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        email = self.request.GET.get('email')
        context['organizer_transactions'] = transactions(email=email).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context


class TopOrganizersLatam(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = transactions()
        context['top_ars'] = get_top_organizers(trx[trx['currency'] == 'ARS'])
        context['top_brl'] = get_top_organizers(trx[trx['currency'] == 'BRL'])
        return context


class TransactionsByDate(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        start_date = self.request.GET.get('start_date')
        end_date = self.request.GET.get('end_date')
        context['organizers_transactions'] = transactions(
            start_date=start_date,
            end_date=end_date,
        )
        return context


class TransactionsEvent(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['event_id'] = self.kwargs['event_id']
        transactions_event, event_paidtix = get_transactions_event(self.kwargs['event_id'])
        context['transactions_event'] = transactions_event
        context['event_paidtix'] = event_paidtix.iloc[0]
        context['organizer_id'] = context['transactions_event'].iloc[0]['eventholder_user_id']
        context['organizer_email'] = context['transactions_event'].iloc[0]['email']
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
        context['transactions'] = transactions(groupby=self.request.GET.get('groupby')).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        )
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

