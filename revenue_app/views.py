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
import json

from chartjs.views.lines import BaseLineOptionsChartView


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
        context['organizers_transactions'] = transactions()
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


class ChartOptionsMixin():
    def get_options(self):
        '''
        Returns a dict of options.
        Not implemented in parent class.
        '''
        options = {
            'scales': {
                'yAxes': [{
                    'ticks': {
                        'beginAtZero': True
                    }
                }]
            }
        }
        return options


class ChartJSONDataView(ChartOptionsMixin, BaseLineOptionsChartView):
    def get_providers(self):
        '''
        Return names of dataset.
        Returns [] in parent class.
        '''
        return ['Org1', 'Org2', 'Org3']

    def get_labels(self):
        '''
        Return labels for the x-axis.
        Not implemented in parent class.
        '''
        return ['GTS', 'GTF', 'Qty']

    def get_data(self):
        '''
        Return lists of datasets to show.
        Example: [[1, 2, 3], [4, 5, 6]].
        Not implemented in parent class.
        '''
        return [[5, 5, 5], [6, 5, 4], [4, 5, 6]]


class ChartJSONDataViewAlt(ChartOptionsMixin, BaseLineOptionsChartView):
    def get_providers(self):
        '''
        Return names of dataset.
        Returns [] in parent class.
        '''
        return ['GTS', 'GTF', 'Qty']

    def get_labels(self):
        '''
        Return labels for the x-axis.
        Not implemented in parent class.
        '''
        return ['Org1', 'Org2', 'Org3']

    def get_data(self):
        '''
        Return lists of datasets to show.
        Example: [[1, 2, 3], [4, 5, 6]].
        Not implemented in parent class.
        '''
        return [[5, 5, 5], [6, 5, 4], [4, 5, 6]]


def top_organizers_json_data(request):
    trx = transactions()
    colors = [random_color() for _ in range(10)]
    res = json.dumps({
        'arg': {
            'labels': get_top_organizers(trx[trx['currency'] == 'ARS'])['email'].tolist(),
            'data': get_top_organizers(trx[trx['currency'] == 'ARS'])['sale__payment_amount__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': get_top_organizers(trx[trx['currency'] == 'BRL'])['email'].tolist(),
            'data': get_top_organizers(trx[trx['currency'] == 'BRL'])['sale__payment_amount__epp'].tolist(),
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
