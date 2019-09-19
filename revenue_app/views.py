from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from .utils import (
    get_dates,
    get_organizer_event_list,
    get_organizer_transactions,
    get_organizers_transactions,
    get_transactions_by_date,
    transactions_search,
    get_transactions_event,
    get_top_organizers,
)

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
        context['organizers_transactions'] = get_organizers_transactions()
        return context


class OrganizerTransactions(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        eventholder_user_id = self.kwargs['organizer_id']
        context['organizer_transactions'] = get_organizer_transactions(eventholder_user_id).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context


class TransactionsSearch(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        email = self.request.GET.get('email')
        context['organizer_transactions'] = transactions_search(email).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context

class TopOrganizersLatam(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        transactions = get_organizers_transactions()
        context['top_ars'] = get_top_organizers(transactions[transactions['currency']=='ARS'])
        context['top_brl'] = get_top_organizers(transactions[transactions['currency']=='BRL'])
        return context


class TransactionsByDate(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        start_date = self.request.GET.get('start_date')
        end_date = self.request.GET.get('end_date')
        context['organizers_transactions'] = get_transactions_by_date(start_date, end_date)
        return context


class TransactionsEvent(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['event_id'] = self.kwargs['event_id']
        transactions_event, event_paidtix = get_transactions_event(self.kwargs['event_id'])
        context['transactions_event'] = transactions_event
        context['event_paidtix'] = event_paidtix.item()
        context['organizer_id'] = context['transactions_event'].iloc[0]['eventholder_user_id']
        context['organizer_email'] = context['transactions_event'].iloc[0]['email']
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
