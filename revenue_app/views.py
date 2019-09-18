from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from .utils import (
    DATE_FILTER_COLUMNS,
    get_dates,
    get_organizer_event_list,
    get_organizer_transactions,
    get_organizers_transactions,
    get_transactions_by_date,
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
        context['organizers_transactions'] = get_organizers_transactions().to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context


class OrganizerTransactions(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        email = self.request.GET.get('email')
        context['organizer_transactions'] = get_organizer_transactions(email).to_html(
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
        return context


class OrganizerEventList(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizer_event_list.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['events'] = get_organizer_event_list(self.kwargs['organizer_id'])
        return context


class TransactionsByDate(LoginRequiredMixin, TransactionsView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        start_date = self.request.GET.get('start_date')
        end_date = self.request.GET.get('end_date')
        context['organizers_transactions'] = get_transactions_by_date(start_date, end_date).to_html(
            columns=DATE_FILTER_COLUMNS,
            index=False,
            classes='table table-sm table-hover table-bordered text-right',
        ).replace(' border="1"', '').replace(' style="text-align: right;"', '')
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
