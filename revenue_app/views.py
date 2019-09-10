from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView

from chartjs.views.lines import BaseLineChartView


class Dashboard(LoginRequiredMixin, TemplateView):
    template_name = 'revenue_app/dashboard.html'


class ChartJSONDataView(BaseLineChartView):
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


class ChartJSONDataViewAlt(BaseLineChartView):
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
