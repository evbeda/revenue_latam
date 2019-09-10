from django.conf.urls import url

from revenue_app.views import (
    ChartJSONDataView,
    ChartJSONDataViewAlt,
    Dashboard,
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'^json/$', ChartJSONDataView.as_view(), name='json_data'),
    url(r'^json_alt/$', ChartJSONDataViewAlt.as_view(), name='json_data_alt'),
]
