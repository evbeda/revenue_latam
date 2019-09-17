from django.conf.urls import url

from revenue_app.views import (
    ChartJSONDataView,
    ChartJSONDataViewAlt,
    Dashboard,
    OrganizerList
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'organizers/$', OrganizerList.as_view(), name='organizer-list'),
    url(r'^json/$', ChartJSONDataView.as_view(), name='json_data'),
    url(r'^json_alt/$', ChartJSONDataViewAlt.as_view(), name='json_data_alt'),
]
