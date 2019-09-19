from django.conf.urls import url

from revenue_app.views import (
    ChartJSONDataView,
    ChartJSONDataViewAlt,
    Dashboard,
    OrganizerEventList,
    OrganizerTransactions,
    OrganizersTransactions,
    TransactionsByDate,
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'transactions/$', OrganizersTransactions.as_view(), name='organizers-transactions'),
    url(r'organizers/(?P<organizer_id>[0-9]+)/$', OrganizerTransactions.as_view(), name='organizer-transactions'),
    # url(r'organizers/search/$', OrganizerTransactions.as_view(), name='organizer-transactions'),
    url(r'transactions/dates/$', TransactionsByDate.as_view(), name='transactions-by-dates'),
    url(r'organizers/(?P<organizer_id>[0-9]+)/events/$', OrganizerEventList.as_view(), name='organizer-event-list'),
    url(r'^json/$', ChartJSONDataView.as_view(), name='json_data'),
    url(r'^json_alt/$', ChartJSONDataViewAlt.as_view(), name='json_data_alt'),
]
