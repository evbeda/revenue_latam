from django.conf.urls import url

from revenue_app.views import (
    ChartJSONDataView,
    ChartJSONDataViewAlt,
    Dashboard,
    OrganizerTransactions,
    OrganizersTransactions,
    TransactionsByDate,
    TransactionsSearch,
    TransactionsEvent,
    TopEventsLatam,
    TopOrganizersLatam,
    top_events_json_data,
    top_organizers_json_data,
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'transactions/$', OrganizersTransactions.as_view(), name='organizers-transactions'),
    url(r'organizers/(?P<organizer_id>[0-9]+)/$', OrganizerTransactions.as_view(), name='organizer-transactions'),
    url(r'transactions/search/$', TransactionsSearch.as_view(), name='transactions-search'),
    url(r'transactions/dates/$', TransactionsByDate.as_view(), name='transactions-by-dates'),
    url(r'organizers/top/', TopOrganizersLatam.as_view(), name='top-organizers'),
    url(r'event/(?P<event_id>[0-9]+)/$', TransactionsEvent.as_view(), name='event-details'),
    url(r'events/top/$', TopEventsLatam.as_view(), name='top-events'),
    url(r'^json/$', ChartJSONDataView.as_view(), name='json_data'),
    url(r'^json/alt/$', ChartJSONDataViewAlt.as_view(), name='json_data_alt'),
    url(r'^json/top_org_arg/$', top_organizers_json_data, name='json_top_organizers'),
    url(r'^json/top_events_arg/$', top_events_json_data, name='json_top_events'),
]
