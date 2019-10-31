from django.conf.urls import url

from revenue_app.views import (
    Dashboard,
    dashboard_summary,
    download_csv,
    download_excel,
    Exchange,
    MakeQuery,
    OrganizerTransactions,
    OrganizersTransactions,
    restore_local_currency,
    TransactionsEvent,
    TransactionsGrouped,
    top_events_json_data,
    TopEventsLatam,
    top_organizers_json_data,
    TopOrganizersLatam,
    top_organizers_refunds_json_data,
    TopOrganizersRefundsLatam,
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'queries/$', MakeQuery.as_view(), name='make-query'),
    url(r'exchange/$', Exchange.as_view(), name='exchange'),
    url(r'restore_currency/$', restore_local_currency, name='restore-currency'),
    url(r'^transactions/$', OrganizersTransactions.as_view(), name='organizers-transactions'),
    url(
        r'^organizer/(?P<eventholder_user_id>[0-9]+)/$',
        OrganizerTransactions.as_view(),
        name='organizer-transactions',
    ),
    url(r'^transactions/grouped/$', TransactionsGrouped.as_view(), name='transactions-grouped'),
    url(r'^organizers/top/refunds/', TopOrganizersRefundsLatam.as_view(), name='top-organizers-refunds'),
    url(r'^organizers/top/', TopOrganizersLatam.as_view(), name='top-organizers'),
    url(r'^event/(?P<event_id>[0-9]+)/$', TransactionsEvent.as_view(), name='event-details'),
    url(r'^events/top/$', TopEventsLatam.as_view(), name='top-events'),
    url(r'^download/csv/(?P<csv_name>\w+)$', download_csv, name='download-csv'),
    url(r'^download/xls/(?P<xls_name>\w+)$', download_excel, name='download-excel'),
    url(r'^json/top_org_arg/$', top_organizers_json_data, name='json_top_organizers'),
    url(r'^json/top_org_ref_arg/$', top_organizers_refunds_json_data, name='json_top_organizers_refunds'),
    url(r'^json/top_events_arg/$', top_events_json_data, name='json_top_events'),
    url(r'^json/dashboard_summary/$', dashboard_summary, name='json_dashboard_summary'),
]
