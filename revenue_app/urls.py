from django.conf.urls import url

from revenue_app.views import (
    Dashboard,
    dashboard_summary,
    download_csv,
    download_excel,
    OrganizerTransactions,
    OrganizerTransactionsPdf,
    OrganizersTransactions,
    TransactionsEvent,
    TransactionsEventPdf,
    TransactionsGrouped,
    top_events_json_data,
    TopEventsLatam,
    TopEventsLatamPdf,
    top_organizers_json_data,
    TopOrganizersLatam,
    TopOrganizersLatamPdf,
    top_organizers_refunds_json_data,
    TopOrganizersRefundsLatam,
    TopOrganizersRefundsLatamPdf,
)


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
    url(r'^transactions/$', OrganizersTransactions.as_view(), name='organizers-transactions'),
    url(
        r'^organizer/(?P<eventholder_user_id>[0-9]+)/$',
        OrganizerTransactions.as_view(),
        name='organizer-transactions',
    ),
    url(
        r'^render/pdf/organizer/(?P<eventholder_user_id>[0-9]+)/$',
        OrganizerTransactionsPdf.as_view(),
        name='download-organizer-pdf',
    ),
    url(r'^transactions/grouped/$', TransactionsGrouped.as_view(), name='transactions-grouped'),
    url(
        r'^render/pdf/organizers/top/refunds/',
        TopOrganizersRefundsLatamPdf.as_view(),
        name='download-organizer-refunds-pdf',
    ),
    url(r'^organizers/top/refunds/', TopOrganizersRefundsLatam.as_view(), name='top-organizers-refunds'),
    url(r'^organizers/top/', TopOrganizersLatam.as_view(), name='top-organizers'),
    url(r'^render/pdf/organizers/top/', TopOrganizersLatamPdf.as_view(), name='download-top-organizers-pdf'),
    url(r'^event/(?P<event_id>[0-9]+)/$', TransactionsEvent.as_view(), name='event-details'),
    url(r'^render/pdf/event/(?P<event_id>[0-9]+)/$', TransactionsEventPdf.as_view(), name='download-event-pdf'),
    url(r'^events/top/$', TopEventsLatam.as_view(), name='top-events'),
    url(r'^render/pdf/events/top/$', TopEventsLatamPdf.as_view(), name='download-top-events-pdf'),
    url(r'^download/csv/(?P<csv_name>\w+)$', download_csv, name='download-csv'),
    url(r'^download/xls/(?P<xls_name>\w+)$', download_excel, name='download-excel'),
    url(r'^json/top_org_arg/$', top_organizers_json_data, name='json_top_organizers'),
    url(r'^json/top_org_ref_arg/$', top_organizers_refunds_json_data, name='json_top_organizers_refunds'),
    url(r'^json/top_events_arg/$', top_events_json_data, name='json_top_events'),
    url(r'^json/dashboard_summary/$', dashboard_summary, name='json_dashboard_summary'),
]
