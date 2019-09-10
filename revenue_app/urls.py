from django.conf.urls import url
from revenue_app.views import Dashboard


urlpatterns = [
    url(r'^$', Dashboard.as_view(), name='dashboard'),
]
