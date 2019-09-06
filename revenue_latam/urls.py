from django.conf.urls import url, include
from django.contrib import admin


urlpatterns = [
    url('', include('revenue_app.urls')),
    url(r'^accounts/', include('login_app.urls')),
    url(r'^admin/', admin.site.urls),
    url('', include('social_django.urls', namespace='social')),
]
