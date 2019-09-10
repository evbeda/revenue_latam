from django.conf.urls import url
from .views import Login, Logout, LoginError


urlpatterns = [
    url(r'^login/$', Login.as_view(), name='login'),
    url(r'^logout/$', Logout.as_view(), name='logout'),
    url(r'^login/error/$', LoginError.as_view(), name='login-error'),
]
