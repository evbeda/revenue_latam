from django.shortcuts import render
from django.contrib.auth.views import TemplateView, LoginView, LogoutView, PasswordResetForm, PasswordResetView, PasswordResetDoneView


# Create your views here.
class Login(LoginView):
    template_name = 'login_app/login.html'
        # pass

class Logout(LogoutView):
        pass
