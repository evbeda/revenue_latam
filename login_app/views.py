from django.contrib.auth.views import LoginView, LogoutView, TemplateView


class Login(LoginView):
    template_name = 'login_app/login.html'


class Logout(LogoutView):
    pass

class LoginError(TemplateView):
    template_name = 'login_app/login_error.html'
