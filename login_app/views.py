from django.contrib.auth.views import LoginView, LogoutView


class Login(LoginView):
    template_name = 'login_app/login.html'


class Logout(LogoutView):
        pass
