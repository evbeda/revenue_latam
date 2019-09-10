from django.views.generic import TemplateView


class Dashboard(TemplateView):
    template_name = 'revenue_app/dashboard.html'
