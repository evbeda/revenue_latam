from django.views.generic import TemplateView


class Index(TemplateView):
    template_name = 'revenue_app/index.html'
