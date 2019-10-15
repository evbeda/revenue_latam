from io import BytesIO

from xhtml2pdf import pisa
import csv
from datetime import (
    date,
    datetime,
)
from dateutil.relativedelta import relativedelta
import json
import xlwt

from django.http import (
    HttpResponse,
    HttpResponseRedirect,
)
from django.template.loader import get_template
from django.views.generic import (
    FormView,
    TemplateView,
)
from django.shortcuts import resolve_url

from revenue_app.forms import QueryForm
from revenue_app.presto_connection import (
    make_query,
    PrestoError,
)
from revenue_app.utils import (
    get_event_transactions,
    get_organizer_transactions,
    get_summarized_data,
    get_top_events,
    get_top_organizers,
    get_top_organizers_refunds,
    manage_transactions,
    payment_processor_summary,
    random_color,
    sales_flag_summary,
)

FULL_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    'refund__ap_organizer__royalty__epp',
]

TRANSACTIONS_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    'refund__ap_organizer__royalty__epp',
]

ORGANIZER_COLUMNS = [
    'transaction_created_date',
    'event_id',
    'event_title',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    'refund__ap_organizer__royalty__epp',
]

EVENT_COLUMNS = [
    'transaction_created_date',
    'eventholder_user_id',
    'sales_flag',
    'payment_processor',
    'currency',
    'PaidTix',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    'refund__ap_organizer__royalty__epp',
]

TOP_ORGANIZERS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'eb_perc_take_rate',
    'sale__gtf_esf__epp',
]

TOP_ORGANIZERS_REFUNDS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'refund__gtf_epp__gtf_esf__epp',
]


TOP_EVENTS_COLUMNS = [
    'eventholder_user_id',
    'email',
    'event_id',
    'event_title',
    'eb_perc_take_rate',
    'sale__gtf_esf__epp',
]


class QueriesRequiredMixin():
    def dispatch(self, request, *args, **kwargs):
        if any([
            request.session.get(dataframe) is None
            for dataframe in ['transactions', 'corrections', 'organizer_sales', 'organizer_refunds']
        ]):
            return HttpResponseRedirect(resolve_url('make-query'))
        return super().dispatch(request, *args, **kwargs)


class Dashboard(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['summarized_data'] = get_summarized_data(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
        )
        return context


class MakeQuery(FormView):
    template_name = 'revenue_app/query.html'
    form_class = QueryForm

    def get_initial(self):
        initial = super().get_initial()
        today = date.today()
        previous_month_end = date(today.year, today.month, 1) - relativedelta(days=1)
        previous_month_start = date(previous_month_end.year, previous_month_end.month, 1)
        initial['start_date'] = previous_month_start
        initial['end_date'] = previous_month_end
        return initial

    def post(self, request, *args, **kwargs):
        form = self.get_form()
        start_date = form.data.get('start_date')
        end_date = form.data.get('end_date')
        okta_username = form.data.get('okta_username')
        okta_password = form.data.get('okta_password')

        queries_status = []
        error = ""

        for query_name in [
            'organizer_sales',
            'organizer_refunds',
            'transactions',
            'corrections',
        ]:
            try:
                dataframe = make_query(
                    start_date=start_date,
                    end_date=end_date,
                    okta_username=okta_username,
                    okta_password=okta_password,
                    query_name=query_name,
                )
                request.session[query_name] = dataframe
                queries_status.append(
                    f'{query_name} ran successfully.'
                )
            except PrestoError as exception:
                error = exception.args[0]
                break

        return self.render_to_response(
            self.get_context_data(
                queries_status=queries_status,
                error=error,
                form=form,
            )
        )


class OrganizersTransactions(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            **self.request.GET.dict(),
        )[TRANSACTIONS_COLUMNS]
        context['transactions'] = trx.head(5000)
        self.request.session['export_transactions'] = trx
        return context


class OrganizerTransactions(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        transactions, details, sales_refunds = get_organizer_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            self.kwargs['eventholder_user_id'],
            **self.request.GET.dict(),
        )
        context['details'] = details
        context['sales_refunds'] = sales_refunds
        context['transactions'] = transactions[ORGANIZER_COLUMNS]
        self.request.session['export_transactions'] = transactions
        return context


class OrganizerTransactionsPdf(OrganizerTransactions):

    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/organizer_transactions_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopOrganizersLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            **(self.request.GET.dict()),
        )
        context['top_ars'] = get_top_organizers(trx[trx['currency'] == 'ARS'])[:10][TOP_ORGANIZERS_COLUMNS]
        context['top_brl'] = get_top_organizers(trx[trx['currency'] == 'BRL'])[:10][TOP_ORGANIZERS_COLUMNS]
        return context


class TopOrganizersLatamPdf(TopOrganizersLatam):
    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_organizers_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopOrganizersRefundsLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_organizers_refunds.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            **(self.request.GET.dict()),
        )
        context['top_ars'] = \
            get_top_organizers_refunds(trx[trx['currency'] == 'ARS'])[:10][TOP_ORGANIZERS_REFUNDS_COLUMNS]
        context['top_brl'] = \
            get_top_organizers_refunds(trx[trx['currency'] == 'BRL'])[:10][TOP_ORGANIZERS_REFUNDS_COLUMNS]
        return context


class TopOrganizersRefundsLatamPdf(TopOrganizersRefundsLatam):
    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_organizers_refunds_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TransactionsEvent(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        transactions, details, sales_refunds = get_event_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            self.kwargs['event_id'],
            **(self.request.GET.dict()),
        )
        context['details'] = details
        context['sales_refunds'] = sales_refunds
        context['transactions'] = transactions[EVENT_COLUMNS]
        self.request.session['export_transactions'] = transactions
        return context


class TransactionsEventPdf(TransactionsEvent):
    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/event_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TopEventsLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_events.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            **(self.request.GET.dict()),
        )
        context['top_event_ars'] = get_top_events(trx[trx['currency'] == 'ARS'])[:10][TOP_EVENTS_COLUMNS]
        context['top_event_brl'] = get_top_events(trx[trx['currency'] == 'BRL'])[:10][TOP_EVENTS_COLUMNS]
        return context


class TopEventsLatamPdf(TopEventsLatam):
    def get(self, request, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        pdf = render_to_pdf('revenue_app/top_events_pdf.html', context)
        return HttpResponse(pdf, content_type='application/pdf')


class TransactionsGrouped(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/transactions_grouped.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('corrections').copy(),
            self.request.session.get('organizer_sales').copy(),
            self.request.session.get('organizer_refunds').copy(),
            **(self.request.GET.dict()),
        )
        context['transactions'] = trx
        self.request.session['export_transactions'] = trx
        return context


def top_organizers_json_data(request):
    trx = manage_transactions(
        request.session.get('transactions'),
        request.session.get('corrections'),
        request.session.get('organizer_sales'),
        request.session.get('organizer_refunds'),
    )
    colors = [random_color() for _ in range(11)]
    top_organizers_ars = get_top_organizers(trx[trx['currency'] == 'ARS'])
    top_organizers_brl = get_top_organizers(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': top_organizers_ars['email'].tolist(),
            'data': top_organizers_ars['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_organizers_brl['email'].tolist(),
            'data': top_organizers_brl['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")


def top_organizers_refunds_json_data(request):
    trx = manage_transactions(
        request.session.get('transactions'),
        request.session.get('corrections'),
        request.session.get('organizer_sales'),
        request.session.get('organizer_refunds'),
    )
    colors = [random_color() for _ in range(11)]
    top_organizers_ars = get_top_organizers_refunds(trx[trx['currency'] == 'ARS'])
    top_organizers_brl = get_top_organizers_refunds(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': top_organizers_ars['email'].tolist(),
            'data': top_organizers_ars['refund__gtf_epp__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_organizers_brl['email'].tolist(),
            'data': top_organizers_brl['refund__gtf_epp__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")


def top_events_json_data(request):
    trx = manage_transactions(
        request.session.get('transactions'),
        request.session.get('corrections'),
        request.session.get('organizer_sales'),
        request.session.get('organizer_refunds'),
    )
    colors = [random_color() for _ in range(11)]
    top_events_ars = get_top_events(trx[trx['currency'] == 'ARS'])
    top_events_brl = get_top_events(trx[trx['currency'] == 'BRL'])
    res = json.dumps({
        'arg': {
            'labels': [event_title[:30] for event_title in top_events_ars['event_title']],
            'data': top_events_ars['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
        'brl': {
            'labels': top_events_brl['event_id'].tolist(),
            'data': top_events_brl['sale__gtf_esf__epp'].tolist(),
            'backgroundColor': colors,
            'borderColor': [color.replace('0.2', '1') for color in colors]
        },
    })
    return HttpResponse(res, content_type="application/json")


def dashboard_summary(request):
    trx = manage_transactions(
        request.session.get('transactions'),
        request.session.get('corrections'),
        request.session.get('organizer_sales'),
        request.session.get('organizer_refunds'),
    )
    ars = trx[trx['currency'] == 'ARS']
    brl = trx[trx['currency'] == 'BRL']
    ars_pp_gtv, ars_pp_gtf = payment_processor_summary(ars)
    brl_pp_gtv, brl_pp_gtf = payment_processor_summary(brl)
    ars_sf_org, ars_sf_gtf = sales_flag_summary(ars)
    brl_sf_org, brl_sf_gtf = sales_flag_summary(brl)
    colors_pp_gtv = [random_color() for _ in range(4)]
    colors_pp_gtf = [random_color() for _ in range(4)]
    colors_sf_org = [random_color() for _ in range(4)]
    colors_sf_gtf = [random_color() for _ in range(4)]
    res = json.dumps({
        'ars_pp_gtv': {
            'labels': ars_pp_gtv.payment_processor.tolist(),
            'data': ars_pp_gtv.sale__payment_amount__epp.tolist(),
            'backgroundColor': colors_pp_gtv,
            'borderColor': [color.replace('0.2', '1') for color in colors_pp_gtv],
        },
        'ars_pp_gtf': {
            'labels': ars_pp_gtf.payment_processor.to_list(),
            'data': ars_pp_gtf.sale__gtf_esf__epp.tolist(),
            'backgroundColor': colors_pp_gtf,
            'borderColor': [color.replace('0.2', '1') for color in colors_pp_gtf],
        },
        'brl_pp_gtv': {
            'labels': brl_pp_gtv.payment_processor.tolist(),
            'data': brl_pp_gtv.sale__payment_amount__epp.tolist(),
            'backgroundColor': colors_pp_gtv,
            'borderColor': [color.replace('0.2', '1') for color in colors_pp_gtv],
        },
        'brl_pp_gtf': {
            'labels': brl_pp_gtf.payment_processor.to_list(),
            'data': brl_pp_gtf.sale__gtf_esf__epp.tolist(),
            'backgroundColor': colors_pp_gtf,
            'borderColor': [color.replace('0.2', '1') for color in colors_pp_gtf],
        },
        'ars_sf_org': {
            'labels': ars_sf_org.index.to_list(),
            'data': ars_sf_org.values.tolist(),
            'backgroundColor': colors_sf_org,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf_org],
        },
        'ars_sf_gtf': {
            'labels': ars_sf_gtf.sales_flag.to_list(),
            'data': ars_sf_gtf.sale__gtf_esf__epp.tolist(),
            'backgroundColor': colors_sf_gtf,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf_gtf],
        },
        'brl_sf_org': {
            'labels': brl_sf_org.index.to_list(),
            'data': brl_sf_org.values.tolist(),
            'backgroundColor': colors_sf_org,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf_org],
        },
        'brl_sf_gtf': {
            'labels': brl_sf_gtf.sales_flag.to_list(),
            'data': brl_sf_gtf.sale__gtf_esf__epp.tolist(),
            'backgroundColor': colors_sf_gtf,
            'borderColor': [color.replace('0.2', '1') for color in colors_sf_gtf],
        },
    })
    return HttpResponse(res, content_type="application/json")


def download_excel(request, xls_name):
    response = HttpResponse(content_type='application/ms-excel')
    response['Content-Disposition'] = 'attachment; filename="{}_{}.xls"'.format(xls_name, datetime.now())
    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Transactions')
    organizers_transactions = request.session.get('export_transactions')
    transactions_list = organizers_transactions.values.tolist()
    columns = organizers_transactions.columns.tolist()
    date_column_idx = columns.index('transaction_created_date')
    row_num = 0
    font_style = xlwt.XFStyle()
    font_style.font.bold = True
    for col_num, col_name in enumerate(columns):
        worksheet.write(row_num, col_num, col_name, font_style)
    for row_list in transactions_list:
        row_list[date_column_idx] = row_list[date_column_idx].strftime("%Y-%m-%d")
        row_num += 1
        for col_num, row_value in enumerate(row_list):
            worksheet.write(row_num, col_num, row_value)
    workbook.save(response)
    return response


def download_csv(request, csv_name):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{}_{}.csv"'.format(csv_name, datetime.now())
    writer = csv.writer(response)
    organizers_transactions = request.session.get('export_transactions')
    columns = organizers_transactions.columns.tolist()
    values = organizers_transactions.values.tolist()
    writer.writerow(columns)
    for transaction in values:
        writer.writerow(transaction)
    return response


def render_to_pdf(template_src, context_dict={}):
    template = get_template(template_src)
    html = template.render(context_dict)
    result = BytesIO()
    pdf = pisa.pisaDocument(BytesIO(html.encode("ISO-8859-1")), result)
    return HttpResponse(result.getvalue(), content_type='application/pdf') if not pdf.err else None
