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
    JsonResponse,
)
from django.views.generic import (
    FormView,
    TemplateView,
)
from django.shortcuts import resolve_url, redirect

from revenue_app.forms import QueryForm
from revenue_app.presto_connection import (
    make_query,
    PrestoError,
)
from revenue_app.utils import (
    generate_transactions_consolidation,
    get_charts_data,
    get_event_transactions,
    get_organizer_transactions,
    get_summarized_data,
    get_top_events,
    get_top_organizers,
    get_top_organizers_refunds,
    manage_transactions,
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
    'payment_processor',
    'currency',
    'PaidTix',
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
    'payment_processor',
    'currency',
    'PaidTix',
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

TOP_ORGANIZERS = {
    'columns': [
        'eventholder_user_id',
        'email',
        'eb_perc_take_rate',
        'sale__gtf_esf__epp',
    ],
    'labels': {
        'eventholder_user_id': 'Organizer',
        'email': 'Email',
        'eb_perc_take_rate': 'Take Rate',
        'sale__gtf_esf__epp': 'SalesGTF',
    }
}

TOP_ORGANIZERS_REFUNDS = {
    'columns': [
        'eventholder_user_id',
        'email',
        'refund__gtf_epp__gtf_esf__epp',
    ],
    'labels': {
        'eventholder_user_id': 'Organizer',
        'email': 'Email',
        'refund__gtf_epp__gtf_esf__epp': 'RefundGTF'
    }
}

TOP_EVENTS_COLUMNS = {
    'columns': [
        'eventholder_user_id',
        'email',
        'event_id',
        'event_title',
        'eb_perc_take_rate',
        'sale__gtf_esf__epp',
    ],
    'labels': {
        'eventholder_user_id': 'Organizer',
        'email': 'Email',
        'event_title': 'Event Title',
        'eb_perc_take_rate': 'Take Rate',
        'sale__gtf_esf__epp': 'SalesGTF',
    }
}


class QueriesRequiredMixin():
    def dispatch(self, request, *args, **kwargs):
        if (
            request.session.get('transactions') is None
            or not request.session.get('query_info')
            or None in request.session.get('query_info').values()
        ):
            return HttpResponseRedirect(resolve_url('make-query'))
        return super().dispatch(request, *args, **kwargs)


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

    def form_valid(self, form):
        start_date = form.data.get('start_date')
        end_date = form.data.get('end_date')
        okta_username = form.data.get('okta_username')
        okta_password = form.data.get('okta_password')

        queries_status = []

        dataframes = {
            'transactions': None,
            'corrections': None,
            'organizer_sales': None,
            'organizer_refunds': None,
        }

        try:
            for name, value in dataframes.items():
                dataframe = make_query(
                    start_date=start_date,
                    end_date=end_date,
                    okta_username=okta_username,
                    okta_password=okta_password,
                    query_name=name,
                )
                dataframes[name] = dataframe
                queries_status.append(
                    f'{name} ran successfully.'
                )
        except PrestoError as exception:
            form.add_error(None, exception.args[0])
        else:
            self.request.session['query_info'] = {
                'run_time': datetime.now(),
                'start_date': datetime.strptime(start_date, '%Y-%m-%d').date(),
                'end_date': datetime.strptime(end_date, '%Y-%m-%d').date(),
            }
            self.request.session['transactions'] = generate_transactions_consolidation(**dataframes)

        return self.render_to_response(
            self.get_context_data(
                queries_status=queries_status,
                form=form,
            )
        )


class Dashboard(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['summarized_data'] = get_summarized_data(
            self.request.session.get('transactions').copy(),
            self.request.session.get('usd'),
        )
        context['title'] = 'Dashboard'
        return context


class OrganizersTransactions(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizers_transactions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            usd=self.request.session.get('usd'),
            **self.request.GET.dict(),
        )[TRANSACTIONS_COLUMNS]
        self.request.session['export_transactions'] = trx
        context['title'] = 'Transactions'
        context['transactions'] = trx.head(500)
        return context


class OrganizerTransactions(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/organizer_transactions.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        transactions, details, sales_refunds, net_sales_refunds = get_organizer_transactions(
            self.request.session.get('transactions').copy(),
            self.kwargs['eventholder_user_id'],
            self.request.session.get('usd'),
            **self.request.GET.dict(),
        )
        context['details'] = details
        context['title'] = 'Organizer ' + details['Email']
        context['sales_refunds'] = sales_refunds
        context['net_sales_refunds'] = net_sales_refunds
        context['transactions'] = transactions[ORGANIZER_COLUMNS]
        self.request.session['export_transactions'] = transactions
        self.request.session['export_details'] = details
        self.request.session['export_sales_refunds'] = sales_refunds
        self.request.session['export_net_sales_refunds'] = net_sales_refunds
        return context


class TopOrganizersLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_organizers.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            **(self.request.GET.dict()),
        )
        context['title'] = 'Top Organizers'
        context['top_ars'] = get_top_organizers(
            trx[trx['currency'] == 'ARS'],
            self.request.session.get('usd'),
        )[:10][TOP_ORGANIZERS['columns']].rename(columns=TOP_ORGANIZERS['labels'])
        context['top_brl'] = get_top_organizers(
            trx[trx['currency'] == 'BRL'],
            self.request.session.get('usd'),
        )[:10][TOP_ORGANIZERS['columns']].rename(columns=TOP_ORGANIZERS['labels'])
        return context


class TopOrganizersRefundsLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_organizers_refunds.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            **(self.request.GET.dict()),
        )
        context['title'] = 'Top Organizers Refunds'
        context['top_ars'] = \
            get_top_organizers_refunds(
                trx[trx['currency'] == 'ARS'],
                self.request.session.get('usd'),
            )[:10][TOP_ORGANIZERS_REFUNDS['columns']].rename(columns=TOP_ORGANIZERS_REFUNDS['labels'])
        context['top_brl'] = \
            get_top_organizers_refunds(
                trx[trx['currency'] == 'BRL'],
                self.request.session.get('usd'),
            )[:10][TOP_ORGANIZERS_REFUNDS['columns']].rename(columns=TOP_ORGANIZERS_REFUNDS['labels'])
        return context


class TransactionsEvent(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/event.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        transactions, details, sales_refunds, net_sales_refunds = get_event_transactions(
            self.request.session.get('transactions').copy(),
            self.request.session.get('usd'),
            self.kwargs['event_id'],
            **(self.request.GET.dict()),
        )
        context['details'] = details
        context['title'] = 'Event ' + details['Event Title']
        context['sales_refunds'] = sales_refunds
        context['net_sales_refunds'] = net_sales_refunds
        context['transactions'] = transactions[EVENT_COLUMNS]
        self.request.session['export_transactions'] = transactions
        self.request.session['export_details'] = details
        self.request.session['export_sales_refunds'] = sales_refunds
        self.request.session['export_net_sales_refunds'] = net_sales_refunds
        return context


class TopEventsLatam(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/top_events.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            **(self.request.GET.dict()),
        )
        context['title'] = 'Top Events'
        context['top_event_ars'] = get_top_events(
            trx[trx['currency'] == 'ARS'],
            self.request.session.get('usd'),
        )[:10][TOP_EVENTS_COLUMNS['columns']].rename(columns=TOP_EVENTS_COLUMNS['labels'])
        context['top_event_brl'] = get_top_events(
            trx[trx['currency'] == 'BRL'],
            self.request.session.get('usd'),
        )[:10][TOP_EVENTS_COLUMNS['columns']].rename(columns=TOP_EVENTS_COLUMNS['labels'])
        return context


class TransactionsGrouped(QueriesRequiredMixin, TemplateView):
    template_name = 'revenue_app/transactions_grouped.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        trx = manage_transactions(
            self.request.session.get('transactions').copy(),
            usd=self.request.session.get('usd'),
            **(self.request.GET.dict()),
        )
        context['title'] = 'Transactions Grouped'
        context['transactions'] = trx
        self.request.session['export_transactions'] = trx
        return context


def top_organizers_json_data(request):
    usd = request.session.get('usd')
    trx = request.session.get('transactions').copy()
    ids = list(range(0, 11))
    top_organizers_ars = get_top_organizers(
        trx[trx['currency'] == 'ARS'],
        usd,
    )
    ars_quantities = top_organizers_ars['sale__gtf_esf__epp'].tolist()
    ars_percent = [str(round(qty/sum(ars_quantities) * 100, 1)) + '% ' for qty in ars_quantities]
    ars_names = top_organizers_ars['email'].tolist()
    top_organizers_brl = get_top_organizers(
        trx[trx['currency'] == 'BRL'],
        usd,
    )
    brl_quantities = top_organizers_brl['sale__gtf_esf__epp'].tolist()
    brl_percent = [str(round(qty/sum(brl_quantities) * 100, 1)) + '% ' for qty in brl_quantities]
    brl_names = top_organizers_brl['email'].tolist()
    res = json.dumps({
        'ars_data': {
            'unit': 'ARS' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': quantity}
                for name, id, quantity in zip(ars_names, ids, ars_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(ars_names, ids, ars_quantities, ars_percent)
            ]
        },
        'brl_data': {
            'unit': 'BRL' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': quantity}
                for name, id, quantity in zip(brl_names, ids, brl_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(brl_names, ids, brl_quantities, brl_percent)
            ]
        },
    })
    return HttpResponse(res, content_type="application/json")


def top_organizers_refunds_json_data(request):
    usd = request.session.get('usd')
    trx = request.session.get('transactions').copy()
    ids = list(range(0, 11))
    top_organizers_ars = get_top_organizers_refunds(
        trx[trx['currency'] == 'ARS'],
        usd,
    )
    ars_quantities = top_organizers_ars['refund__gtf_epp__gtf_esf__epp'].tolist()
    ars_percent = [str(round(qty/sum(ars_quantities) * 100, 1)) + '% ' for qty in ars_quantities]
    ars_names = top_organizers_ars['email'].tolist()
    top_organizers_brl = get_top_organizers_refunds(
        trx[trx['currency'] == 'BRL'],
        usd,
    )
    brl_quantities = top_organizers_brl['refund__gtf_epp__gtf_esf__epp'].tolist()
    brl_percent = [str(round(qty/sum(brl_quantities) * 100, 1)) + '% ' for qty in brl_quantities]
    brl_names = top_organizers_brl['email'].tolist()
    res = json.dumps({
        'ars_data': {
            'unit': 'ARS' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': abs(quantity)}
                for name, id, quantity in zip(ars_names, ids, ars_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(ars_names, ids, ars_quantities, ars_percent)
            ]
        },
        'brl_data': {
            'unit': 'BRL' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': abs(quantity)}
                for name, id, quantity in zip(brl_names, ids, brl_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(brl_names, ids, brl_quantities, brl_percent)
            ]
        },
    })
    return HttpResponse(res, content_type="application/json")


def top_events_json_data(request):
    usd = request.session.get('usd')
    trx = request.session.get('transactions').copy()
    ids = list(range(0, 11))
    top_events_ars = get_top_events(
        trx[trx['currency'] == 'ARS'],
        usd,
    )
    ars_quantities = top_events_ars['sale__gtf_esf__epp'].tolist()
    ars_percent = [str(round(qty/sum(ars_quantities) * 100, 1)) + '% ' for qty in ars_quantities]
    ars_names = [f'[{id}] {title[:20]}' for id, title in zip(top_events_ars['event_id'], top_events_ars['event_title'])]
    top_events_brl = get_top_events(
        trx[trx['currency'] == 'BRL'],
        usd,
    )
    brl_quantities = top_events_brl['sale__gtf_esf__epp'].tolist()
    brl_percent = [str(round(qty/sum(brl_quantities) * 100, 1)) + '% ' for qty in brl_quantities]
    brl_names = [f'[{id}] {title[:20]}' for id, title in zip(top_events_brl['event_id'], top_events_brl['event_title'])]
    res = json.dumps({
        'ars_data': {
            'unit': 'ARS' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': quantity}
                for name, id, quantity in zip(ars_names, ids, ars_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(ars_names, ids, ars_quantities, ars_percent)
            ]
        },
        'brl_data': {
            'unit': 'BRL' if (not usd) or (None in usd.values()) else "USD",
            'data': [
                {'name': name, 'id': id, 'quantity': quantity}
                for name, id, quantity in zip(brl_names, ids, brl_quantities)
            ],
            'legend': [
                {'name': str(percent) + name, 'id': id, 'quantity': quantity}
                for name, id, quantity, percent in zip(brl_names, ids, brl_quantities, brl_percent)
            ]
        },
    })
    return HttpResponse(res, content_type="application/json")


def dashboard_summary(request):
    transactions = request.session.get('transactions').copy()
    if request.GET.get('type') and request.GET.get('filter'):
        res = get_charts_data(
            transactions,
            request.GET.get('type'),
            request.GET.get('filter'),
            usd=request.session.get('usd'),
        )
        return JsonResponse(res, status=200)
    return JsonResponse({}, status=400)


def download_excel(request, xls_name):
    response = HttpResponse(content_type='application/ms-excel')
    response['Content-Disposition'] = 'attachment; filename="{}_{}.xls"'.format(xls_name, datetime.now())
    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('Transactions')
    organizers_transactions = request.session.get('export_transactions')
    query_info = request.session.get('query_info')
    usd_info = request.session.get('usd')
    transactions_list = organizers_transactions.values.tolist()
    columns = organizers_transactions.columns.tolist()
    date_column_idx = columns.index('transaction_created_date')

    title_style = xlwt.XFStyle()
    title_style.font.bold = True
    title_style.font.height = 18 * 20  # you need to multiply by 20 always
    worksheet.write(0, 0, xls_name.replace('_', ' ').capitalize(), title_style)

    col_header_style = xlwt.XFStyle()
    col_header_style.font.bold = True
    worksheet.write(1, 0, "Query ran at:", col_header_style)
    worksheet.write(1, 1, query_info['run_time'].strftime("%Y-%m-%d, %X"))
    worksheet.write(1, 2, "Currency:", col_header_style)
    if not usd_info or None in usd_info.values():
        usd_msg = "Local currency"
    else:
        usd_msg = "USD"
    worksheet.write(1, 3, usd_msg)
    worksheet.write(2, 0, "Start date:", col_header_style)
    worksheet.write(2, 1, query_info['start_date'].strftime("%Y-%m-%d"))
    worksheet.write(2, 2, "End date:", col_header_style)
    worksheet.write(2, 3, query_info['end_date'].strftime("%Y-%m-%d"))

    row_num = 4
    for col_num, col_name in enumerate(columns):
        worksheet.write(row_num, col_num, col_name, col_header_style)

    for row_list in transactions_list:
        row_list[date_column_idx] = row_list[date_column_idx].strftime("%Y-%m-%d")
        row_num += 1
        for col_num, row_value in enumerate(row_list):
            worksheet.write(row_num, col_num, row_value)
    if 'transactions' not in xls_name:
        workbook = excel_summary(request, workbook, title_style, col_header_style)
    workbook.save(response)
    return response


def excel_summary(request, workbook, title_style, col_header_style):
    worksheet = workbook.add_sheet('Summary')
    details = request.session.get('export_details')
    sales_refunds = request.session.get('export_sales_refunds')
    net_sales_refunds = request.session.get('export_net_sales_refunds')
    worksheet.write(1, 1, "Details", title_style)
    row_num = 2
    col_num = 1
    for key, value in details.items():
        worksheet.write(row_num, col_num, key, col_header_style)
        worksheet.write(row_num, col_num + 1, str(value))
        row_num += 1
    row_num = 2
    col_num = 5
    for parent_key, parent_value in sales_refunds.items():
        worksheet.write(1, col_num, parent_key, title_style)
        for key, value in parent_value.items():
            worksheet.write(row_num, col_num, key, col_header_style)
            worksheet.write(row_num, col_num + 1, str(value))
            row_num += 1
        row_num = 2
        col_num += 2
    row_num = 2
    col_num = 9
    for parent_key, parent_value in net_sales_refunds.items():
        worksheet.write(1, col_num, parent_key, title_style)
        for key, value in parent_value.items():
            worksheet.write(row_num, col_num, key, col_header_style)
            worksheet.write(row_num, col_num + 1, str(value))
            row_num += 1
    return workbook


def download_csv(request, csv_name):
    query_info = request.session.get('query_info')
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="{}_[query_ran_at_{}]_[exported_at_{}].csv"'.format(
        csv_name,
        query_info['run_time'],
        datetime.now(),
    )
    writer = csv.writer(response)
    organizers_transactions = request.session.get('export_transactions')
    columns = organizers_transactions.columns.tolist()
    values = organizers_transactions.values.tolist()
    writer.writerow(columns)
    for transaction in values:
        writer.writerow(transaction)
    return response


def usd(request):
    if ('Apply' in request.POST['apply']) and request.POST.get('usd_ars') and request.POST.get('usd_brl'):
        request.session['usd'] = {
            'ARS': float(request.POST.get('usd_ars')),
            'BRL': float(request.POST.get('usd_brl')),
        }
    else:
        request.session['usd'] = {
            'ARS': None,
            'BRL': None,
        }
    return redirect(request.POST.get('next', '/'))
