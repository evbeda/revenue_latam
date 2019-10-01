from datetime import date
from django.template import (
    Context,
    Template,
)
from django.test import (
    Client,
    TestCase,
)
from django.urls import reverse
from unittest.mock import patch

from pandas import read_csv
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from parameterized import parameterized

from revenue_app.utils import (
    calc_gtv,
    calc_perc_take_rate,
    filter_transactions,
    get_organizer_sales,
    get_top_events,
    get_top_organizers,
    get_transactions,
    get_transactions_event,
    group_transactions,
    merge_transactions,
    organizer_details,
    random_color,
    summarize_dataframe,
    transactions,
)

from revenue_app.views import (
    Dashboard,
    OrganizerTransactions,
    OrganizersTransactions,
    TopEventsLatam,
    TopOrganizersLatam,
    TransactionsEvent,
    TransactionsGrouped,
    TransactionsSearch,
)

TRANSACTIONS_EXAMPLE_PATH = 'revenue_app/tests/transactions_example.csv'
ORGANIZER_SALES_EXAMPLE_PATH = 'revenue_app/tests/organizer_sales_example.csv'

BASE_ORGANIZER_SALES_COLUMNS = [
    'transaction_created_date',
    'email',
    'organizer_name',
    'event_id',
    'event_title',
    'sales_flag',
    'sales_vertical',
    'vertical',
    'sub_vertical',
    'GTSntv',
    'GTFntv',
    'PaidTix',
]

BASE_TRANSACTION_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'email',
    'payment_processor',
    'currency',
    # Vertical (not found yet)
    # Subvertical (not found yet)
    'event_id',
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

MERGED_TRANSACTIONS_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
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


class UtilsTestCase(TestCase):

    @property
    def transactions(self):
        with patch('pandas.read_csv', return_value=read_csv(TRANSACTIONS_EXAMPLE_PATH)):
            return get_transactions()

    @property
    def organizer_sales(self):
        with patch('pandas.read_csv', return_value=read_csv(ORGANIZER_SALES_EXAMPLE_PATH)):
            return get_organizer_sales()

    def test_get_transactions(self):
        transactions = self.transactions
        self.assertIsInstance(transactions, DataFrame)
        self.assertListEqual(
            sorted(transactions.columns.tolist()),
            sorted(BASE_TRANSACTION_COLUMNS),
        )
        self.assertNotIn('sale__eb_tax__epp__1', transactions.columns.tolist())
        self.assertEqual(len(transactions), 27)

    def test_get_organizer_sales(self):
        organizer_sales = self.organizer_sales
        self.assertIsInstance(organizer_sales, DataFrame)
        self.assertListEqual(
            sorted(organizer_sales.columns.tolist()),
            sorted(BASE_ORGANIZER_SALES_COLUMNS),
        )
        self.assertNotIn('organizer_email', organizer_sales.columns.tolist())
        self.assertEqual(len(organizer_sales), 27)

    def test_merge_transactions(self):
        merged_transactions = merge_transactions(self.transactions, self.organizer_sales)
        self.assertIsInstance(merged_transactions, DataFrame)
        self.assertListEqual(
            sorted(merged_transactions.columns),
            sorted(MERGED_TRANSACTIONS_COLUMNS),
        )
        self.assertEqual(len(merged_transactions), 27)

    @parameterized.expand([
        ('day', 27),
        ('week', 5),
        ('semi-month', 2),
        ('month', 1),
        ('quarter', 1),
        ('year', 1),
        ('eventholder_user_id', 5),
        (['eventholder_user_id', 'email'], 5),
        ('event_id', 5),
        (['payment_processor'], 3),
        ('payment_processor', 4),
        ('currency', 2),
    ])
    def test_group_transactions(self, by, expected_length):
        grouped = group_transactions(self.transactions, by)
        self.assertEqual(len(grouped), expected_length)

    @parameterized.expand([
        ({}, 27),
        ({'eventholder_user_id': '634364434'}, 7),
        ({'eventholder_user_id': '497321858'}, 5),
        ({'eventholder_user_id': '434444537'}, 4),
        ({'eventholder_user_id': '696421958'}, 6),
        ({'eventholder_user_id': '506285738'}, 5),
        ({'start_date': date(2018, 8, 3), 'end_date': None}, 2),
        ({'start_date': date(2018, 8, 5), 'end_date': None}, 2),
        ({'start_date': date(2018, 8, 3), 'end_date': date(2018, 8, 16)}, 17),
        ({'start_date': date(2018, 8, 5), 'end_date': date(2018, 8, 3)}, 0),
        ({'email': 'arg_domain@superdomain.org.ar'}, 7),
        ({'email': 'some_fake_mail@gmail.com'}, 5),
        ({'email': 'wow_fake_mail@hotmail.com'}, 4),
        ({'email': 'another_fake_mail@gmail.com'}, 6),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': '66220941'}, 5),
        ({'event_id': '98415193'}, 6),
        ({'event_id': '17471621'}, 4),
        ({'event_id': '35210860'}, 5),
        ({'event_id': '88128252'}, 7),
        ({'invalid_key': 'something'}, 27),
        ({'eventholder_user_id': '', 'start_date': '', 'end_date': ''}, 27),
        ({'eventholder_user_id': '696421958'}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-06'}, 1),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-06', 'invalid_key': 'something'}, 1),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-07', 'end_date': '2018-08-09'}, 3),
        ({'start_date': '2018-08-02'}, 2),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 8),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'}, 0),
        ({'end_date': '2018-08-05'}, 27),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': '88128252'}, 7),
        ({'groupby': 'day'}, 27),
        ({'groupby': 'day', 'start_date': '2018-08-02', 'end_date': '2018-08-15'}, 10),
        ({'groupby': 'week'}, 5),
        ({'groupby': 'semi-month'}, 2),
        ({'groupby': 'month'}, 1),
        ({'groupby': 'quarter'}, 1),
        ({'groupby': 'year'}, 1),
        ({'groupby': 'eventholder_user_id'}, 5),
        ({'groupby': ['eventholder_user_id', 'email']}, 5),
        ({'groupby': 'event_id'}, 5),
        ({'groupby': ['payment_processor']}, 3),
        ({'groupby': 'payment_processor'}, 4),
        ({'groupby': 'currency'}, 2),
    ])
    def test_transactions(self, kwargs, expected_length):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            organizer_transactions = transactions(**kwargs)
        self.assertIsInstance(organizer_transactions, DataFrame)
        self.assertEqual(len(organizer_transactions), expected_length)

    @parameterized.expand([
        ('66220941', 5, 3500),
        ('98415193', 6, 3402),
        ('17471621', 4, 2250),
        ('35210860', 5, 39),
        ('88128252', 7, 481),
    ])
    def test_event_transactions(self, event_id, transactions_qty, tickets_qty):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            transactions_event, paidtix, total = get_transactions_event(event_id)
        self.assertIsInstance(transactions_event, DataFrame)
        self.assertEqual(len(transactions_event), transactions_qty)
        self.assertIsInstance(paidtix, Series)
        self.assertEqual(len(paidtix), transactions_qty)
        self.assertEqual(paidtix.iloc[0], tickets_qty)

    def test_get_top_ten_organizers(self):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            trx = transactions()
        top_ars = get_top_organizers(trx[trx['currency'] == 'ARS'])
        top_brl = get_top_organizers(trx[trx['currency'] == 'BRL'])
        self.assertIsInstance(top_ars, DataFrame)
        self.assertIsInstance(top_brl, DataFrame)
        self.assertEqual(len(top_ars), 3)
        self.assertEqual(len(top_brl), 4)

    def test_calc_perc_take_rate(self):
        transactions = self.transactions
        initial_columns = transactions.columns
        result = calc_perc_take_rate(transactions)
        self.assertEqual(len(initial_columns) + 1, len(result.columns))
        self.assertIn('eb_perc_take_rate', result.columns)

    def test_calc_gtv(self):
        transactions = self.transactions
        initial_columns = transactions.columns
        result = calc_gtv(transactions)
        self.assertEqual(len(initial_columns) + 1, len(result.columns))
        self.assertIn('gtv', result.columns)

    @parameterized.expand([
        ({}, 27),
        ({'invalid_key': 'something'}, 27),
        ({'eventholder_user_id': '', 'start_date': '', 'end_date': ''}, 27),
        ({'eventholder_user_id': '696421958'}, 6),
        ({'eventholder_user_id': '	696421958 '}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-06'}, 1),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-06', 'invalid_key': 'something'}, 1),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-07', 'end_date': '2018-08-10'}, 4),
        ({'eventholder_user_id': ' 696421958	', 'start_date': '2018-08-07', 'end_date': '2018-08-10'}, 4),
        ({'start_date': '2018-08-02'}, 2),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 8),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'}, 0),
        ({'end_date': '2018-08-05'}, 27),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'email': '     personalized_domain@wowdomain.com.br    '}, 5),
        ({'event_id': '88128252'}, 7),
        ({'event_id': '			88128252		'}, 7),
    ])
    def test_filter_transactions(self, kwargs, expected_length):
        with patch('pandas.read_csv', return_value=read_csv(TRANSACTIONS_EXAMPLE_PATH)):
            filtered_transactions = filter_transactions(**kwargs)
        self.assertEqual(len(filtered_transactions), expected_length)

    def test_get_top_ten_events(self):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            trx = transactions()
        top_ars = get_top_events(trx[trx['currency'] == 'ARS'])
        top_brl = get_top_events(trx[trx['currency'] == 'BRL'])
        self.assertIsInstance(top_ars, DataFrame)
        self.assertIsInstance(top_brl, DataFrame)
        self.assertEqual(len(top_ars), 3)
        self.assertEqual(len(top_brl), 4)

    def test_random_color(self):
        color = random_color()
        self.assertIsInstance(color, str)
        self.assertEqual(color[:5], "rgba(")
        self.assertEqual(color[-1], ")")
        color_list = color[5:-1].split(", ")
        self.assertEqual(len(color_list), 4)
        self.assertEqual(color_list[-1], "0.2")
        for c in color_list[:-1]:
            self.assertTrue(0 <= int(c) <= 255)

    def test_download_csv(self):
        URL = reverse('download-csv')
        client = Client()
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = client.get(URL)
        self.assertTrue(str(type(response)), "_csv.reader")

    def test_download_excel(self):
        URL = reverse('download-excel')
        client = Client()
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = client.get(URL)
        self.assertTrue(str(type(response)), "_excel.reader")

    @parameterized.expand([
        ('497321858', {
            'sale__payment_amount__epp': 4555.3,
            'sale__eb_tax__epp': 61.7,
            'sale__ap_organizer__gts__epp': 4200.0,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 293.6,
            'refund__payment_amount__epp': 0,
            'refund__gtf_epp__gtf_esf__epp': 0,
            'refund__ap_organizer__gts__epp': 0,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('696421958', {
            'sale__payment_amount__epp': 6270.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 5831.7,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 438.3,
            'refund__payment_amount__epp': 0,
            'refund__gtf_epp__gtf_esf__epp': 0,
            'refund__ap_organizer__gts__epp': 0,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('434444537', {
            'sale__payment_amount__epp': 1188.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 1080.0,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 108.0,
            'refund__payment_amount__epp': 0,
            'refund__gtf_epp__gtf_esf__epp': 0,
            'refund__ap_organizer__gts__epp': 0,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('506285738', {
            'sale__payment_amount__epp': 18150.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 16698.0,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 1452.0,
            'refund__payment_amount__epp': 0,
            'refund__gtf_epp__gtf_esf__epp': 0,
            'refund__ap_organizer__gts__epp': 0,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
    ])
    def test_summarize_dataframe(self, eventholder_user_id, expected_total):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            organizer = transactions(eventholder_user_id=eventholder_user_id)
        total_organizer = summarize_dataframe(organizer)
        self.assertEqual(total_organizer, expected_total)

    @parameterized.expand([
        ('497321858', {'email': 'some_fake_mail@gmail.com', 'name': 'Fake 1'}),
        ('696421958', {'email': 'another_fake_mail@gmail.com', 'name': 'Fake 2'}),
        ('434444537', {'email': 'wow_fake_mail@hotmail.com', 'name': 'Wow Such Fake'}),
        ('506285738', {'email': 'personalized_domain@wowdomain.com.br', 'name': 'Br Fake'}),
        ('634364434', {'email': 'arg_domain@superdomain.org.ar', 'name': 'Ar Fake'}),
    ])
    def test_organizer_details(self, eventholder_user_id, details_organizer):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = organizer_details(eventholder_user_id)
        self.assertEqual(response, details_organizer)


class ViewsTest(TestCase):
    def setUp(self):
        self.client = Client()

    def test_dashboard_view_returns_200(self):
        URL = reverse('dashboard')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Dashboard.template_name)

    @parameterized.expand([
        ({},),
        ({'start_date': '2018-08-02'},),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'},),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'},),
        ({'end_date': '2018-08-05'},),
    ])
    def test_organizers_transactions_view_returns_200(self, kwargs):
        URL = reverse('organizers-transactions')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL, kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizersTransactions.template_name)

    @parameterized.expand([
        (497321858, 5),
        (696421958, 6),
        (434444537, 4),
        (506285738, 5),
        (634364434, 7),
    ])
    def test_organizer_transactions_view_returns_200(self, eventholder_user_id, expected_length):
        URL = reverse('organizer-transactions', kwargs={'eventholder_user_id': eventholder_user_id})
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL)
        self.assertEqual(len(response.context['transactions']), expected_length)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizerTransactions.template_name)

    @parameterized.expand([
        ('arg_domain@superdomain.org.ar', 7),
        ('some_fake_mail@gmail.com', 5),
        ('wow_fake_mail@hotmail.com', 4),
        ('another_fake_mail@gmail.com', 6),
        ('personalized_domain@wowdomain.com.br', 5),
    ])
    def test_transactions_search_view_returns_200(self, email, expected_length):
        URL = '{}?email={}'.format(reverse('transactions-search'), email)
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL)
        self.assertEqual(len(response.context['transactions']), expected_length)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsSearch.template_name)

    def test_top_organizers_view_returns_200(self):
        URL = reverse('top-organizers')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL)
        self.assertEqual(len(response.context['top_ars']), 3)
        self.assertEqual(len(response.context['top_brl']), 4)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopOrganizersLatam.template_name)

    def test_top_events_view_returns_200(self):
        URL = reverse('top-events')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL)
        self.assertEqual(len(response.context['top_event_ars']), 3)
        self.assertEqual(len(response.context['top_event_brl']), 4)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopEventsLatam.template_name)

    @parameterized.expand([
        (66220941, 5),
        (98415193, 6),
        (17471621, 4),
        (35210860, 5),
        (88128252, 7),
    ])
    def test_events_transactions_view_returns_200(self, event_id, expected_length):
        URL = reverse('event-details', kwargs={'event_id': event_id})
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL)
        self.assertEqual(len(response.context['transactions']), expected_length)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsEvent.template_name)

    def test_transactions_grouped_view_returns_200(self):
        kwargs = {'groupby': 'day'}
        URL = reverse('transactions-grouped')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.client.get(URL, kwargs)
        self.assertEqual(len(response.context['transactions']), 27)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsGrouped.template_name)


class TemplateTagsTest(TestCase):
    def render_template(self, string, context=None):
        context = context or {}
        return Template(string).render(Context(context))

    @parameterized.expand([
        ({'value': date(2019, 8,  4)}, 'July 29, 2019'),
        ({'value': date(2019, 8, 11)},  'Aug. 5, 2019'),
        ({'value': date(2019, 8, 18)}, 'Aug. 12, 2019'),
        ({'value': date(2019, 8, 25)}, 'Aug. 19, 2019'),
        ({'value': date(2019, 9,  1)}, 'Aug. 26, 2019'),
    ])
    def test_week_start(self, context, expected):
        rendered = self.render_template(
            '{% load date_filters %}'
            '{{value|week_start|date}}',
            context
        )
        self.assertEqual(rendered, expected)

    @parameterized.expand([
        ({'value': date(2019, 8,  1)}, 'Aug. 14, 2019'),
        ({'value': date(2019, 8, 15)}, 'Aug. 31, 2019'),
        ({'value': date(2019, 2,  1)}, 'Feb. 14, 2019'),
        ({'value': date(2019, 2, 15)}, 'Feb. 28, 2019'),
        ({'value': date(2020, 2,  1)}, 'Feb. 14, 2020'),
        ({'value': date(2020, 2, 15)}, 'Feb. 29, 2020'),
    ])
    def test_semimonth_end(self, context, expected):
        rendered = self.render_template(
            '{% load date_filters %}'
            '{{value|semimonth_end|date}}',
            context
        )
        self.assertEqual(rendered, expected)

    @parameterized.expand([
        ({'value': date(2019,  1,  1)}, 'Q1'),
        ({'value': date(2019,  2, 15)}, 'Q1'),
        ({'value': date(2019,  3, 19)}, 'Q1'),
        ({'value': date(2019,  4, 30)}, 'Q2'),
        ({'value': date(2019,  5, 24)}, 'Q2'),
        ({'value': date(2019,  6, 14)}, 'Q2'),
        ({'value': date(2019,  7, 10)}, 'Q3'),
        ({'value': date(2019,  8, 25)}, 'Q3'),
        ({'value': date(2019,  9, 21)}, 'Q3'),
        ({'value': date(2019, 10,  8)}, 'Q4'),
        ({'value': date(2019, 11, 11)}, 'Q4'),
        ({'value': date(2019, 12, 16)}, 'Q4'),
    ])
    def test_quarter(self, context, expected):
        rendered = self.render_template(
            '{% load date_filters %}'
            'Q{{value|quarter}}',
            context
        )
        self.assertEqual(rendered, expected)

    @parameterized.expand([
        ({'value': date(2019,  1,  1)}, 'Jan. 1, 2019'),
        ({'value': date(2019,  2, 15)}, 'Jan. 1, 2019'),
        ({'value': date(2019,  3, 19)}, 'Jan. 1, 2019'),
        ({'value': date(2019,  4, 30)}, 'April 1, 2019'),
        ({'value': date(2019,  5, 24)}, 'April 1, 2019'),
        ({'value': date(2019,  6, 14)}, 'April 1, 2019'),
        ({'value': date(2019,  7, 10)}, 'July 1, 2019'),
        ({'value': date(2019,  8, 25)}, 'July 1, 2019'),
        ({'value': date(2019,  9, 21)}, 'July 1, 2019'),
        ({'value': date(2019, 10,  8)}, 'Oct. 1, 2019'),
        ({'value': date(2019, 11, 11)}, 'Oct. 1, 2019'),
        ({'value': date(2019, 12, 16)}, 'Oct. 1, 2019'),
    ])
    def test_quarter_start(self, context, expected):
        rendered = self.render_template(
            '{% load date_filters %}'
            '{{value|quarter_start}}',
            context
        )
        self.assertEqual(rendered, expected)
