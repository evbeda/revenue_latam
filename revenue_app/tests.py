from datetime import date
from django.contrib.auth.models import User
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

from .utils import (
    calc_perc_take_rate,
    filter_transactions,
    get_dates,
    get_organizer_sales,
    get_transactions,
    get_transactions_event,
    get_top_organizers,
    get_top_events,
    group_transactions,
    merge_transactions,
    random_color,
    transactions,
)

from .views import (
    Dashboard,
    OrganizerTransactions,
    OrganizersTransactions,
    TopEventsLatam,
    TopOrganizersLatam,
    TransactionsByDate,
    TransactionsEvent,
    TransactionsSearch,
)

TRANSACTIONS_EXAMPLE_PATH = 'revenue_app/tests/transactions_example.csv'
ORGANIZER_SALES_EXAMPLE_PATH = 'revenue_app/tests/organizer_sales_example.csv'

BASE_ORGANIZER_SALES_COLUMNS = [
    'email',
    'organizer_name',
    'sales_flag',
    'event_id',
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
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]

MERGED_TRANSACTIONS_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'email',
    'sales_flag',
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
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]

FULL_TRANSACTIONS_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'email',
    'sales_flag',
    'payment_processor',
    'currency',
    # Vertical (not found yet)
    # Subvertical (not found yet)
    'event_id',
    'eb_perc_take_rate',
    'sale__payment_amount__epp',
    'sale__gtf_esf__epp',
    'sale__eb_tax__epp',
    'sale__ap_organizer__gts__epp',
    'sale__ap_organizer__royalty__epp',
    'sale__gtf_esf__offline',
    'refund__payment_amount__epp',
    'refund__gtf_epp__gtf_esf__epp',
    'refund__eb_tax__epp',
    'refund__ap_organizer__gts__epp',
    # 'refund__ap_organizer__royalty__epp', (not found yet)
]


class UtilsTestCase(TestCase):
    def setUp(self):
        pass

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
        self.assertEqual(len(organizer_sales), 5)

    def test_merge_transactions(self):
        merged_transactions = merge_transactions(self.transactions, self.organizer_sales)
        self.assertIsInstance(merged_transactions, DataFrame)
        self.assertListEqual(
            sorted(merged_transactions.columns),
            sorted(MERGED_TRANSACTIONS_COLUMNS),
        )
        self.assertEqual(len(merged_transactions), 27)

    @parameterized.expand([
        ('daily', 31),
        ('weekly', 5),
        ('semi-monthly', 2),
        ('monthly', 1),
        ('quarterly', 1),
        ('yearly', 1),
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
        ({'organizer_id': '497321858'}, 5),
        ({'organizer_id': '696421958'}, 6),
        ({'organizer_id': '434444537'}, 4),
        ({'organizer_id': '506285738'}, 5),
        ({'organizer_id': '634364434'}, 7),
        ({'start_date': date(2018, 8, 3), 'end_date': None}, 1),
        ({'start_date': date(2018, 8, 5), 'end_date': None}, 2),
        ({'start_date': date(2018, 8, 3), 'end_date': date(2018, 8, 16)}, 4),
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
        ({'eventholder_user_id': '', 'organizer_id': '', 'start_date': '', 'end_date': ''}, 27),
        ({'eventholder_user_id': '696421958'}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02'}, 4),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02', 'invalid_key': 'something'}, 4),
        ({'organizer_id': '696421958', 'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 5),
        ({'start_date': '2018-08-02'}, 7),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 10),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'}, 0),
        ({'end_date': '2018-08-05'}, 27),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': '88128252'}, 7),
        ({'groupby': 'daily'}, 31),
        ({'groupby': 'daily', 'start_date': '2018-08-02', 'end_date': '2018-08-15'}, 14),
        ({'groupby': 'weekly'}, 5),
        ({'groupby': 'semi-monthly'}, 2),
        ({'groupby': 'monthly'}, 1),
        ({'groupby': 'quarterly'}, 1),
        ({'groupby': 'yearly'}, 1),
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

    def test_get_dates(self):
        with patch('pandas.read_csv', return_value=read_csv(TRANSACTIONS_EXAMPLE_PATH)):
            dates = get_dates()
        self.assertIsInstance(dates, list)
        self.assertEqual(len(dates), 12)

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
            transactions_event, paidtix = get_transactions_event(event_id)
        self.assertIsInstance(transactions_event, DataFrame)
        self.assertEqual(len(transactions_event), transactions_qty)
        self.assertIsInstance(paidtix, Series)
        self.assertEqual(len(paidtix), 1)
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
        self.assertEqual(len(top_ars), 2)
        self.assertEqual(len(top_brl), 3)

    def test_calc_perc_take_rate(self):
        transactions = self.transactions
        initial_columns = transactions.columns
        result = calc_perc_take_rate(transactions)
        self.assertEqual(len(initial_columns) + 1, len(result.columns))
        self.assertIn('eb_perc_take_rate', result.columns)

    @parameterized.expand([
        ({}, 27),
        ({'invalid_key': 'something'}, 27),
        ({'eventholder_user_id': '', 'organizer_id': '', 'start_date': '', 'end_date': ''}, 27),
        ({'eventholder_user_id': '696421958'}, 6),
        ({'eventholder_user_id': '	696421958 '}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02'}, 4),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02', 'invalid_key': 'something'}, 4),
        ({'organizer_id': '696421958', 'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 5),
        ({'organizer_id': ' 696421958	', 'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 5),
        ({'start_date': '2018-08-02'}, 7),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 10),
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
        self.assertEqual(len(top_ars), 2)
        self.assertEqual(len(top_brl), 3)

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



class ViewsTest(TestCase):
    def setUp(self):
        username = 'test_user'
        password = 'test_pass'
        User.objects.create_user(username=username, password=password)
        self.logged_client = Client()
        self.logged_client.login(username=username, password=password)

    def test_dashboard_view_returns_302_when_not_logged(self):
        URL = reverse('dashboard')
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    def test_dashboard_view_returns_200_when_logged(self):
        URL = reverse('dashboard')
        with patch('pandas.read_csv', return_value=read_csv(TRANSACTIONS_EXAMPLE_PATH)):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Dashboard.template_name)

    def test_organizers_transactions_view_returns_302_when_not_logged(self):
        URL = reverse('organizers-transactions')
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    def test_organizers_transactions_view_returns_200_when_logged(self):
        URL = reverse('organizers-transactions')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizersTransactions.template_name)

    def test_organizer_transactions_view_returns_302_when_not_logged(self):
        eventholder_user_id = 497321858
        URL = reverse('organizer-transactions', kwargs={'organizer_id': eventholder_user_id})
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    @parameterized.expand([
        (66220941,),
        (98415193,),
        (17471621,),
        (35210860,),
        (88128252,),
    ])
    def test_organizer_transactions_view_returns_200_when_logged(self, eventholder_user_id):
        URL = reverse('organizer-transactions', kwargs={'organizer_id': eventholder_user_id})
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizerTransactions.template_name)

    def test_transactions_search_view_returns_302_when_not_logged(self):
        email = 'arg_domain@superdomain.org.ar'
        URL = '{}?email={}'.format(reverse('transactions-search'), email)
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)

    @parameterized.expand([
        ('arg_domain@superdomain.org.ar',),
        ('some_fake_mail@gmail.com',),
        ('wow_fake_mail@hotmail.com',),
        ('another_fake_mail@gmail.com',),
        ('personalized_domain@wowdomain.com.br',),
    ])
    def test_transactions_search_view_returns_200_when_logged(self, email):
        URL = '{}?email={}'.format(reverse('transactions-search'), email)
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsSearch.template_name)

    def test_top_organizers_view_returns_302_when_not_logged(self):
        URL = reverse('top-organizers')
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)

    def test_top_organizers_view_returns_200_when_logged(self):
        URL = reverse('top-organizers')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopOrganizersLatam.template_name)

    def test_top_events_view_returns_302_when_not_logged(self):
        URL = reverse('top-events')
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)

    def test_top_events_view_returns_200_when_logged(self):
        URL = reverse('top-events')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopEventsLatam.template_name)

    def test_transactions_by_date_view_returns_302_when_not_logged(self):
        date = '2018-08-02'
        URL = reverse('transactions-by-dates')
        client = Client()
        response = client.get(URL, {'start_date': date})
        self.assertEqual(response.status_code, 302)

    @parameterized.expand([
        ({'start_date': '2018-08-02'},),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'},),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'},),
        ({'end_date': '2018-08-05'},),
    ])
    def test_transactions_by_date_view_returns_200_when_logged(self, kwargs):
        URL = reverse('transactions-by-dates')
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL, kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsByDate.template_name)

    def test_events_transactions_view_returns_302_when_not_logged(self):
        event_id = 66220941
        URL = reverse('event-details', kwargs={'event_id': event_id})
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    @parameterized.expand([
        (66220941,),
        (98415193,),
        (17471621,),
        (35210860,),
        (88128252,),
    ])
    def test_events_transactions_view_returns_200_when_logged(self, event_id):
        URL = reverse('event-details', kwargs={'event_id': event_id})
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            response = self.logged_client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsEvent.template_name)
