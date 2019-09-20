from datetime import date
from django.contrib.auth.models import User
from django.test import (
    Client,
    TestCase,
)
from unittest.mock import patch

from pandas import read_csv
from pandas.core.frame import DataFrame
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
    merge_transactions,
    transactions,
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
        ({}, 27),
        ({'eventholder_user_id': 634364434}, 7),
        ({'eventholder_user_id': 497321858}, 5),
        ({'eventholder_user_id': 434444537}, 4),
        ({'eventholder_user_id': 696421958}, 6),
        ({'eventholder_user_id': 506285738}, 5),
        ({'organizer_id': 497321858}, 5),
        ({'organizer_id': 696421958}, 6),
        ({'organizer_id': 434444537}, 4),
        ({'organizer_id': 506285738}, 5),
        ({'organizer_id': 634364434}, 7),
        ({'start_date': date(2018, 8, 3), 'end_date': None}, 1),
        ({'start_date': date(2018, 8, 5), 'end_date': None}, 2),
        ({'start_date': date(2018, 8, 3), 'end_date': date(2018, 8, 16)}, 4),
        ({'start_date': date(2018, 8, 5), 'end_date': date(2018, 8, 3)}, 0),
        ({'email': 'arg_domain@superdomain.org.ar'}, 7),
        ({'email': 'some_fake_mail@gmail.com'}, 5),
        ({'email': 'wow_fake_mail@hotmail.com'}, 4),
        ({'email': 'another_fake_mail@gmail.com'}, 6),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': 66220941}, 5),
        ({'event_id': 98415193}, 6),
        ({'event_id': 17471621}, 4),
        ({'event_id': 35210860}, 5),
        ({'event_id': 88128252}, 7),
        ({'invalid_key': 'something'}, 27),
        ({'eventholder_user_id': '', 'organizer_id': '', 'start_date': '', 'end_date': ''}, 27),
        ({'eventholder_user_id': 696421958}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02'}, 4),
        ({'eventholder_user_id': 696421958, 'start_date': '2018-08-02', 'invalid_key': 'something'}, 4),
        ({'organizer_id': 696421958, 'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 5),
        ({'start_date': '2018-08-02'}, 7),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 10),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'}, 0),
        ({'end_date': '2018-08-05'}, 27),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': '88128252'}, 7),
    ])
    def test_transactions(self, kwargs, expected_length):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            organizer_transactions = transactions(**kwargs)
        self.assertIsInstance(organizer_transactions, DataFrame)
        self.assertListEqual(
            sorted(organizer_transactions.columns),
            sorted(FULL_TRANSACTIONS_COLUMNS),
        )
        self.assertEqual(len(organizer_transactions), expected_length)

    def test_get_dates(self):
        with patch('pandas.read_csv', return_value=read_csv(TRANSACTIONS_EXAMPLE_PATH)):
            dates = get_dates()
        self.assertIsInstance(dates, list)
        self.assertEqual(len(dates), 12)

    @parameterized.expand([
        (66220941, 5),
        (98415193, 6),
        (17471621, 4),
        (35210860, 5),
        (88128252, 7),
    ])
    def test_event_transactions(self, event_id, transactions_qty):
        with patch('pandas.read_csv', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
        )):
            transactions_event, paidtix = get_transactions_event(event_id)
        self.assertIsInstance(transactions_event, DataFrame)
        self.assertEqual(len(transactions_event), transactions_qty)

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
        ({'eventholder_user_id': 696421958}, 6),
        ({'eventholder_user_id': '696421958', 'start_date': '2018-08-02'}, 4),
        ({'eventholder_user_id': 696421958, 'start_date': '2018-08-02', 'invalid_key': 'something'}, 4),
        ({'organizer_id': 696421958, 'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 5),
        ({'start_date': '2018-08-02'}, 7),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'}, 10),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'}, 0),
        ({'end_date': '2018-08-05'}, 27),
        ({'email': 'personalized_domain@wowdomain.com.br'}, 5),
        ({'event_id': '88128252'}, 7),
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



class ViewsTest(TestCase):
    def setUp(self):
        username = 'test_user'
        password = 'test_pass'
        User.objects.create_user(username=username, password=password)
        self.logged_client = Client()
        self.logged_client.login(username=username, password=password)

    def test_organizers_transactions_view_returns_302_when_not_logged(self):
        URL = '/transactions/'
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    def test_organizer_transactions_view_returns_302_when_not_logged(self):
        EVENT_ID = 497321858
        URL = '/organizers/{}/'.format(EVENT_ID)
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))

    def test_transactions_search_view_returns_302_when_not_logged(self):
        email = 'arg_domain@superdomain.org.ar'
        URL = '/transactions/search/'
        client = Client()
        response = client.get(URL, {'email': email})
        self.assertEqual(response.status_code, 302)

    def test_top_organizers_view_returns_302_when_not_logged(self):
        URL = '/organizers/top/'
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
