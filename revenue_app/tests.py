from datetime import date
from django.contrib.auth.models import User
from django.test import TestCase, Client
from unittest.mock import patch

from pandas import read_csv
from pandas.core.frame import DataFrame
from parameterized import parameterized

from .utils import (
    filter_transactions_by_date,
    get_organizer_sales,
    get_transactions,
    merge_transactions,
    get_organizers_transactions,
    get_organizer_transactions,
    get_organizer_event_list,
)


class UtilsTestCase(TestCase):
    def setUp(self):
        pass

    def test_get_transactions(self):
        with patch(
            'pandas.read_csv',
            return_value=read_csv('revenue_app/tests/transactions_example.csv')
        ):
            transactions = get_transactions()
        self.assertIsInstance(
            transactions,
            DataFrame
        )
        self.assertListEqual(
            transactions.columns.tolist(),
            [
                'eventholder_user_id',
                'transaction_created_date',
                'payment_processor',
                'currency',
                'event_id',
                'email',
                'sale__payment_amount__epp',
                'sale__eb_tax__epp',
                'sale__ap_organizer__gts__epp',
                'sale__ap_organizer__royalty__epp',
                'sale__gtf_esf__epp',
                'sale__gtf_esf__offline',
                'refund__payment_amount__epp',
                'refund__gtf_epp__gtf_esf__epp',
                'refund__ap_organizer__gts__epp',
                'refund__eb_tax__epp',
            ]
        )
        self.assertNotIn(
            'sale__eb_tax__epp__1',
            transactions.columns.tolist()
        )
        self.assertEqual(
            len(transactions),
            27
        )

    def test_get_organizer_sales(self):
        with patch(
            'pandas.read_csv',
            return_value=read_csv('revenue_app/tests/organizer_sales_example.csv')
        ):
            organizer_sales = get_organizer_sales()
        self.assertIsInstance(
            organizer_sales,
            DataFrame
        )
        self.assertListEqual(
            organizer_sales.columns.tolist(),
            [
                'email',
                'organizer_name',
                'sales_flag',
                'event_id',
                'GTSntv',
                'GTFntv',
                'PaidTix'
            ]
        )
        self.assertNotIn(
            'organizer_email',
            organizer_sales.columns.tolist()
        )
        self.assertEqual(
            len(organizer_sales),
            5
        )

    @parameterized.expand([
        (date(2018, 8, 3), None, 1),
        (date(2018, 8, 5), None, 2),
        (date(2018, 8, 3), date(2018, 8, 16), 4),
        (date(2018, 8, 5), date(2018, 8, 3), 0),
    ])
    def test_filter_transactions_by_date(self, start_date, end_date, expected_length):
        with patch(
            'pandas.read_csv',
            return_value=read_csv('revenue_app/tests/transactions_example.csv')
        ):
            transactions = get_transactions()
        filtered_transactions = filter_transactions_by_date(transactions, start_date, end_date)
        self.assertIsInstance(
            filtered_transactions,
            DataFrame
        )
        self.assertEqual(
            len(filtered_transactions),
            expected_length,
        )

    @parameterized.expand([
        (497321858, 5),
        (696421958, 6),
        (434444537, 4),
        (506285738, 5),
        (634364434, 7),
        ])
    def test_organizer_event_list(self, organizer_id, expected_length):
        with patch(
            'pandas.read_csv',
            return_value=read_csv('revenue_app/tests/transactions_example.csv')
        ):
            transactions = get_organizer_event_list(organizer_id)
        self.assertEqual(len(transactions), expected_length)

    def test_merge_transactions(self):
        with patch('pandas.read_csv', side_effect=(
            read_csv('revenue_app/tests/transactions_example.csv'),
            read_csv('revenue_app/tests/organizer_sales_example.csv'),
        )):
            merged_transactions = merge_transactions()
        self.assertIsInstance(
            merged_transactions,
            DataFrame
        )
        self.assertListEqual(
            merged_transactions.columns.tolist(),
            ['eventholder_user_id',
             'transaction_created_date',
             'payment_processor',
             'currency',
             'event_id',
             'email',
             'sale__payment_amount__epp',
             'sale__eb_tax__epp',
             'sale__ap_organizer__gts__epp',
             'sale__ap_organizer__royalty__epp',
             'sale__gtf_esf__epp',
             'sale__gtf_esf__offline',
             'refund__payment_amount__epp',
             'refund__gtf_epp__gtf_esf__epp',
             'refund__ap_organizer__gts__epp',
             'refund__eb_tax__epp',
             'sales_flag',
             'eb_perc_take_rate']
        )
        self.assertEqual(
            len(merged_transactions),
            27
        )

    def test_get_organizers_transactions(self):
        with patch('pandas.read_csv', side_effect=(
            read_csv('revenue_app/tests/transactions_example.csv'),
            read_csv('revenue_app/tests/organizer_sales_example.csv'),
        )):
            organizers_transactions = get_organizers_transactions()
        self.assertIsInstance(
            organizers_transactions,
            DataFrame
        )
        self.assertListEqual(
            organizers_transactions.columns.tolist(),
            ['transaction_created_date',
             'email',
             'sales_flag',
             'payment_processor',
             'currency',
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
             'refund__ap_organizer__gts__epp']
        )
        self.assertEqual(
            len(organizers_transactions),
            27
        )

    @parameterized.expand([
            ('arg_domain@superdomain.org.ar', 7),
            ('some_fake_mail@gmail.com', 5),
            ('wow_fake_mail@hotmail.com', 4),
            ('another_fake_mail@gmail.com', 6),
            ('personalized_domain@wowdomain.com.br', 5),
    ])
    def test_get_organizer_transactions(self, email, expected_length):
        with patch('pandas.read_csv', side_effect=(
            read_csv('revenue_app/tests/transactions_example.csv'),
            read_csv('revenue_app/tests/organizer_sales_example.csv'),
        )):
            organizer_transactions = get_organizer_transactions(email)
        self.assertIsInstance(
            organizer_transactions,
            DataFrame
        )
        self.assertListEqual(
            organizer_transactions.columns.tolist(),
            ['transaction_created_date',
             'payment_processor',
             'currency',
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
             'refund__ap_organizer__gts__epp']
        )
        self.assertEqual(
            len(organizer_transactions),
            expected_length,
        )


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
        URL = '/organizers/search/'
        client = Client()
        response = client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/accounts/login/?next={}'.format(URL))
