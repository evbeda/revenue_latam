from datetime import date
from django.test import TestCase
from unittest.mock import patch

from pandas import read_csv
from pandas.core.frame import DataFrame
from parameterized import parameterized

from .utils import (
    filter_transactions_by_date,
    get_organizer_sales,
    get_transactions,
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
                'refund__eb_tax__epp'
            ]
        )
        self.assertNotIn(
            'sale__eb_tax__epp__1',
            transactions.columns.tolist()
        )
        self.assertEqual(
            len(transactions),
            5
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
            expected_length
        )
