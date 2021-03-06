import json
from datetime import (
    date,
    datetime,
)
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

from freezegun import freeze_time
from pandas import read_csv
from pandas.core.frame import DataFrame
from pandas.testing import assert_frame_equal
from parameterized import parameterized


from revenue_app.const import (
    ARS,
    BRL,
)
from revenue_app.presto_connection import read_sql
from revenue_app.utils import (
    calc_perc_take_rate,
    clean_corrections,
    clean_organizer_refunds,
    clean_organizer_sales,
    clean_transactions,
    dataframe_to_usd,
    event_details,
    filter_transactions,
    generate_transactions_consolidation,
    get_charts_data,
    get_event_transactions,
    get_organizer_transactions,
    get_summarized_data,
    get_top_events,
    get_top_organizers,
    get_top_organizers_refunds,
    group_transactions,
    manage_transactions,
    merge_corrections,
    merge_transactions,
    payment_processor_summary,
    restore_currency,
    sales_flag_summary,
    summarize_dataframe,
)

from revenue_app.views import (
    Dashboard,
    Exchange,
    MakeQuery,
    OrganizerTransactions,
    OrganizersTransactions,
    TopEventsLatam,
    TopOrganizersLatam,
    TransactionsEvent,
    TransactionsGrouped,
    TopOrganizersRefundsLatam,
)

TRANSACTIONS_EXAMPLE_PATH = 'revenue_app/tests/transactions_example.csv'
CORRECTIONS_EXAMPLE_PATH = 'revenue_app/tests/corrections_example.csv'
ORGANIZER_SALES_EXAMPLE_PATH = 'revenue_app/tests/organizer_sales_example.csv'
ORGANIZER_REFUNDS_EXAMPLE_PATH = 'revenue_app/tests/organizer_refunds_example.csv'
TRANSACTIONS_SQL_EXAMPLE_PATH = 'revenue_app/tests/transactions_example.sql'

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
    'event_id',
    'is_refund',
    'is_sale',
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
    'organizer_name',
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

TOTAL_TRANSACTIONS_COLUMNS = [
    'eventholder_user_id',
    'transaction_created_date',
    'email',
    'organizer_name',
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
    'eb_perc_take_rate',
]

NEW_EXCHANGE_COLUMNS = [
    'local_sale__payment_amount__epp',
    'local_sale__gtf_esf__epp',
    'local_sale__eb_tax__epp',
    'local_sale__ap_organizer__gts__epp',
    'local_sale__ap_organizer__royalty__epp',
    'local_refund__payment_amount__epp',
    'local_refund__gtf_epp__gtf_esf__epp',
    'local_refund__eb_tax__epp',
    'local_refund__ap_organizer__gts__epp',
    'local_refund__ap_organizer__royalty__epp',
    'local_currency',
    'exchange_rate',
]


class UtilsTestCase(TestCase):

    @property
    def transactions(self):
        return clean_transactions(read_csv(TRANSACTIONS_EXAMPLE_PATH))

    @property
    def corrections(self):
        return clean_corrections(read_csv(CORRECTIONS_EXAMPLE_PATH))

    @property
    def organizer_sales(self):
        return clean_organizer_sales(read_csv(ORGANIZER_SALES_EXAMPLE_PATH))

    @property
    def organizer_refunds(self):
        return clean_organizer_refunds(read_csv(ORGANIZER_REFUNDS_EXAMPLE_PATH))

    @property
    def transactions_consolidation(self):
        total = merge_corrections(self.transactions, self.corrections)
        merged = merge_transactions(total, self.organizer_sales, self.organizer_refunds)
        merged = calc_perc_take_rate(merged)
        return merged.round(2)

    def test_clean_transactions(self):
        transactions = self.transactions
        self.assertIsInstance(transactions, DataFrame)
        self.assertListEqual(
            sorted(transactions.columns.tolist()),
            sorted(BASE_TRANSACTION_COLUMNS),
        )
        self.assertNotIn('sale__eb_tax__epp__1', transactions.columns.tolist())
        self.assertEqual(len(transactions), 27)

    def test_clean_corrections(self):
        corrections = self.corrections
        self.assertIsInstance(corrections, DataFrame)
        self.assertListEqual(
            sorted(corrections.columns.tolist()),
            sorted(BASE_TRANSACTION_COLUMNS),
        )
        self.assertNotIn('sale__eb_tax__epp__1', corrections.columns.tolist())
        self.assertEqual(len(corrections), 12)

    def test_clean_organizer_sales(self):
        organizer_sales = self.organizer_sales
        self.assertIsInstance(organizer_sales, DataFrame)
        self.assertListEqual(
            sorted(organizer_sales.columns.tolist()),
            sorted(BASE_ORGANIZER_SALES_COLUMNS),
        )
        self.assertNotIn('organizer_email', organizer_sales.columns.tolist())
        self.assertEqual(len(organizer_sales), 27)

    def test_clean_organizer_refunds(self):
        organizer_refunds = self.organizer_refunds
        self.assertIsInstance(organizer_refunds, DataFrame)
        self.assertListEqual(
            sorted(organizer_refunds.columns.tolist()),
            sorted(BASE_ORGANIZER_SALES_COLUMNS),
        )
        self.assertNotIn('organizer_email', organizer_refunds.columns.tolist())
        self.assertEqual(len(organizer_refunds), 5)

    def test_merge_corrections(self):
        trx_total = merge_corrections(self.transactions, self.corrections)
        self.assertIsInstance(trx_total, DataFrame)
        self.assertListEqual(
            sorted(trx_total.columns),
            sorted(BASE_TRANSACTION_COLUMNS),
        )
        self.assertEqual(len(trx_total), 27)

    def test_merge_transactions(self):
        trx_total = merge_corrections(self.transactions, self.corrections)
        merged_transactions = merge_transactions(trx_total, self.organizer_sales, self.organizer_refunds)
        self.assertIsInstance(merged_transactions, DataFrame)
        self.assertListEqual(
            sorted(merged_transactions.columns),
            sorted(MERGED_TRANSACTIONS_COLUMNS),
        )
        self.assertEqual(len(merged_transactions), 27)

    def test_generate_transactions_consolidation(self):
        transactions = generate_transactions_consolidation(
            self.transactions,
            self.corrections,
            self.organizer_sales,
            self.organizer_refunds,
        )
        self.assertIsInstance(transactions, DataFrame)
        self.assertListEqual(
            sorted(transactions.columns),
            sorted(TOTAL_TRANSACTIONS_COLUMNS),
        )
        self.assertEqual(len(transactions), 27)

    @parameterized.expand([
        ('day', 22),
        ('week', 5),
        ('semi_month', 3),
        ('month', 2),
        ('quarter', 2),
        ('eventholder_user_id', 5),
        (['eventholder_user_id', 'email'], 5),
        ('event_id', 5),
        (['payment_processor'], 3),
        ('payment_processor', 4),
        ('sales_flag', 3),
        ('sales_vertical', 2),
        ('vertical', 5),
        ('sub_vertical', 5),
        ('currency', 2),
    ])
    def test_group_transactions(self, by, expected_length):
        grouped = group_transactions(
            merge_transactions(self.transactions, self.organizer_sales, self.organizer_refunds),
            by,
        )
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
        ({'groupby': 'day'}, 22),
        ({'groupby': 'day', 'start_date': '2018-08-02', 'end_date': '2018-08-15'}, 14),
        ({'groupby': 'week'}, 5),
        ({'groupby': 'semi_month'}, 3),
        ({'groupby': 'month'}, 2),
        ({'groupby': 'quarter'}, 2),
        ({'groupby': 'eventholder_user_id'}, 5),
        ({'groupby': ['eventholder_user_id', 'email']}, 5),
        ({'groupby': 'event_id'}, 5),
        ({'groupby': ['payment_processor']}, 3),
        ({'groupby': 'payment_processor'}, 4),
        ({'groupby': 'sales_flag'}, 3),
        ({'groupby': 'sales_vertical'}, 2),
        ({'groupby': 'vertical'}, 5),
        ({'groupby': 'sub_vertical'}, 5),
        ({'groupby': 'currency'}, 2),
    ])
    def test_manage_transactions(self, kwargs, expected_length):
        organizer_transactions = manage_transactions(
            self.transactions_consolidation,
            **kwargs,
        )
        self.assertIsInstance(organizer_transactions, DataFrame)
        self.assertEqual(len(organizer_transactions), expected_length)

    @parameterized.expand([
        ('66220941', 5, 10500),
        ('98415193', 6, 13608),
        ('17471621', 4, 4500),
        ('35210860', 5, 117),
        ('88128252', 7, 2405),
    ])
    def test_get_event_transactions(self, event_id, transactions_qty, tickets_qty):
        transactions, details, sales_refunds, net_sales_refunds = get_event_transactions(
            self.transactions_consolidation,
            event_id,
        )
        self.assertIsInstance(transactions, DataFrame)
        self.assertEqual(len(transactions), transactions_qty)
        self.assertEqual(details['PaidTix'], tickets_qty)

    @parameterized.expand([
        ('497321858', 5, 10500),
        ('696421958', 6, 13608),
        ('434444537', 4, 4500),
        ('506285738', 5, 117),
        ('634364434', 7, 2405),
    ])
    def test_get_organizer_transactions(self, eventholder_user_id, transactions_qty, tickets_qty):
        transactions, details, sales_refunds, net_sales_refunds = get_organizer_transactions(
            self.transactions_consolidation,
            eventholder_user_id,
        )
        self.assertIsInstance(transactions, DataFrame)
        self.assertEqual(len(transactions), transactions_qty)
        self.assertEqual(details['PaidTix'], tickets_qty)

    def test_get_top_ten_organizers(self):
        trx = self.transactions_consolidation
        top_ars = get_top_organizers(
                trx[trx['currency'] == ARS],
            )
        top_brl = get_top_organizers(
                trx[trx['currency'] == BRL],
            )
        self.assertIsInstance(top_ars, DataFrame)
        self.assertIsInstance(top_brl, DataFrame)
        self.assertEqual(len(top_ars), 3)
        self.assertEqual(len(top_brl), 4)

    def test_get_top_ten_organizers_refunds(self):
        trx = self.transactions_consolidation
        top_ars = get_top_organizers_refunds(
                trx[trx['currency'] == ARS],
            )
        top_brl = get_top_organizers_refunds(
                trx[trx['currency'] == BRL],
            )
        self.assertIsInstance(top_ars, DataFrame)
        self.assertIsInstance(top_brl, DataFrame)
        self.assertEqual(len(top_ars), 3)
        self.assertEqual(len(top_brl), 4)

    def test_get_top_ten_events(self):
        trx = self.transactions_consolidation
        top_ars = get_top_events(
                trx[trx['currency'] == ARS],
            )
        top_brl = get_top_events(
                trx[trx['currency'] == BRL],
            )
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
        filtered_transactions = filter_transactions(self.transactions_consolidation, **kwargs)
        self.assertEqual(len(filtered_transactions), expected_length)

    @parameterized.expand([
        ('497321858', {
            'PaidTix': 10500,
            'sale__payment_amount__epp': 1822.12,
            'sale__eb_tax__epp': 24.68,
            'sale__ap_organizer__gts__epp': 1680.0,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 117.44,
            'refund__payment_amount__epp': -911.06,
            'refund__gtf_epp__gtf_esf__epp': -58.72,
            'refund__ap_organizer__gts__epp': -840.0,
            'refund__eb_tax__epp': -12.34,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('634364434', {
            'PaidTix': 2405,
            'sale__payment_amount__epp': 21149.25,
            'sale__eb_tax__epp': 286.2,
            'sale__ap_organizer__gts__epp': 19500,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 1363.05,
            'refund__payment_amount__epp': -4229.85,
            'refund__gtf_epp__gtf_esf__epp': -272.61,
            'refund__ap_organizer__gts__epp': -3900,
            'refund__eb_tax__epp': -57.24,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('696421958', {
            'PaidTix': 13608,
            'sale__payment_amount__epp': 4180.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 3887.8,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 292.2,
            'refund__payment_amount__epp': -1045.0,
            'refund__gtf_epp__gtf_esf__epp': -73.05,
            'refund__ap_organizer__gts__epp': -971.95,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('434444537', {
            'PaidTix': 4500,
            'sale__payment_amount__epp': 891.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 810.0,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 81.0,
            'refund__payment_amount__epp': -297.0,
            'refund__gtf_epp__gtf_esf__epp': -27.0,
            'refund__ap_organizer__gts__epp': -270.0,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
        ('506285738', {
            'PaidTix': 117,
            'sale__payment_amount__epp': 7260.0,
            'sale__eb_tax__epp': 0,
            'sale__ap_organizer__gts__epp': 6679.2,
            'sale__ap_organizer__royalty__epp': 0,
            'sale__gtf_esf__epp': 580.8,
            'refund__payment_amount__epp': -3630.0,
            'refund__gtf_epp__gtf_esf__epp': -290.4,
            'refund__ap_organizer__gts__epp': -3339.6,
            'refund__eb_tax__epp': 0,
            'refund__ap_organizer__royalty__epp': 0,
        }),
    ])
    def test_summarize_dataframe(self, eventholder_user_id, expected_total):
        organizer = manage_transactions(
            self.transactions_consolidation,
            eventholder_user_id=eventholder_user_id,
        )
        total_organizer = summarize_dataframe(organizer)
        self.assertEqual(total_organizer, expected_total)

    @parameterized.expand([
        ('66220941', '497321858', {
            'Event ID': '66220941',
            'Event Title': 'Event Name 2',
            'Organizer ID': '497321858',
            'Organizer Name': 'Fake 1',
            'Email': 'some_fake_mail@gmail.com',
            'Sales Flag': 'sales',
            'Sales Vertical': 'Argentina',
        }),
        ('98415193', '98415193', {
            'Event ID': '98415193',
            'Event Title': 'Event Name 4',
            'Organizer ID': '98415193',
            'Organizer Name': 'Fake 2',
            'Email': 'another_fake_mail@gmail.com',
            'Sales Flag': 'SSO',
            'Sales Vertical': 'Brazil',
        }),
        ('17471621', '434444537', {
            'Event ID': '17471621',
            'Event Title': 'Event Name 3',
            'Organizer ID': '434444537',
            'Organizer Name': 'Wow Such Fake',
            'Email': 'wow_fake_mail@hotmail.com',
            'Sales Flag': 'sales',
            'Sales Vertical': 'Brazil',
        }),
        ('35210860', '506285738', {
            'Event ID': '35210860',
            'Event Title': 'Event Name 5',
            'Organizer ID': '506285738',
            'Organizer Name': 'Br Fake',
            'Email': 'personalized_domain@wowdomain.com.br',
            'Sales Flag': 'SSO',
            'Sales Vertical': 'Brazil',
        }),
        ('88128252', '634364434', {
            'Event ID': '88128252',
            'Event Title': 'Event Name 1',
            'Organizer ID': '634364434',
            'Organizer Name': 'Ar Fake',
            'Email': 'arg_domain@superdomain.org.ar',
            'Sales Flag': 'sales',
            'Sales Vertical': 'Argentina',
        }),
    ])
    def test_event_details(self, event_id, eventholder_user_id, details_organizer):
        response = event_details(self.transactions_consolidation, event_id, eventholder_user_id)
        self.assertEqual(response, details_organizer)

    @parameterized.expand([
        ('Argentina', 'Totals', 'Organizers', 2),
        ('Argentina', 'Totals', 'Events', 2),
        ('Argentina', 'Totals', 'PaidTix', 12905),
        ('Argentina', 'Gross', 'GTF', 1480.49),
        ('Argentina', 'Gross', 'GTV', 22971.37),
        ('Argentina', 'Gross', 'ATV', 1.67),
        ('Argentina', 'Gross', 'Avg EB Take Rate', 6.44),
        ('Argentina', 'Net', 'GTF', 1149.16),
        ('Argentina', 'Net', 'GTV', 17830.46),
        ('Argentina', 'Net', 'ATV', 1.29),
        ('Argentina', 'Net', 'Avg EB Take Rate', 6.44),
        ('Brazil', 'Totals', 'Organizers', 3),
        ('Brazil', 'Totals', 'Events', 3),
        ('Brazil', 'Totals', 'PaidTix', 18225),
        ('Brazil', 'Gross', 'GTF', 954.0),
        ('Brazil', 'Gross', 'GTV', 12331.0),
        ('Brazil', 'Gross', 'ATV', 0.62),
        ('Brazil', 'Gross', 'Avg EB Take Rate', 7.74),
        ('Brazil', 'Net', 'GTF', 563.55),
        ('Brazil', 'Net', 'GTV', 7359.0),
        ('Brazil', 'Net', 'ATV', 0.37),
        ('Brazil', 'Net', 'Avg EB Take Rate', 7.66),
    ])
    def test_get_summarized_data(self, country, group, data, expected):
        summarized_data = get_summarized_data(
            self.transactions_consolidation,
        )
        self.assertEqual(summarized_data[country][group][data], expected)

    @parameterized.expand([
        ('payment_processor', 'gtv'),
        ('payment_processor', 'gtf'),
        ('sales_flag', 'gtf'),
        ('sales_flag', 'organizers'),
    ])
    def test_get_charts_data_with_valid_params(self, type, filter):
        trx = self.transactions_consolidation
        response = get_charts_data(trx, type, filter)
        self.assertIn('data', response['Argentina'])
        self.assertIn('data', response['Brazil'])

    @parameterized.expand([
        ('payment_processor', 'organizers'),
        ('sales_flag', 'gts'),
    ])
    def test_get_charts_data_with_invalid_params(self, type, filter):
        trx = self.transactions_consolidation
        response = get_charts_data(trx, type, filter)
        self.assertEqual(response, {})

    @parameterized.expand([
        ('gtv', ),
        ('gtf', ),
    ])
    def test_payment_processor_summary(self, filter):
        trx = self.transactions_consolidation
        trx_currencies = {
            'Argentina': trx[trx['currency'] == ARS],
            'Brazil': trx[trx['currency'] == BRL],
        }
        response = payment_processor_summary(trx_currencies, filter)
        self.assertIsInstance(response['Argentina']['data'], list)
        self.assertIsInstance(response['Brazil']['data'], list)

    @parameterized.expand([
        ('gtf', ),
        ('organizers', ),
    ])
    def test_sales_flag_summary(self, filter):
        trx = self.transactions_consolidation
        trx_currencies = {
            'Argentina': trx[trx['currency'] == ARS],
            'Brazil': trx[trx['currency'] == BRL],
        }
        response = sales_flag_summary(trx_currencies, filter)
        self.assertIsInstance(response['Argentina']['data'], list)
        self.assertIsInstance(response['Brazil']['data'], list)

    @parameterized.expand([
        (60.01, 5, 2848.99),
        (59.95, 4.90, 2899.71),
    ])
    def test_dataframe_to_usd(self, ars, brl, expected_sum_gtv):
        trx = self.transactions_consolidation
        exchange_data = {
            'August': {
                'ars_to_usd': ars,
                'brl_to_usd': brl,
            }
        }
        converted = dataframe_to_usd(trx, exchange_data)
        for column in NEW_EXCHANGE_COLUMNS:
            self.assertNotIn(column, trx.columns)
            self.assertIn(column, converted.columns)
        self.assertIsInstance(converted, DataFrame)
        self.assertEqual(converted.sale__payment_amount__epp.sum().round(2), expected_sum_gtv)

    def test_restore_currency(self):
        TEST_VALUE = 1
        trx = self.transactions_consolidation
        exchange_data = {
            'August': {
                'ars_to_usd': TEST_VALUE,
                'brl_to_usd': TEST_VALUE,
            }
        }
        converted = dataframe_to_usd(trx, exchange_data)
        restored = restore_currency(converted)
        for column in NEW_EXCHANGE_COLUMNS:
            self.assertIn(column, converted.columns)
            self.assertNotIn(column, restored.columns)
        self.assertIsInstance(restored, DataFrame)


class ViewsTest(TestCase):
    def setUp(self):
        self.client = Client()

    def load_dataframes(self):
        session = self.client.session
        transactions = read_csv(TRANSACTIONS_EXAMPLE_PATH)
        corrections = read_csv(CORRECTIONS_EXAMPLE_PATH)
        organizer_sales = read_csv(ORGANIZER_SALES_EXAMPLE_PATH)
        organizer_refunds = read_csv(ORGANIZER_REFUNDS_EXAMPLE_PATH)
        session['transactions'] = generate_transactions_consolidation(
            transactions,
            corrections,
            organizer_sales,
            organizer_refunds,
        )
        session['query_info'] = {
            'run_time': datetime.now(),
            'start_date': date(2018, 8, 1),
            'end_date': date(2018, 8, 30),
        }
        session['exchange_data'] = None
        session.save()

    def test_dashboard_view_returns_200(self):
        URL = reverse('dashboard')
        context_dict = [
            ('Argentina', 'Totals', 'Organizers'),
            ('Argentina', 'Totals', 'Events'),
            ('Argentina', 'Totals', 'PaidTix'),
            ('Argentina', 'Gross', 'GTF'),
            ('Argentina', 'Gross', 'GTV'),
            ('Argentina', 'Gross', 'ATV'),
            ('Argentina', 'Gross', 'Avg EB Take Rate'),
            ('Argentina', 'Net', 'GTF'),
            ('Argentina', 'Net', 'GTV'),
            ('Argentina', 'Net', 'ATV'),
            ('Argentina', 'Net', 'Avg EB Take Rate'),
            ('Brazil', 'Totals', 'Organizers'),
            ('Brazil', 'Totals', 'Events'),
            ('Brazil', 'Totals', 'PaidTix'),
            ('Brazil', 'Gross', 'GTF'),
            ('Brazil', 'Gross', 'GTV'),
            ('Brazil', 'Gross', 'ATV'),
            ('Brazil', 'Gross', 'Avg EB Take Rate'),
            ('Brazil', 'Net', 'GTF'),
            ('Brazil', 'Net', 'GTV'),
            ('Brazil', 'Net', 'ATV'),
            ('Brazil', 'Net', 'Avg EB Take Rate'),
        ]
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Dashboard.template_name)
        for elem in context_dict:
            self.assertIn(elem[2], response.context['summarized_data'][elem[0]][elem[1]])

    def test_dashboard_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('dashboard')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    @parameterized.expand([
        ({},),
        ({'start_date': '2018-08-02'},),
        ({'start_date': '2018-08-02', 'end_date': '2018-08-05'},),
        ({'start_date': '2018-08-05', 'end_date': '2018-08-02'},),
        ({'end_date': '2018-08-05'},),
    ])
    def test_organizers_transactions_view_returns_200(self, kwargs):
        URL = reverse('organizers-transactions')
        self.load_dataframes()
        response = self.client.get(URL, kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizersTransactions.template_name)

    def test_organizers_transactions_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('organizers-transactions')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    @parameterized.expand([
        (497321858, 5),
        (696421958, 6),
        (434444537, 4),
        (506285738, 5),
        (634364434, 7),
    ])
    def test_organizer_transactions_view_returns_200(self, eventholder_user_id, expected_length):
        URL = reverse('organizer-transactions', kwargs={'eventholder_user_id': eventholder_user_id})
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(len(response.context['transactions']), expected_length)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], OrganizerTransactions.template_name)

    @parameterized.expand([
        (497321858,),
        (696421958,),
        (434444537,),
        (506285738,),
        (634364434,),
    ])
    def test_organizer_transactions_view_returns_302_if_doesnt_have_queries(self, eventholder_user_id):
        URL = reverse('organizer-transactions', kwargs={'eventholder_user_id': eventholder_user_id})
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    def test_top_organizers_view_returns_200(self):
        URL = reverse('top-organizers')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(len(response.context['top_ars']), 3)
        self.assertEqual(len(response.context['top_brl']), 4)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopOrganizersLatam.template_name)

    def test_top_organizers_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('top-organizers')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    def test_top_organizers_refunds_view_returns_200(self):
        URL = reverse('top-organizers-refunds')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(len(response.context['top_ars']), 3)
        self.assertEqual(len(response.context['top_brl']), 4)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopOrganizersRefundsLatam.template_name)

    def test_top_organizers_refunds_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('top-organizers-refunds')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    def test_top_events_view_returns_200(self):
        URL = reverse('top-events')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(len(response.context['top_event_ars']), 3)
        self.assertEqual(len(response.context['top_event_brl']), 4)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TopEventsLatam.template_name)

    def test_top_events_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('top-events')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    @parameterized.expand([
        (66220941, 5),
        (98415193, 6),
        (17471621, 4),
        (35210860, 5),
        (88128252, 7),
    ])
    def test_events_transactions_view_returns_200(self, event_id, expected_length):
        URL = reverse('event-details', kwargs={'event_id': event_id})
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(len(response.context['transactions']), expected_length)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsEvent.template_name)

    @parameterized.expand([
        (66220941, 5),
        (98415193, 6),
        (17471621, 4),
        (35210860, 5),
        (88128252, 7),
    ])
    def test_events_transactions_view_returns_302_if_doesnt_have_queries(self, event_id, expected_length):
        URL = reverse('event-details', kwargs={'event_id': event_id})
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    def test_transactions_grouped_view_returns_200(self):
        kwargs = {'groupby': 'day'}
        URL = reverse('transactions-grouped')
        self.load_dataframes()
        response = self.client.get(URL, kwargs)
        self.assertEqual(len(response.context['transactions']), 22)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], TransactionsGrouped.template_name)

    def test_transactions_grouped_view_returns_302_if_doesnt_have_queries(self):
        kwargs = {'groupby': 'day'}
        URL = reverse('transactions-grouped')
        response = self.client.get(URL, kwargs)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    @parameterized.expand([
        (reverse('organizers-transactions'), 'transactions'),
        (reverse('organizer-transactions', kwargs={'eventholder_user_id': 497321858}), 'organizer_497321858'),
        (reverse('transactions-grouped') + '?groupby=week', 'transactions_grouped_by_week'),
        (reverse('event-details', kwargs={'event_id': 98415193}), 'event_98415193'),
    ])
    def test_download_csv(self, url_from, csv_name):
        URL = reverse('download-csv', kwargs={'csv_name': csv_name})
        self.load_dataframes()
        # load an URL that set session['transactions']
        self.client.get(url_from)
        response = self.client.get(URL)
        self.assertEqual(response['Content-Type'], 'text/csv')
        self.assertIn('attachment; filename=', response['Content-Disposition'])
        self.assertIn(csv_name, response['Content-Disposition'])
        self.assertIn('.csv', response['Content-Disposition'])
        self.assertEqual(response.status_code, 200)

    @parameterized.expand([
        (reverse('organizers-transactions'), 'transactions'),
        (reverse('organizer-transactions', kwargs={'eventholder_user_id': 497321858}), 'organizer_497321858'),
        (reverse('transactions-grouped') + '?groupby=week', 'transactions_grouped_by_week'),
        (reverse('event-details', kwargs={'event_id': 98415193}), 'event_98415193'),
    ])
    def test_download_excel(self, url_from, xls_name):
        URL = reverse('download-excel', kwargs={'xls_name': xls_name})
        self.load_dataframes()
        # load an URL that set session['transactions']
        self.client.get(url_from)
        response = self.client.get(URL)
        self.assertEqual(response['Content-Type'], 'application/ms-excel')
        self.assertIn('attachment; filename=', response['Content-Disposition'])
        self.assertIn(xls_name, response['Content-Disposition'])
        self.assertIn('.xls', response['Content-Disposition'])
        self.assertEqual(response.status_code, 200)

    def test_dashboard_summary_with_no_data_returns_400(self):
        URL = reverse('json_dashboard_summary')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )

    @parameterized.expand([
        ('payment_processor', 'gtv'),
        ('payment_processor', 'gtf'),
        ('sales_flag', 'gtf'),
        ('sales_flag', 'gtv'),
        ('sales_flag', 'organizers'),
    ])
    def test_dashboard_summary_with_valid_data_returns_200(self, type, filter):
        URL = '{0}?type={1}&filter={2}'.format(reverse('json_dashboard_summary'), type, filter)
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )
        self.assertIn('Argentina', json.loads(response.content))
        self.assertIn('Brazil', json.loads(response.content))

    @parameterized.expand([
        ('payment_processor', 'organizers'),
        ('sales_flag', 'gts'),
    ])
    def test_dashboard_summary_with_invalid_data_returns_200(self, type, filter):
        URL = '{0}?type={1}&filter={2}'.format(reverse('json_dashboard_summary'), type, filter)
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )
        self.assertEqual(json.loads(response.content), {})
        self.assertEqual(json.loads(response.content), {})

    def test_top_events_json_data(self):
        URL = reverse('json_top_events')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )

    def test_top_organizers_refunds_json_data(self):
        URL = reverse('json_top_organizers_refunds')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )

    def test_top_organizers_json_data(self):
        URL = reverse('json_top_organizers')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            str(response._headers['content-type']),
            "('Content-Type', 'application/json')",
        )

    def test_make_query_view_returns_200_but_does_not_make_query(self):
        URL = reverse('make-query')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], MakeQuery.template_name)
        self.assertNotContains(response, 'ran successfully.')

    @parameterized.expand([
        ("2019-01-30", '2018-12-01'),
        ("2019-02-14", '2019-01-01'),
        ("2019-03-31", '2019-02-01'),
    ])
    def test_make_query_view_initial_has_previous_month_start(self, time, expected):
        with freeze_time(time):
            URL = reverse('make-query')
            response = self.client.get(URL)
        self.assertEqual(str(response.context['form'].initial['start_date']), expected)

    @parameterized.expand([
        ("2019-01-30", '2018-12-31'),
        ("2019-02-14", '2019-01-31'),
        ("2019-03-31", '2019-02-28'),
        ("2020-03-05", '2020-02-29'),
        ("2019-05-10", '2019-04-30'),
    ])
    def test_make_query_view_initial_has_previous_month_end(self, time, expected):
        with freeze_time(time):
            URL = reverse('make-query')
            response = self.client.get(URL)
        self.assertEqual(str(response.context['form'].initial['end_date']), expected)

    def test_make_query_view_returns_200_and_makes_queries(self):
        queries = ['transactions', 'corrections', 'organizer_sales', 'organizer_refunds']
        for query_name in queries:
            self.assertNotIn(query_name, self.client.session)
        kwargs = {
            'start_date': '2018-08-02',
            'end_date': '2018-08-05',
            'okta_username': 'fakename',
            'okta_password': 'fakepass',
        }
        URL = reverse('make-query')
        with patch('revenue_app.views.make_query', side_effect=(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(CORRECTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_REFUNDS_EXAMPLE_PATH),
        )):
            response = self.client.post(URL, kwargs)
        expected = generate_transactions_consolidation(
            read_csv(TRANSACTIONS_EXAMPLE_PATH),
            read_csv(CORRECTIONS_EXAMPLE_PATH),
            read_csv(ORGANIZER_SALES_EXAMPLE_PATH),
            read_csv(ORGANIZER_REFUNDS_EXAMPLE_PATH),
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], MakeQuery.template_name)
        for query_name in queries:
            self.assertContains(response, f'{query_name} ran successfully')
        assert_frame_equal(self.client.session['transactions'], expected)

    @parameterized.expand([
        ({}, 'This field is required.'),
        (
            {
                'okta_username': 'fakename',
                'okta_password': 'fakepass',
                'start_date': '2018-08-06',
                'end_date': '2018-08-05',
            },
            'End date must be greater or equal than start date.',
        ),
        (
            {
                'okta_username': 'fakename',
                'okta_password': 'fakepass',
                'start_date': '2018-05-01',
                'end_date': '2018-08-05',
            },
            'Time between End and Start date can&#39;t be over 3 months.',
        ),
        (
            {
                'okta_username': 'fakename',
                'okta_password': 'fakepass',
                'start_date': '2018-09-14',
                'end_date': '2018-09-15',
            },
            "can&#39;t be greater than today.",
        ),
    ])
    def test_make_query_form_validation(self, kwargs, error_message):
        URL = reverse('make-query')
        with freeze_time("2018-08-10"):
            response = self.client.post(URL, kwargs)
        self.assertContains(response, error_message)

    def test_exchange_view_returns_302_if_doesnt_have_queries(self):
        URL = reverse('exchange')
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse('make-query'))

    def test_exchange_view_returns_200(self):
        URL = reverse('exchange')
        self.load_dataframes()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Exchange.template_name)

    @parameterized.expand([
        ({}, 'This field is required.'),
        (
            {
                'August-ars_to_usd': 'invalid',
                'August-brl_to_usd': 'invalid',
            },
            'Enter a number.',
        ),
        (
            {
                'August-ars_to_usd': -23,
                'August-brl_to_usd': -45,
            },
            'Ensure this value is greater than or equal to 0.01.',
        ),
    ])
    def test_exchange_view_with_invalid_data(self, kwargs, error_message):
        URL = reverse('exchange')
        self.load_dataframes()
        response = self.client.post(URL, kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Exchange.template_name)
        self.assertContains(response, error_message)

    def test_exchange_view_populates_form_if_already_loaded(self):
        kwargs = {
            'August-ars_to_usd': 60.01,
            'August-brl_to_usd': 5.02,
        }

        URL = reverse('exchange')
        self.load_dataframes()
        self.client.post(URL, kwargs)
        response = self.client.get(URL)
        self.assertEqual(response.context['forms']['August'].initial['ars_to_usd'], 60.01)
        self.assertEqual(response.context['forms']['August'].initial['brl_to_usd'], 5.02)

    def test_exchange_view_returns_200_and_makes_conversion(self):
        kwargs = {
            'August-ars_to_usd': 60.01,
            'August-brl_to_usd': 5.02,
        }

        URL = reverse('exchange')
        self.load_dataframes()
        response = self.client.post(URL, kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.template_name[0], Exchange.template_name)
        trx = self.client.session['transactions'].copy()
        for column in NEW_EXCHANGE_COLUMNS:
            self.assertIn(column, trx.columns)

    def test_restore_local_currency_view_redirects_to_dashboard(self):
        TEST_VALUE = 1
        exchange_data = {
            'August': {
                'ars_to_usd': TEST_VALUE,
                'brl_to_usd': TEST_VALUE,
            }
        }
        URL = reverse('restore-currency')
        self.load_dataframes()
        session = self.client.session
        trx = session.get('transactions').copy()
        converted = dataframe_to_usd(trx, exchange_data)
        session['transactions'] = converted
        session.save()
        response = self.client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, '/')
        for column in NEW_EXCHANGE_COLUMNS:
            self.assertIn(column, session['transactions'].columns)


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

    @parameterized.expand([
        ({'value': 'day'}, 'Day'),
        ({'value': 'week'}, 'Week'),
        ({'value': 'semi_month'}, 'Semi Month'),
        ({'value': 'month'}, 'Month'),
        ({'value': 'quarter'}, 'Quarter'),
        ({'value': 'eventholder_user_id'}, 'Eventholder User Id'),
        ({'value': 'email'}, 'Email'),
        ({'value': 'event_id'}, 'Event Id'),
        ({'value': 'payment_processor'}, 'Payment Processor'),
        ({'value': 'sales_flag'}, 'Sales Flag'),
        ({'value': 'sales_vertical'}, 'Sales Vertical'),
        ({'value': 'vertical'}, 'Vertical'),
        ({'value': 'sub_vertical'}, 'Sub Vertical'),
        ({'value': 'currency'}, 'Currency'),
    ])
    def test_title(self, context, expected):
        rendered = self.render_template(
            '{% load string_filters %}'
            '{{value|title}}',
            context
        )
        self.assertEqual(rendered, expected)

    @parameterized.expand([
        ({'value': '2'}, 'Not number type.'),
        ({'value': 2}, 'Is number type.'),
        ({'value': 2.5}, 'Is number type.'),
        ({'value': '2.5'}, 'Not number type.'),
    ])
    def test_numeric(self, context, expected):
        rendered = self.render_template(
            '{% load number_filters %}'
            '{% if value|is_numeric %}'
            'Is number type.'
            '{% else %}'
            'Not number type.'
            '{% endif %}',
            context
        )
        self.assertEqual(rendered, expected)

    @parameterized.expand([
        ({'key': 'invalid key'}, 'None'),
        ({'key': 'eventholder_user_id'}, 'from transactions query'),
        ({'key': 'eb_perc_take_rate'}, 'sale__gtf_esf__epp / sale__payment_amount__epp * 100'),
        ({'key': 'net__ap_organizer__gts__epp'}, 'sale__ap_organizer__gts__epp + refund__ap_organizer__gts__epp'),
    ])
    def test_glossary(self, context, expected):
        rendered = self.render_template(
            '{% load glossary_filters %}'
            '{% glossary key %}',
            context
        )
        self.assertEqual(rendered, expected)


class PrestoQueriesTestCase(TestCase):
    def test_read_sql(self):
        expected = '''SELECT
column_one,
column_two,
sum(column_three/100.00) AS column_three
FROM some_table
JOIN other_table o ON o.id = column_one
WHERE column_date >= CAST('2019-12-10' AS DATE)
AND column_date <= CAST('2019-12-25' AS DATE)
AND column_three IN ('case1', 'case2')
GROUP BY 1, 2
ORDER BY 1, 2
'''
        with patch("revenue_app.presto_connection.open", return_value=open(TRANSACTIONS_SQL_EXAMPLE_PATH)):
            readed = read_sql('transactions')
        self.assertEqual(readed, expected)
