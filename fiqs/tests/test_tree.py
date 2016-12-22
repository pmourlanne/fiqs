# -*- coding: utf-8 -*-

from fiqs import flatten_result
from fiqs.tests.conftest import load_output


def test_no_aggregate_no_metric():
    expected = []
    assert flatten_result(load_output('no_aggregate_no_metric')) == expected


def test_nb_sales_by_shop():
    lines = flatten_result(load_output('nb_sales_by_shop'))

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation is present
        assert 'shop' in line
        assert type(line['shop']) == int


def test_total_sales_by_shop():
    lines = flatten_result(load_output('total_sales_by_shop'))

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'shop' in line
        assert type(line['shop']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float


def test_total_sales_by_payment_type():
    lines = flatten_result(load_output('total_sales_by_payment_type'))

    assert len(lines) == 3  # One for each payment type

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'payment' in line
        assert type(line['payment']) == unicode
        assert 'total_sales' in line
        assert type(line['total_sales']) == float


def test_total_sales_by_payment_type_by_shop():
    lines = flatten_result(load_output('total_sales_by_payment_type_by_shop'))

    assert len(lines) == 3 * 10  # One for each and one for each payment type

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Both aggregations and metric are present
        assert 'payment' in line
        assert type(line['payment']) == unicode
        assert 'shop' in line
        assert type(line['shop']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float

    # Ten lines for each payment type
    for payment_type in ['cash', 'wire_transfer', 'store_credit', ]:
        payment_type_lines = [l for l in lines if l['payment'] == payment_type]
        assert len(payment_type_lines) == 10

        # For each payment type, lines are ordered by doc_count
        assert payment_type_lines == sorted(
            payment_type_lines, key=(lambda l: l['doc_count']), reverse=True)


def test_total_sales_day_by_day():
    lines = flatten_result(load_output('total_sales_day_by_day'))

    assert len(lines) == 31  # Number of days in the data's month

    # Lines are sorted by date
    assert lines == sorted(lines, key=(lambda l: l['day']))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # The aggregation and the metric are present
        assert 'day' in line
        assert type(line['day']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float


def test_total_sales_day_by_day_by_shop_and_by_client():
    lines = flatten_result(load_output('total_sales_day_by_day_by_shop_and_by_client'))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Day by day aggregation is always present
        assert 'day' in line
        assert type(line['day']) == int
        # Metric is always present
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        # Either shop aggregation is present
        try:
            assert 'shop' in line
            assert type(line['shop']) == int
        # Or payment type aggregation is present
        except AssertionError:
            assert 'payment' in line
            assert type(line['payment']) == unicode

    # Documents are counted once in the payment aggregations, once in the shop aggregation
    sum([line['doc_count']]) == 2 * 500

    # There are more than 31 buckets within the shop buckets
    assert len([l for l in lines if 'shop' in l]) >= 31
    # There are more than 31 buckets within the payment buckets
    assert len([l for l in lines if 'payment' in l]) >= 31


def test_total_and_avg_sales_by_shop():
    lines = flatten_result(load_output('total_and_avg_sales_by_shop'))

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metrics are present
        assert 'shop' in line
        assert type(line['shop']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        assert 'avg_sales' in line
        assert type(line['avg_sales']) == float


def test_total_sales():
    lines = flatten_result(load_output('total_sales'))

    assert len(lines) == 1

    line = lines[0]
    # Only metric is present
    assert line.keys() == ['total_sales']
    assert type(line['total_sales']) == float
