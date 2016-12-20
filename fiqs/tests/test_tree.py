# -*- coding: utf-8 -*-

from fiqs import flatten_result
from fiqs.tests.conftest import load_output


def test_no_aggregate_no_metric():
    expected = []
    assert flatten_result(load_output('no_aggregate_no_metric')) == expected


def test_total_sales_by_shop():
    lines = flatten_result(load_output('total_sales_by_shop'))

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Both aggregations are present
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
        # Both aggregations are present
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
        # All three aggregations are present
        assert 'payment' in line
        assert type(line['payment']) == unicode
        assert 'shop' in line
        assert type(line['shop']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float

    # Ten lines for each payment type
    for payment_type in ['cash', 'cc', 'store_credit', ]:
        payment_type_lines = [l for l in lines if l['payment'] == payment_type]
        assert len(payment_type_lines) == 10

        # For each payment type, lines are ordered by doc_count
        assert payment_type_lines == sorted(
            payment_type_lines, key=(lambda l: l['doc_count']), reverse=True)
