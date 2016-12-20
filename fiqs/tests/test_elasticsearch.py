# -*- coding: utf-8 -*-

import pytest

from fiqs.testing.utils import get_search
from fiqs.tests.conftest import write_output


@pytest.mark.docker
def test_count(elasticsearch):
    assert get_search().count() == 500


@pytest.mark.docker
def test_write_search_outputs(elasticsearch):
    # Nothing :o
    write_output(get_search(), 'no_aggregate_no_metric')

    # Total sales by shop
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_by_shop')

    # Total sales by payment type
    search = get_search()
    search.aggs.bucket(
        'payment', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_by_payment_type')

    # Total sales by shop by payment type
    search = get_search()
    search.aggs.bucket(
        'payment', 'terms', field='payment_type',
    ).bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_by_payment_type_by_shop')

    # Number of sales

    # Number of sales by shop

    # Number of sales by day

    # Number of sales by day by shop

    # Number of different products by shop
