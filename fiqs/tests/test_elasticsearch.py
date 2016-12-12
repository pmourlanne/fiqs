# -*- coding: utf-8 -*-

import json
import os

from fiqs.testing.utils import get_search


def test_count(elasticsearch):
    assert get_search().count() == 500


BASE_PATH = 'fiqs/testing/outputs/'


def write_output(search, name):
    result = search.execute()

    path = os.path.join(BASE_PATH, '{}.json'.format(name))
    with open(path, 'w') as f:
        json.dump(result._d_, f, indent=4, ensure_ascii=False, encoding='utf-8', sort_keys=True)


def test_write_search_outputs(elasticsearch):
    # Nothing :o
    write_output(get_search(), 'no_aggregate_no_metric')

    # Total sales by shop
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='value',
    )
    write_output(search, 'total_sales_by_shop')

    # Total sales by payment type
    search = get_search()
    search.aggs.bucket(
        'payment', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='value',
    )
    write_output(search, 'total_sales_by_payment_type')

    # Total sales by shop by payment type
    search = get_search()
    search.aggs.bucket(
        'payment', 'terms', field='payment_type',
    ).bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='value',
    )
    write_output(search, 'total_sales_by_payment_type_by_shop')

    # Number of sales

    # Number of sales by shop

    # Number of sales by day

    # Number of sales by day by shop

    # Number of different products by shop
