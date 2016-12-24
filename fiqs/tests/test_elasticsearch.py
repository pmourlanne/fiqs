# -*- coding: utf-8 -*-

import pytest

from fiqs.testing.utils import get_search
from fiqs.tests.conftest import write_output


@pytest.mark.docker
def test_count(elasticsearch):
    assert get_search().count() == 500


@pytest.mark.docker
def test_write_nested_search_output(elasticsearch):
    # Average product price by product type
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_type', 'terms', field='products.product_type',
    ).metric(
        'avg_product_price', 'avg', field='products.product_price',
    )
    write_output(search, 'avg_product_price_by_product_type')

    # Average part price by part
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    write_output(search, 'avg_part_price_by_part')

    # Average part price by product
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product', 'terms', field='products.product_id',
    ).bucket(
        'paths', 'nested', path='products.parts',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    write_output(search, 'avg_part_price_by_product')

    # Average part price by product by part
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    write_output(search, 'avg_part_price_by_product_by_part')

    # Average product price by shop by product
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    ).bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_type', 'terms', field='products.product_type',
    ).metric(
        'avg_product_price', 'avg', field='products.product_price',
    )
    write_output(search, 'avg_product_price_by_shop_by_product_type')

    # Average part price by product and by part
    search = get_search()
    products_bucket = search.aggs.bucket(
        'products', 'nested', path='products',
    )
    products_bucket.bucket(
        'product', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    products_bucket.bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    write_output(search, 'avg_part_price_by_product_and_by_part')


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

    # Total sales day by day
    search = get_search()
    search.aggs.bucket(
        'day', 'date_histogram', field='timestamp', interval='1d',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_day_by_day')

    # Number of sales by shop
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    )
    write_output(search, 'nb_sales_by_shop')

    # Total sales day by day, by shop and by payment type
    search = get_search()
    agg = search.aggs.bucket(
        'day', 'date_histogram', field='timestamp', interval='1d',
    )
    agg.bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    agg.bucket(
        'payment', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_day_by_day_by_shop_and_by_payment')

    # Total sales and average sales by shop
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    ).metric(
        'avg_sales', 'avg', field='price',
    )
    write_output(search, 'total_and_avg_sales_by_shop')

    # Total sales, no aggregations
    search = get_search()
    search.aggs.metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales')

    # Total sales by shop and by payment type
    search = get_search()
    search.aggs.bucket(
        'shop', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    search.aggs.bucket(
        'payment', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_by_shop_and_by_payment')
