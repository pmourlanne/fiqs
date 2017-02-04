# -*- coding: utf-8 -*-

import pytest

from fiqs.aggregations import Avg, Sum, DateHistogram
from fiqs.query import FQuery
from fiqs.testing.models import Sale
from fiqs.testing.utils import get_search
from fiqs.tests.conftest import write_output, write_fquery_output


@pytest.mark.docker
def test_count(elasticsearch):
    assert get_search().count() == 500


@pytest.mark.docker
def test_write_nested_search_output(elasticsearch):
    # Average product price by product type
    write_fquery_output(
        FQuery(get_search()).metric(
            avg_product_price=Avg(Sale.product_price),
        ).group_by(
            Sale.product_type,
        ),
        'avg_product_price_by_product_type',
    )

    # Average part price by part
    write_fquery_output(
        FQuery(get_search()).metric(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.part_id,
        ),
        'avg_part_price_by_part',
    )

    # Average part price by product
    write_fquery_output(
        FQuery(get_search()).metric(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.product_id,
            Sale.parts,
        ),
        'avg_part_price_by_product',
    )

    # Average part price by product by part
    write_fquery_output(
        FQuery(get_search()).metric(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.product_id,
            Sale.part_id,
        ),
        'avg_part_price_by_product_by_part',
    )

    # Average product price by shop by product
    write_fquery_output(
        FQuery(get_search()).metric(
            avg_product_price=Avg(Sale.product_price),
        ).group_by(
            Sale.shop_id,
            Sale.product_type,
        ),
        'avg_product_price_by_shop_by_product_type',
    )

    # Average part price by product and by part
    # This type of query is not possible with FQuery
    search = get_search()
    products_bucket = search.aggs.bucket(
        'products', 'nested', path='products',
    )
    products_bucket.bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    products_bucket.bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )
    write_output(search, 'avg_part_price_by_product_and_by_part')


@pytest.mark.docker
def test_write_search_outputs(elasticsearch):
    # Nothing :o
    write_output(get_search(), 'no_aggregate_no_metric')

    # Total sales by shop
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_sales_by_shop',
    )

    # Total sales by payment type
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
        ),
        'total_sales_by_payment_type',
    )

    # Total sales by shop by payment type
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
            Sale.shop_id,
        ),
        'total_sales_by_payment_type_by_shop',
    )

    # Total sales day by day
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
        ).group_by(
            DateHistogram(
                Sale.timestamp,
                interval='1d',
            ),
        ),
        'total_sales_day_by_day',
    )

    # Number of sales by shop
    write_fquery_output(
        FQuery(get_search()).metric(
            # Count(Sale.id),
        ).group_by(
            Sale.shop_id,
        ),
        'nb_sales_by_shop',
    )

    # Total sales day by day, by shop and by payment type
    # This type of query is not possible with FQuery
    search = get_search()
    agg = search.aggs.bucket(
        'timestamp', 'date_histogram', field='timestamp', interval='1d',
    )
    agg.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    agg.bucket(
        'payment_type', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_day_by_day_by_shop_and_by_payment')

    # Total sales and average sales by shop
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
            avg_sales=Avg(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_and_avg_sales_by_shop',
    )

    # Total sales, no aggregations
    write_fquery_output(
        FQuery(get_search()).metric(
            total_sales=Sum(Sale.price),
        ),
        'total_sales',
    )

    # Total sales by shop and by payment type
    # This type of query is not possible with FQuery
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    search.aggs.bucket(
        'payment_type', 'terms', field='payment_type',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    write_output(search, 'total_sales_by_shop_and_by_payment')
