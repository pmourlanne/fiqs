# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import pytest

from fiqs.aggregations import Avg, Sum, DateHistogram, Count, ReverseNested
from fiqs.fields import FieldWithRanges
from fiqs.query import FQuery
from fiqs.testing.models import Sale, TrafficCount
from fiqs.testing.utils import get_search
from fiqs.tests.conftest import write_output, write_fquery_output


@pytest.mark.docker
def test_count(elasticsearch_sale):
    assert get_search().count() == 500


@pytest.mark.xfail  # See https://github.com/elastic/elasticsearch/issues/23776
@pytest.mark.docker
def test_offset_date_histogram(elasticsearch_sale):
    start = datetime(2016, 2, 1, 6)
    end = start + timedelta(days=2, hours=2)

    search = get_search()
    search = search.filter('range', **{
        'timestamp': {
            'gte': start.isoformat(),
            'lte': end.isoformat(),
        },
    })

    fquery = FQuery(search).values(
        Count(Sale),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1d',
            min=start,
            max=end,
            offset='+6h',
        )
    )

    result = fquery.eval(flat=False)
    aggregations = result._d_['aggregations']
    buckets = aggregations['timestamp']['buckets']

    keys = [datetime.utcfromtimestamp(bucket['key'] / 1000) for bucket in buckets]
    expected_keys = [
        start,
        start + timedelta(days=1),
        start + timedelta(days=2),
    ]
    assert keys == expected_keys


@pytest.mark.docker
def test_write_traffic_search_outputs(elasticsearch_traffic):
    # Total in traffic and out traffic
    write_fquery_output(
        FQuery(get_search()).values(
            Sum(TrafficCount.incoming_traffic),
            Sum(TrafficCount.outgoing_traffic),
        ),
        'total_in_traffic_and_total_out_traffic',
    )

    # Total in traffic and out traffic by shop id
    write_fquery_output(
        FQuery(get_search()).values(
            Sum(TrafficCount.incoming_traffic),
            Sum(TrafficCount.outgoing_traffic),
        ).group_by(
            TrafficCount.shop_id,
        ),
        'total_in_traffic_and_total_out_traffic_by_shop',
    )


@pytest.mark.docker
def test_write_nested_search_output(elasticsearch_sale):
    # Average product price by product type
    write_fquery_output(
        FQuery(get_search()).values(
            avg_product_price=Avg(Sale.product_price),
        ).group_by(
            Sale.product_type,
        ),
        'avg_product_price_by_product_type',
    )

    # Average part price by part
    write_fquery_output(
        FQuery(get_search()).values(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.part_id,
        ),
        'avg_part_price_by_part',
    )

    # Average part price by product
    write_fquery_output(
        FQuery(get_search()).values(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.product_id,
            Sale.parts,
        ),
        'avg_part_price_by_product',
    )

    # Average part price by product by part
    write_fquery_output(
        FQuery(get_search()).values(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.product_id,
            Sale.part_id,
        ),
        'avg_part_price_by_product_by_part',
    )

    # Average product price by shop by product
    write_fquery_output(
        FQuery(get_search()).values(
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

    # Nb sales by product_type
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            Sale.product_type,
            ReverseNested(),
        ),
        'nb_sales_by_product_type',
    )

    # Nb sales by product type by part_id
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            Sale.product_type,
            Sale.part_id,
            ReverseNested(),
        ),
        'nb_sales_by_product_type_by_part_id',
    )


@pytest.mark.docker
def test_write_search_outputs(elasticsearch_sale):
    # Nothing :o
    write_output(get_search(), 'no_aggregate_no_metric')

    # Total sales by shop
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_sales_by_shop',
    )

    # Total sales by payment type
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
        ),
        'total_sales_by_payment_type',
    )

    # Total sales by shop by payment type
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
            Sale.shop_id,
        ),
        'total_sales_by_payment_type_by_shop',
    )

    # Total sales day by day
    write_fquery_output(
        FQuery(get_search()).values(
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
        FQuery(get_search()).values(
            Count(Sale.id),
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
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
            avg_sales=Avg(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_and_avg_sales_by_shop',
    )

    # Total sales, no aggregations
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ),
        'total_sales',
    )

    # Total sales and avg sales, no aggregations
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
            avg_sales=Avg(Sale.price),
        ),
        'total_sales_and_avg_sales',
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

    # Total sales by payment by shop range
    ranges = [[1, 5], [5, 11], [11, 15]]
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
            FieldWithRanges(Sale.shop_id, ranges=ranges),
        ),
        'total_sales_by_payment_type_by_shop_range',
    )

    # Total sales by shop range by payment_type
    ranges = [[1, 5], [5, 11], [11, 15]]
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            FieldWithRanges(Sale.shop_id, ranges=ranges),
            Sale.payment_type,
        ),
        'total_sales_by_shop_range_by_payment_type',
    )
