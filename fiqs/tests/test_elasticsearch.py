# -*- coding: utf-8 -*-

from datetime import datetime

import pytest
from elasticsearch_dsl import A

from fiqs.aggregations import (
    Avg,
    Count,
    DateHistogram,
    DateRange,
    Histogram,
    ReverseNested,
    Sum,
)
from fiqs.fields import FieldWithRanges, GroupedField
from fiqs.query import FQuery
from fiqs.testing.models import Sale, TrafficCount
from fiqs.testing.utils import get_search
from fiqs.tests.conftest import write_fquery_output, write_output

pytestmark = pytest.mark.docker


def test_count(elasticsearch_sale):
    assert get_search().count() == 500


def test_total_in_traffic_and_total_out_traffic(elasticsearch_traffic):
    # Total in traffic and out traffic
    write_fquery_output(
        FQuery(get_search()).values(
            Sum(TrafficCount.incoming_traffic),
            Sum(TrafficCount.outgoing_traffic),
        ),
        'total_in_traffic_and_total_out_traffic',
    )


def test_total_in_traffic_and_total_out_traffic_by_shop(elasticsearch_traffic):
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


def test_avg_product_price_by_product_type(elasticsearch_sale):
    # Average product price by product type
    write_fquery_output(
        FQuery(get_search()).values(
            avg_product_price=Avg(Sale.product_price),
        ).group_by(
            Sale.product_type,
        ),
        'avg_product_price_by_product_type',
    )


def test_avg_part_price_by_part(elasticsearch_sale):
    # Average part price by part
    write_fquery_output(
        FQuery(get_search()).values(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            Sale.part_id,
        ),
        'avg_part_price_by_part',
    )


def test_avg_part_price_by_product(elasticsearch_sale):
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


def test_avg_part_price_by_product_by_part(elasticsearch_sale):
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


def test_avg_product_price_by_shop_by_product_type(elasticsearch_sale):
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


def test_avg_part_price_by_shop_range_by_part_id(elasticsearch_sale):
    # Average part price by shop range by part id
    ranges = [{
        'from': 1,
        'to': 5,
        'key': '1 - 5',
    }, {
        'from': 5,
        'key': '5+',
    }]
    write_fquery_output(
        FQuery(get_search()).values(
            avg_part_price=Avg(Sale.part_price),
        ).group_by(
            FieldWithRanges(Sale.shop_id, ranges=ranges),
            Sale.part_id,
        ),
        'avg_part_price_by_shop_range_by_part_id',
    )


def test_avg_part_price_by_product_and_by_part(elasticsearch_sale):
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


def test_nb_sales_by_product_type(elasticsearch_sale):
    # Nb sales by product_type
    write_fquery_output(
        FQuery(get_search()).values(
            ReverseNested(
                Sale,
                Count(Sale),
            ),
        ).group_by(
            Sale.product_type,
        ),
        'nb_sales_by_product_type',
    )


def test_nb_sales_by_product_type_by_part_id(elasticsearch_sale):
    # Nb sales by product type by part_id
    write_fquery_output(
        FQuery(get_search()).values(
            ReverseNested(
                Sale,
                Count(Sale),
            ),
        ).group_by(
            Sale.product_type,
            Sale.part_id,
        ),
        'nb_sales_by_product_type_by_part_id',
    )


def test_total_and_avg_sales_by_product_type(elasticsearch_sale):
    # Average sale price by product type
    write_fquery_output(
        FQuery(get_search()).values(
            ReverseNested(
                Sale,
                avg_sales=Avg(Sale.price),
                total_sales=Sum(Sale.price),
            ),
        ).group_by(
            Sale.product_type,
        ),
        'total_and_avg_sales_by_product_type',
    )


def test_avg_product_price_and_avg_sales_by_product_type(elasticsearch_sale):
    # Average sale price and average product price by product type
    write_fquery_output(
        FQuery(get_search()).values(
            ReverseNested(
                Sale,
                avg_sales=Avg(Sale.price),
            ),
            avg_product_price=Avg(Sale.product_price),
        ).group_by(
            Sale.product_type,
        ),
        'avg_product_price_and_avg_sales_by_product_type',
    )


def test_no_aggregate_no_metric(elasticsearch_sale):
    # Nothing :o
    write_output(get_search(), 'no_aggregate_no_metric')


def test_total_sales_by_shop(elasticsearch_sale):
    # Total sales by shop
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_sales_by_shop',
    )


def test_total_sales_by_payment_type(elasticsearch_sale):
    # Total sales by payment type
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.payment_type,
        ),
        'total_sales_by_payment_type',
    )


def test_total_sales_by_payment_type_by_shop(elasticsearch_sale):
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


def test_total_sales_by_price_histogram(elasticsearch_sale):
    # Total sales by price histogram
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Histogram(
                Sale.price,
                interval=100,
            ),
        ),
        'total_sales_by_price_histogram',
    )


@pytest.mark.parametrize('interval,pretty_period', [
    ('1d', 'day'),
    ('1w', 'week'),
    ('1M', 'month'),
    ('1y', 'year'),
])
def test_total_sales_by_period(elasticsearch_sale, interval, pretty_period):
    # Total sales period by period
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            DateHistogram(
                Sale.timestamp,
                interval=interval,
            ),
        ),
        'total_sales_{}_by_{}'.format(pretty_period, pretty_period),
    )


def test_total_sales_by_day_offset(elasticsearch_sale):
    # Total sales by day, with offset
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            DateHistogram(
                Sale.timestamp,
                interval='1d',
                offset='+8h',
            ),
        ),
        'total_sales_by_day_offset_8hours',
    )


def test_total_sales_every_four_days(elasticsearch_sale):
    # Total sales every four days
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            DateHistogram(
                Sale.timestamp,
                interval='4d',
            ),
        ),
        'total_sales_every_four_days',
    )


def test_nb_sales_by_shop(elasticsearch_sale):
    # Number of sales by shop
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale.id),
        ).group_by(
            Sale.shop_id,
        ),
        'nb_sales_by_shop',
    )


def test_total_sales_day_by_day_by_shop_and_by_payment(elasticsearch_sale):
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


def test_total_and_avg_sales_by_shop(elasticsearch_sale):
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


def test_total_sales(elasticsearch_sale):
    # Total sales, no aggregations
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ),
        'total_sales',
    )


def test_total_sales_and_avg_sales(elasticsearch_sale):
    # Total sales and avg sales, no aggregations
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
            avg_sales=Avg(Sale.price),
        ),
        'total_sales_and_avg_sales',
    )


def test_total_sales_by_shop_and_by_payment(elasticsearch_sale):
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


def test_total_sales_by_payment_type_by_shop_range(elasticsearch_sale):
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


def test_total_sales_by_shop_range_by_payment_type(elasticsearch_sale):
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


def test_total_sales_by_shop_range(elasticsearch_sale):
    # Total sales by shop range
    ranges = [{
        'from': 1,
        'to': 5,
        'key': '1 - 5',
    }, {
        'from': 5,
        'key': '5+',
    }]
    write_fquery_output(
        FQuery(get_search()).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            FieldWithRanges(Sale.shop_id, ranges=ranges),
        ),
        'total_sales_by_shop_range',
    )


def test_nb_sales_by_shop_limited_size(elasticsearch_sale):
    # Nb sales by shop limited size
    write_fquery_output(
        FQuery(get_search(), default_size=2).values(
            Count(Sale),
        ).group_by(
            Sale.shop_id,
        ),
        'nb_sales_by_shop_limited_size',
    )


def test_nb_sales_by_shop_by_payment_type_limited_size(elasticsearch_sale):
    # Nb sales by shop by payment type limited size
    write_fquery_output(
        FQuery(get_search(), default_size=2).values(
            Count(Sale),
        ).group_by(
            Sale.shop_id,
            Sale.payment_type,
        ),
        'nb_sales_by_shop_by_payment_type_limited_size',
    )


def test_total_sales_by_shop_limited_size(elasticsearch_sale):
    # Total sales by shop limited size
    write_fquery_output(
        FQuery(get_search(), default_size=2).values(
            total_sales=Sum(Sale.price),
        ).group_by(
            Sale.shop_id,
        ),
        'total_sales_by_shop_limited_size',
    )


@pytest.fixture
def date_ranges_with_keys():
    return [
        {
            'from': datetime(2016, 1, 1),
            'to': datetime(2016, 1, 15),
            'key': 'first_half',
        },
        {
            'from': datetime(2016, 1, 15),
            'to': datetime(2016, 1, 31),
            'key': 'second_half',
        },
    ]


def test_nb_sales_by_date_range_with_keys(elasticsearch_sale,
                                          date_ranges_with_keys):
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            DateRange(
                Sale.timestamp,
                ranges=date_ranges_with_keys,
            ),
        ),
        'nb_sales_by_date_range_with_keys',
    )


def test_nb_sales_by_date_range_without_keys(elasticsearch_sale):
    # Nb sales by date range without keys
    ranges_without_keys = [
        {
            'from': datetime(2016, 1, 1),
            'to': datetime(2016, 1, 15),
        },
        {
            'from': datetime(2016, 1, 15),
            'to': datetime(2016, 1, 31),
        },
    ]
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            DateRange(
                Sale.timestamp,
                ranges=ranges_without_keys,
            ),
        ),
        'nb_sales_by_date_range_without_keys',
    )


def test_nb_sales_by_date_range_by_payment_type(elasticsearch_sale,
                                                date_ranges_with_keys):
    # Nb sales by date range by payment type
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            DateRange(
                Sale.timestamp,
                ranges=date_ranges_with_keys,
            ),
            Sale.payment_type,
        ),
        'nb_sales_by_date_range_by_payment_type',
    )


def test_nb_sales_by_payment_type_by_date_range(elasticsearch_sale,
                                                date_ranges_with_keys):
    # Nb sales by payment type by date range
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            Sale.payment_type,
            DateRange(
                Sale.timestamp,
                ranges=date_ranges_with_keys,
            ),
        ),
        'nb_sales_by_payment_type_by_date_range',
    )


@pytest.fixture
def shops_by_group():
    return {
        'group_a': list(range(1, 6)),
        'group_b': list(range(6, 11)),
    }


def test_nb_sales_by_grouped_shop(elasticsearch_sale, shops_by_group):
    # Nb sales by grouped shop id
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            GroupedField(
                Sale.shop_id,
                groups=shops_by_group,
            ),
        ),
        'nb_sales_by_grouped_shop',
    )


def test_nb_sales_by_grouped_shop_by_payment_type(elasticsearch_sale,
                                                  shops_by_group):
    # Nb sales by grouped shop id by payment type
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            GroupedField(
                Sale.shop_id,
                groups=shops_by_group,
            ),
            Sale.payment_type,
        ),
        'nb_sales_by_grouped_shop_by_payment_type',
    )


def test_nb_sales_by_payment_type_by_grouped_shop(elasticsearch_sale,
                                                  shops_by_group):
    # Nb sales by payment type by grouped shop id
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            Sale.payment_type,
            GroupedField(
                Sale.shop_id,
                groups=shops_by_group,
            ),
        ),
        'nb_sales_by_payment_type_by_grouped_shop',
    )


def test_avg_sales_by_grouped_shop(elasticsearch_sale, shops_by_group):
    # Avg price by grouped shop id
    write_fquery_output(
        FQuery(get_search()).values(
            avg_sales=Avg(Sale.price),
        ).group_by(
            GroupedField(
                Sale.shop_id,
                groups=shops_by_group,
            ),
        ),
        'avg_sales_by_grouped_shop',
    )


# All these filter tests still use elasticsearch_dsl (for the time being?)
def test_avg_price_filter_shop_id_1(elasticsearch_sale):
    # Avg price for shop_id 1
    a = A('filter', term={'shop_id': 1})
    a.bucket(
        'avg_price',
        'avg',
        field='price',
    )
    search = get_search()
    search.aggs.bucket(
        'shop_id_1',
        a,
    )
    write_output(search, 'avg_price_filter_shop_id_1')


def test_nb_sales_by_product_type_filter_product_type_1(elasticsearch_sale):
    # Number of sales, by product type, for product_type_1
    a = A('filter', term={'products.product_type': 'product_type_1'})
    a.bucket(
        'reverse_nested_root',
        'reverse_nested',
    )
    search = get_search()
    search.aggs.bucket(
        'products',
        'nested',
        path='products',
    ).bucket(
        'product_type_1',
        a,
    )
    write_output(search, 'nb_sales_by_product_type_filter_product_type_1')


def test_nb_sales_by_product_type_by_part_id_filter_product_type_1(
        elasticsearch_sale):
    # Number of sales, by product type, by part id, for product_type_1
    a = A('filter', term={'products.product_type': 'product_type_1'})
    a.bucket(
        'parts',
        'nested',
        path='products.parts',
    ).bucket(
        'part_id',
        'terms',
        field='products.parts.part_id',
    ).metric(
        'reverse_nested_root',
        'reverse_nested',
    )
    search = get_search()
    search.aggs.bucket(
        'products',
        'nested',
        path='products',
    ).bucket(
        'product_type_1',
        a,
    )
    write_output(
        search, 'nb_sales_by_product_type_by_part_id_filter_product_type_1')
