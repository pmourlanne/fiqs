# -*- coding: utf-8 -*-

from collections import Counter
from datetime import datetime

import pytest
import six

from fiqs.aggregations import (
    Sum,
    Count,
    Avg,
    DateHistogram,
    Addition,
    Ratio,
    Subtraction,
    ReverseNested,
    Cardinality,
    DateRange,
)
from fiqs.exceptions import ConfigurationError
from fiqs.fields import (
    IntegerField,
    DataExtendedField,
    FieldWithChoices,
    FieldWithRanges,
    GroupedField,
)
from fiqs.models import Model
from fiqs.query import FQuery

from fiqs.testing.models import Sale, TrafficCount
from fiqs.testing.utils import get_search
from fiqs.tests.conftest import load_output


def test_one_metric():
    expected = get_search().aggs.metric(
        'total_sales', 'sum', field='price',
    ).to_dict()

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    )
    search = fquery._configure_search()

    assert expected == search.to_dict()


def test_one_aggregation():
    expected = get_search().aggs.metric(
        'shop_id', 'terms', field='shop_id',
    ).to_dict()

    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.shop_id,
    )
    search = fquery._configure_search()

    assert expected == search.to_dict()


def test_one_aggregation_one_metric():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_one_aggregation_count():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    )

    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.shop_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_two_aggregations_one_metric():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).bucket(
        'client_id', 'terms', field='client_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_one_aggregation_two_metrics():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    ).metric(
        'avg_sales', 'avg', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
        avg_sales=Avg(Sale.price),
    ).group_by(
        Sale.shop_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_two_aggregations_two_metrics():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).bucket(
        'client_id', 'terms', field='client_id',
    ).metric(
        'total_sales', 'sum', field='price',
    ).metric(
        'avg_sales', 'avg', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
        avg_sales=Avg(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_date_histogram():
    start = datetime(2016, 1, 1)
    end = datetime(2016, 1, 31)

    date_histogram_params = {
        'field': 'timestamp',
        'interval': '1d',
        'min_doc_count': 0,
        'extended_bounds': {
            'min': start,
            'max': end,
        },
    }

    search = get_search()
    search.aggs.bucket(
        'timestamp', 'date_histogram', **date_histogram_params
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1d',
            min=start,
            max=end,
        ),
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_date_histogram_month():
    start = datetime(2016, 1, 1)
    end = datetime(2016, 6, 1)

    date_histogram_params = {
        'field': 'timestamp',
        'interval': '1M',
        'min_doc_count': 0,
        'extended_bounds': {
            'min': start,
            'max': end,
        },
    }

    search = get_search()
    search.aggs.bucket(
        'timestamp', 'date_histogram', **date_histogram_params
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1M',
            min=start,
            max=end,
        ),
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_one_nested_aggregation_one_metric():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_type', 'terms', field='products.product_type',
    ).metric(
        'avg_product_price', 'avg', field='products.product_price',
    )

    fquery = FQuery(get_search()).values(
        avg_product_price=Avg(Sale.product_price),
    ).group_by(
        Sale.products,
        Sale.product_type,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_two_nested_aggregations_one_metric():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )

    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.products,
        Sale.product_id,
        Sale.parts,
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_nested_parent_automatically_added():
    """Need to add one parent for each group_by key"""
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )

    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_nested_parent_automatically_added_2():
    """Need to add multiple parents for one group_by key"""
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )

    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_nested_parent_automatically_added_3():
    """Should this work differently? Can it?"""
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )

    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.product_id,
        Sale.parts,  # Can we get rid of this?
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_nested_parent_automatically_added_4():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).metric(
        'avg_part_price', 'avg', field='products.parts.part_price',
    )

    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        FieldWithChoices(Sale.product_id, choices=[]),
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'reverse_nested_root', 'reverse_nested',
    ).metric(
        'avg_price', 'avg', field='price',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            avg_price=Avg(Sale.price),
        ),
    ).group_by(
        Sale.product_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation_2():
    search = get_search()
    product_id_bucket = search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    )
    product_id_bucket.metric(
        'avg_product_price', 'avg', field='products.product_price',
    )
    product_id_bucket.bucket(
        'reverse_nested_root', 'reverse_nested',
    ).metric(
        'avg_price', 'avg', field='price',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            avg_price=Avg(Sale.price),
        ),
        avg_product_price=Avg(Sale.product_price),
    ).group_by(
        Sale.product_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation_doc_count():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'reverse_nested_root', 'reverse_nested',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            Count(Sale),
        ),
    ).group_by(
        Sale.product_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation_doc_count_2():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).bucket(
        'reverse_nested_root', 'reverse_nested',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            Count(Sale),
        ),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation_doc_count_path():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).bucket(
        'reverse_nested_products', 'reverse_nested', path='products',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            'products',
            Count(Sale.products),
        ),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_reverse_nested_aggregation_doc_count_path_2():
    search = get_search()
    search.aggs.bucket(
        'products', 'nested', path='products',
    ).bucket(
        'product_id', 'terms', field='products.product_id',
    ).bucket(
        'parts', 'nested', path='products.parts',
    ).bucket(
        'part_id', 'terms', field='products.parts.part_id',
    ).bucket(
        'reverse_nested_products', 'reverse_nested', path='products',
    )

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale.products,
            Count(Sale.products),
        ),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_default_size():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id', size=5,
    ).bucket(
        'client_id', 'terms', field='client_id', size=5,
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search(), default_size=5).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_default_size_with_choices():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id', size=5,
    ).bucket(
        'client_id', 'terms', field='client_id', size=5,
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search(), default_size=5).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        FieldWithChoices(Sale.shop_id, choices=range(1, 5)),
        Sale.client_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_aggregation_size():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id', size=5,
    ).bucket(
        'client_id', 'terms', field='client_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DataExtendedField(Sale.shop_id, size=5),
        Sale.client_id,
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_ranges():
    ranges = [
        {
            'to': 4,
            'key': 'first_three',
        },
        {
            'from': 4,
            'to': 7,
            'key': 'next_three',
        },
        {
            'from': 7,
            'key': 'last_four',
        },
    ]

    search = get_search()
    search.aggs.bucket(
        'shop_id', 'range', field='shop_id', ranges=ranges, keyed=True,
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DataExtendedField(Sale.shop_id, ranges=ranges),
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_ranges_2():
    # Ranges can also be given as a list of tuples
    ranges = [
        {
            'from': 1,
            'to': 4,
            'key': '1 - 4',
        },
        {
            'from': 4,
            'to': 7,
            'key': '4 - 7',
        },
        {
            'from': 7,
            'to': 11,
            'key': '7 - 11',
        },
    ]

    search = get_search()
    search.aggs.bucket(
        'shop_id', 'range', field='shop_id', ranges=ranges, keyed=True,
    ).metric(
        'total_sales', 'sum', field='price',
    )

    ranges_as_list = [
        (1, 4),
        (4, 7),
        (7, 11),
    ]

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DataExtendedField(Sale.shop_id, ranges=ranges_as_list),
    )
    fsearch = fquery._configure_search()

    assert search.to_dict() == fsearch.to_dict()


def test_order_by():
    expected = {
        'aggs': {
            'shop_id': {
                'aggs': {
                    'total_sales': {'sum': {'field': 'price'}},
                },
                'terms': {
                    'field': 'shop_id',
                    'order': {'total_sales': 'desc'},
                }
            }
        },
        'query': {'match_all': {}},
    }

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    ).order_by(
        {'total_sales': 'desc'}
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == expected


def test_order_by_multiple_group_by():
    expected = {
        'aggs': {
            'shop_id': {
                'aggs': {
                    'client_id': {
                        'aggs': {
                            'total_sales': {'sum': {'field': 'price'}},
                        },
                        'terms': {
                            'field': 'client_id',
                            'order': {'total_sales': 'desc'},
                        },
                    },
                },
                'terms': {'field': 'shop_id'},
            },
        },
        'query': {'match_all': {}}
    }

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    ).order_by(
        {'total_sales': 'desc'}
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == expected


def test_order_by_field_with_choices():
    expected = {
        'aggs': {
            'payment_type': {
                'aggs': {
                    'total_sales': {'sum': {'field': 'price'}},
                },
                'terms': {
                    'field': 'payment_type',
                    'order': {'total_sales': 'desc'},
                }
            }
        },
        'query': {'match_all': {}},
    }

    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.payment_type,
    ).order_by(
        {'total_sales': 'desc'}
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == expected


def test_order_by_count():
    pass


def test_computed_automatically_added():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        str(Sum(TrafficCount.incoming_traffic)), 'sum', field='incoming_traffic',
    ).metric(
        str(Sum(TrafficCount.outgoing_traffic)), 'sum', field='outgoing_traffic',
    )

    fquery = FQuery(get_search()).values(
        Addition(
            Sum(TrafficCount.incoming_traffic),
            Sum(TrafficCount.outgoing_traffic),
        ),
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    ).group_by(
        TrafficCount.shop_id,
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()

    fquery = FQuery(get_search()).values(
        Addition(
            Sum(TrafficCount.incoming_traffic),
            Sum(TrafficCount.outgoing_traffic),
        ),
    ).group_by(
        TrafficCount.shop_id,
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()

    fquery = FQuery(get_search()).values(
        Ratio(
            Sum(TrafficCount.incoming_traffic),
            Addition(
                Sum(TrafficCount.incoming_traffic),
                Sum(TrafficCount.outgoing_traffic),
            ),
        ),
    ).group_by(
        TrafficCount.shop_id,
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()


def test_cardinality():
    search = get_search()
    search.aggs.metric(
        'nb_shops', 'cardinality', field='shop_id',
    )

    fquery = FQuery(get_search()).values(
        nb_shops=Cardinality(Sale.shop_id),
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()


def test_filters_aggregation():
    shops_by_country = {
        'country_a': range(1, 6),
        'country_b': range(6, 11),
    }

    filters = {}
    for country, shop_ids in shops_by_country.items():
        filters[country] = {
            'terms': {'shop_id': shop_ids},
        }
    search = get_search()
    search.aggs.metric(
        'shop_id', 'filters', filters=filters,
    )

    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        GroupedField(
            Sale.shop_id,
            groups=shops_by_country,
        ),
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()


def test_filters_aggregation_multiple_aggregations():
    shops_by_country = {
        'country_a': range(1, 6),
        'country_b': range(6, 11),
    }
    filters = {
        country: {
            'terms': {'shop_id': shop_ids},
        }
        for country, shop_ids in shops_by_country.items()
    }

    search = get_search()
    search.aggs.bucket(
        'payment_type', 'terms', field='payment_type',
    ).bucket(
        'shop_id', 'filters', filters=filters,
    )

    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.payment_type,
        GroupedField(
            Sale.shop_id,
            groups=shops_by_country,
        ),
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()

    # We switch the order, for fun.
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'filters', filters=filters,
    ).bucket(
        'payment_type', 'terms', field='payment_type',
    )

    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        GroupedField(
            Sale.shop_id,
            groups=shops_by_country,
        ),
        Sale.payment_type,
    )
    fsearch = fquery._configure_search()

    assert fsearch.to_dict() == search.to_dict()

###################
# Flatten results #
###################

def test_flatten_result_count():
    search = get_search()
    fquery = FQuery(search).values(
        Count(Sale),
    ).group_by(
        Sale.shop_id,
    )

    result = load_output('nb_sales_by_shop')
    lines = fquery._flatten_result(result)

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation is present
        assert 'shop_id' in line
        assert type(line['shop_id']) == int


def test_flatten_result_cast_sum_to_int():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    )

    result = load_output('total_sales_by_shop')
    lines = fquery._flatten_result(result)

    assert len(lines) == 10  # One for each shop

    # Lines are sorted by doc_count
    assert lines == sorted(lines, key=(lambda l: l['doc_count']), reverse=True)

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
        # Total sales aggregation results were casted to int
        assert 'total_sales' in line
        assert type(line['total_sales']) == int


def test_flatten_result_cast_timestamp():
    fquery = FQuery(get_search()).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1d',
        ),
    )

    result = load_output('total_sales_day_by_day')
    lines = fquery._flatten_result(result)

    assert len(lines) == 31

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'timestamp' in line
        assert type(line['timestamp']) == datetime
        # Total sales aggregation results were casted to int
        assert 'total_sales' in line
        assert type(line['total_sales']) == int


def test_computed_field():
    computed_field = Addition(
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    )

    fquery = FQuery(get_search()).values(
        computed_field,
    ).group_by(
        TrafficCount.shop_id,
    )

    result = load_output('total_in_traffic_and_total_out_traffic')
    lines = fquery._flatten_result(result)

    assert len(lines) == 1

    line = lines[0]
    # Base expressions are there
    assert str(Sum(TrafficCount.incoming_traffic)) in line
    assert str(Sum(TrafficCount.outgoing_traffic)) in line
    # Computed field is present
    assert str(computed_field) in line


def test_multiple_computed_fields():
    addition = Addition(
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    )
    computed_field = Ratio(
        Sum(TrafficCount.incoming_traffic),
        addition,
    )

    fquery = FQuery(get_search()).values(
        computed_field,
    ).group_by(
        TrafficCount.shop_id,
    )

    result = load_output('total_in_traffic_and_total_out_traffic')
    lines = fquery._flatten_result(result)

    assert len(lines) == 1

    line = lines[0]
    # Base expressions are there
    assert str(Sum(TrafficCount.incoming_traffic)) in line
    assert str(Sum(TrafficCount.outgoing_traffic)) in line
    # Addition is there
    assert str(addition) in line
    # Computed field is present
    assert str(computed_field) in line


def test_multiple_computed_fields_2():
    addition = Addition(
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    )
    subtraction = Subtraction(
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    )
    computed_field = Ratio(
        addition,
        subtraction,
    )

    fquery = FQuery(get_search()).values(
        computed_field,
    ).group_by(
        TrafficCount.shop_id,
    )

    result = load_output('total_in_traffic_and_total_out_traffic')
    lines = fquery._flatten_result(result)

    assert len(lines) == 1

    line = lines[0]
    # Base expressions are there
    assert str(Sum(TrafficCount.incoming_traffic)) in line
    assert str(Sum(TrafficCount.outgoing_traffic)) in line
    # Addition and subtraction are there
    assert str(addition) in line
    assert str(subtraction)
    # Computed field is present
    assert str(computed_field) in line


def test_computed_fields_raise_when_non_flat():
    addition = Addition(
        Sum(TrafficCount.incoming_traffic),
        Sum(TrafficCount.outgoing_traffic),
    )

    fquery = FQuery(get_search()).values(
        addition,
    ).group_by(
        TrafficCount.shop_id,
    )
    with pytest.raises(ConfigurationError):
        fquery.eval(flat=False)


def test_reverse_nested_doc_count():
    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            Count(Sale),
        ),
    ).group_by(
        Sale.product_type,
    )

    result = load_output('nb_sales_by_product_type')
    lines = fquery._flatten_result(result)

    assert len(lines) == 5
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        # Group by is present
        assert 'product_type' in line
        # Reverse nested is present
        assert str(ReverseNested(
            Sale,
            Count(Sale),
        )) in line


def test_reverse_nested():
    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            avg_sales=Avg(Sale.price),
            total_sales=Sum(Sale.price),
        ),
    ).group_by(
        Sale.product_type,
    )

    result = load_output('total_and_avg_sales_by_product_type')
    lines = fquery._flatten_result(result)

    assert len(lines) == 5
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        # Group by is present
        assert 'product_type' in line
        # Reverse nested metrics are present and properly casted
        assert str(ReverseNested(Sale, Count(Sale))) in line
        assert type(line[str(ReverseNested(Sale, Count(Sale)))]) == int
        assert str(ReverseNested(Sale, avg_sales=Avg(Sale.price))) in line
        assert type(line[str(ReverseNested(Sale, avg_sales=Avg(Sale.price)))]) == float
        assert str(ReverseNested(Sale, total_sales=Sum(Sale.price))) in line
        assert type(line[str(ReverseNested(Sale, total_sales=Sum(Sale.price)))]) == int


def test_reverse_nested_2():
    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            avg_sales=Avg(Sale.price),
        ),
        avg_product_price=Avg(Sale.product_price),
    ).group_by(
        Sale.product_type,
    )

    result = load_output('avg_product_price_and_avg_sales_by_product_type')
    lines = fquery._flatten_result(result)

    assert len(lines) == 5  # 5 product types
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        # Group by is present
        assert 'product_type' in line
        # Reverse nested doc count is present
        assert str(ReverseNested(Sale, Count(Sale))) in line
        assert type(line[str(ReverseNested(Sale, Count(Sale)))]) == int
        # Reverse nested metric is present
        assert str(ReverseNested(Sale, avg_sales=Avg(Sale.price))) in line
        assert type(line[str(ReverseNested(Sale, avg_sales=Avg(Sale.price)))]) == float
        # Standard metric is present
        assert 'avg_product_price' in line
        assert type(line['avg_product_price']) == float


def test_add_others_doc_count():
    fquery = FQuery(get_search(), default_size=2).values(
        Count(Sale),
    ).group_by(
        Sale.shop_id,
    )

    result = load_output('nb_sales_by_shop_limited_size')
    lines = fquery._flatten_result(result, add_others_line=True)

    assert len(lines) == 3  # size 2 plus others

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'shop_id' in line

    # 2 shop id lines, one others line
    assert len([l for l in lines if type(l['shop_id']) == int]) == 2
    assert len([l for l in lines if l['shop_id'] == 'others']) == 1


def test_date_range_with_keys():
    ranges = [
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
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        DateRange(
            Sale.timestamp,
            ranges=ranges,
        ),
    )

    result = load_output('nb_sales_by_date_range_with_keys')
    lines = fquery._flatten_result(result)

    assert len(lines) == 2
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'timestamp' in line
        assert type(line['timestamp']) == six.text_type

    assert [l['timestamp'] for l in lines] == ['first_half', 'second_half']


def test_date_range_without_keys():
    ranges = [
        {
            'from': datetime(2016, 1, 1),
            'to': datetime(2016, 1, 15),
        },
        {
            'from': datetime(2016, 1, 15),
            'to': datetime(2016, 1, 31),
        },
    ]
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        DateRange(
            Sale.timestamp,
            ranges=ranges,
        ),
    )

    result = load_output('nb_sales_by_date_range_with_keys')
    lines = fquery._flatten_result(result)

    assert len(lines) == 2
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'timestamp' in line
        assert type(line['timestamp']) == six.text_type


def test_flatten_result_grouped_field():
    shops_by_group = {
        'group_a': range(1, 6),
        'group_b': range(6, 11),
    }
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        GroupedField(
            Sale.shop_id,
            groups=shops_by_group,
        ),
    )

    result = load_output('nb_sales_by_grouped_shop')
    lines = fquery._flatten_result(result)

    assert len(lines) == 2
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'shop_id' in line
        # But was not casted to int
        assert type(line['shop_id']) == six.text_type


def test_flatten_result_grouped_field_multiple_aggregations():
    shops_by_group = {
        'group_a': range(1, 6),
        'group_b': range(6, 11),
    }
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.payment_type,
        GroupedField(
            Sale.shop_id,
            groups=shops_by_group,
        ),
    )

    result = load_output('nb_sales_by_payment_type_by_grouped_shop')
    lines = fquery._flatten_result(result)

    assert len(lines) == 3 * 2
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Shop aggregation is present
        assert 'shop_id' in line
        # Shop id was a string in the GroupedField
        assert type(line['shop_id']) == six.text_type
        # Payment type aggregation is present
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type


def test_flatten_result_grouped_field_with_metric():
    shops_by_group = {
        'group_a': range(1, 6),
        'group_b': range(6, 11),
    }
    fquery = FQuery(get_search()).values(
        avg_sales=Avg(Sale.price),
    ).group_by(
        GroupedField(
            Sale.shop_id,
            groups=shops_by_group,
        ),
    )

    result = load_output('avg_sales_by_grouped_shop')
    lines = fquery._flatten_result(result)

    assert len(lines) == 2
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Metric is present
        assert 'avg_sales' in line
        assert type(line['avg_sales']) == float
        # Shop aggregation is present
        assert 'shop_id' in line
        # Shop id was a string in the GroupedField
        assert type(line['shop_id']) == six.text_type

########################
# Fill missing buckets #
########################

def test_fill_missing_buckets_nothing_to_do():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    )

    result = load_output('total_sales_by_shop')
    lines = fquery._flatten_result(result)
    assert lines == fquery._add_missing_lines(result, lines)


def test_fill_missing_buckets_cannot_do_anything():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    )

    result = load_output('total_sales_by_shop')
    result['aggregations']['shop_id']['buckets'] = [
        bucket for bucket in result['aggregations']['shop_id']['buckets']
        if bucket['key'] != 1
    ]

    lines = fquery._flatten_result(result)
    assert lines == fquery._add_missing_lines(result, lines)

    assert len(lines) == 9


def test_fill_missing_buckets_custom_choices():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        FieldWithChoices(Sale.shop_id, choices=range(1, 11)),
    )

    result = load_output('total_sales_by_shop')
    result['aggregations']['shop_id']['buckets'] = [
        bucket for bucket in result['aggregations']['shop_id']['buckets']
        if bucket['key'] != 1
    ]

    lines = fquery._flatten_result(result)
    assert len(lines) == 9
    assert sorted([line['shop_id'] for line in lines]) == list(range(2, 11))

    lines == fquery._add_missing_lines(result, lines)
    assert len(lines) == 10
    assert sorted([line['shop_id'] for line in lines]) == list(range(1, 11))

    added_line = [line for line in lines if line['shop_id'] == 1][0]
    assert added_line == {
        'shop_id': 1,
        'total_sales': None,
        'doc_count': 0,
    }


def test_fill_missing_buckets_field_choices():
    class SaleWithChoices(Model):
        shop_id = IntegerField(choices=range(1, 11))
        price = IntegerField()

    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(SaleWithChoices.price),
    ).group_by(
        SaleWithChoices.shop_id,
    )

    result = load_output('total_sales_by_shop')
    result['aggregations']['shop_id']['buckets'] = [
        bucket for bucket in result['aggregations']['shop_id']['buckets']
        if bucket['key'] != 1
    ]

    lines = fquery._flatten_result(result)
    assert len(lines) == 9
    assert sorted([line['shop_id'] for line in lines]) == list(range(2, 11))

    lines == fquery._add_missing_lines(result, lines)
    assert len(lines) == 10
    assert sorted([line['shop_id'] for line in lines]) == list(range(1, 11))

    added_line = [line for line in lines if line['shop_id'] == 1][0]
    assert added_line == {
        'shop_id': 1,
        'total_sales': None,
        'doc_count': 0,
    }


def test_fill_missing_buckets_field_choices_pretty():
    pretty_choices = [(i, 'Shop {}'.format(i)) for i in range(1, 11)]

    class SaleWithPrettyChoices(Model):
        shop_id = IntegerField(choices=pretty_choices)
        price = IntegerField()

    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(SaleWithPrettyChoices.price),
    ).group_by(
        SaleWithPrettyChoices.shop_id,
    )

    result = load_output('total_sales_by_shop')
    result['aggregations']['shop_id']['buckets'] = [
        bucket for bucket in result['aggregations']['shop_id']['buckets']
        if bucket['key'] != 1
    ]

    lines = fquery._flatten_result(result)
    assert len(lines) == 9
    assert sorted([line['shop_id'] for line in lines]) == list(range(2, 11))

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 10
    assert sorted([line['shop_id'] for line in lines]) == list(range(1, 11))

    added_line = [line for line in lines if line['shop_id'] == 1][0]
    assert added_line == {
        'shop_id': 1,
        'total_sales': None,
        'doc_count': 0,
    }


def test_fill_missing_buckets_values_in_other_agg():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.payment_type,
        Sale.shop_id,
    )

    result = load_output('total_sales_by_payment_type_by_shop_id')
    wire_transfer_bucket = [
        b for b in result['aggregations']['payment_type']['buckets']
        if b['key'] == 'wire_transfer'
    ][0]
    wire_transfer_bucket['shop_id']['buckets'] = [
        b for b in wire_transfer_bucket['shop_id']['buckets']
        if b['key'] != 1
    ]

    lines = fquery._flatten_result(result)
    assert len(lines) == 29
    # All shop ids are present three times
    counter = Counter([line['shop_id'] for line in lines])
    for i in range(2, 11):
        assert counter[i] == 3
    # Except the one I removed
    assert counter[1] == 2

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 30
    # All shop ids are present three times
    counter = Counter([line['shop_id'] for line in lines])
    for i in range(1, 11):
        assert counter[i] == 3

    added_line = [
        line for line in lines
        if line['shop_id'] == 1 and line['payment_type'] == 'wire_transfer'
    ][0]
    assert added_line == {
        'shop_id': 1,
        'total_sales': None,
        'doc_count': 0,
        'payment_type': 'wire_transfer',
    }


def test_fill_missing_buckets_range_nothing_to_do():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1d',
            min=datetime(2016, 1, 1),
            max=datetime(2016, 1, 31),
        ),
    )
    fquery._configure_search()

    result = load_output('total_sales_day_by_day')

    lines = fquery._flatten_result(result)
    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 31


def test_fill_missing_buckets_date_histogram():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1d',
            min=datetime(2015, 12, 1),
            max=datetime(2016, 1, 31),
        ),
    )
    fquery._configure_search()

    result = load_output('total_sales_day_by_day')

    lines = fquery._flatten_result(result)
    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 62


def test_fill_missing_buckets_date_histogram_month_nothing_to_do():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1M',
            min=datetime(2016, 1, 1),
            max=datetime(2016, 1, 31),
        ),
    )
    fquery._configure_search()

    result = load_output('total_sales_month_by_month')

    lines = fquery._flatten_result(result)
    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 2


def test_fill_missing_buckets_date_histogram_month():
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='1M',
            min=datetime(2015, 12, 1),
            max=datetime(2016, 6, 1),
        ),
    )
    fquery._configure_search()

    result = load_output('total_sales_month_by_month')

    lines = fquery._flatten_result(result)
    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 7


def test_fill_missing_buckets_ranges():
    ranges = [[1, 5], [5, 11], [11, 15]]
    search = get_search()
    fquery = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.payment_type,
        FieldWithRanges(Sale.shop_id, ranges=ranges)
    )
    fquery._configure_search()

    result = load_output('total_sales_by_payment_type_by_shop_range')
    # We remove one bucket
    payment_type_buckets = [
        b for b in result['aggregations']['payment_type']['buckets']
        if b['key'] != 'wire_transfer'
    ]
    result['aggregations']['payment_type']['buckets'] = payment_type_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 6  # 3 shop ranges, 2 payment types
    assert set([l['payment_type'] for l in lines]) == {'cash', 'store_credit'}

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 9  # 3 shop ranges, 3 payment types
    added_lines = [l for l in lines if l['payment_type'] == 'wire_transfer']
    range_keys = ['1 - 5', '5 - 11', '11 - 15']
    assert sorted([l['shop_id'] for l in added_lines]) == sorted(range_keys)


def test_fill_missing_buckets_nested_nothing_to_do():
    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.part_id,
    )
    fquery._configure_search()

    result = load_output('avg_part_price_by_part')

    lines = fquery._flatten_result(result)
    assert len(lines) == 10  # 10 parts

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 10


def test_fill_missing_buckets_nested():
    fquery = FQuery(get_search()).values(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fquery._configure_search()

    result = load_output('avg_part_price_by_product_by_part')
    # We remove one part bucket in the first product bucket
    product_bucket = result['aggregations']['products']['product_id']['buckets'][0]
    part_id_buckets = [
        b for b in product_bucket['parts']['part_id']['buckets']
        if b['key'] != 'part_1'
    ]
    product_bucket['parts']['part_id']['buckets'] = part_id_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 99  # 10 parts, 10 products minus the one we removed

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 100


def test_fill_missing_buckets_reverse_nested_doc_count():
    product_types = [
        'product_type_{}'.format(i)
        for i in range(5)
    ]

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            Count(Sale),
        ),
    ).group_by(
        FieldWithChoices(Sale.product_type, choices=product_types),
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_product_type')
    product_type_buckets = result['aggregations']['products']['product_type']['buckets']
    product_type_buckets = [
        b for b in product_type_buckets
        if b['key'] != 'product_type_0'
    ]
    result['aggregations']['products']['product_type']['buckets'] = product_type_buckets
    lines = fquery._flatten_result(result)

    assert len(lines) == 4
    assert sorted([l['product_type'] for l in lines]) == product_types[1:]

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 5
    assert sorted([l['product_type'] for l in lines]) == product_types

    # Reverse nested aggregation is present in all lines
    for line in lines:
        assert str(ReverseNested(
            Sale,
            Count(Sale),
        )) in line


def test_fill_missing_buckets_reverse_nested():
    product_types = [
        'product_type_{}'.format(i)
        for i in range(5)
    ]

    fquery = FQuery(get_search()).values(
        ReverseNested(
            Sale,
            avg_sales=Avg(Sale.price),
            total_sales=Sum(Sale.price),
        ),
    ).group_by(
        FieldWithChoices(Sale.product_type, choices=product_types),
    )
    fquery._configure_search()

    result = load_output('total_and_avg_sales_by_product_type')
    product_type_buckets = result['aggregations']['products']['product_type']['buckets']
    product_type_buckets = [
        b for b in product_type_buckets
        if b['key'] != 'product_type_0'
    ]
    result['aggregations']['products']['product_type']['buckets'] = product_type_buckets
    lines = fquery._flatten_result(result)

    assert len(lines) == 4
    assert sorted([l['product_type'] for l in lines]) == product_types[1:]
    for line in lines:
        assert str(ReverseNested(Sale, avg_sales=Avg(Sale.price))) in line
        assert str(ReverseNested(Sale, total_sales=Sum(Sale.price))) in line
        assert str(ReverseNested(Sale, Count(Sale))) in line

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 5
    assert sorted([l['product_type'] for l in lines]) == product_types
    for line in lines:
        assert str(ReverseNested(Sale, avg_sales=Avg(Sale.price))) in line
        assert str(ReverseNested(Sale, total_sales=Sum(Sale.price))) in line
        assert str(ReverseNested(Sale, Count(Sale))) in line


def test_fill_missing_buckets_date_range_with_keys():
    ranges = [
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
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        DateRange(
            Sale.timestamp,
            ranges=ranges,
        ),
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_date_range_with_keys')
    buckets = result['aggregations']['timestamp']['buckets']
    buckets = buckets[1:]
    result['aggregations']['timestamp']['buckets'] = buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 1
    assert [l['timestamp'] for l in lines] == ['second_half']

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 2
    assert [l['timestamp'] for l in lines] == ['second_half', 'first_half']


def test_fill_missing_buckets_date_range_multiple_group_by():
    ranges = [
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
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        DateRange(
            Sale.timestamp,
            ranges=ranges,
        ),
        Sale.payment_type,
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_payment_type_by_date_range')
    payment_type_buckets = result['aggregations']['payment_type']['buckets']
    payment_type_buckets = [b for b in payment_type_buckets if b['key'] != 'wire_transfer']
    result['aggregations']['payment_type']['buckets'] = payment_type_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 4  # 2 date periods, 2 payment types

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 6  # 2 date periods, 2 + 1 payment types


def test_fill_missing_buckets_date_range_multiple_group_by_2():
    ranges = [
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
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.payment_type,
        DateRange(
            Sale.timestamp,
            ranges=ranges,
        ),
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_date_range_by_payment_type')
    timestamp_buckets = result['aggregations']['timestamp']['buckets']
    timestamp_buckets = [b for b in timestamp_buckets if b['key'] != 'second_half']
    result['aggregations']['timestamp']['buckets'] = timestamp_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 3  # 1 date period, 3 payment types

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 6  # 1 + 1 date periods, 3 payment types


def test_fill_missing_buckets_grouped_field():
    shops_by_group = {
        'group_a': range(1, 6),
        'group_b': range(6, 11),
    }
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        Sale.payment_type,
        GroupedField(
            Sale.shop_id,
            groups=shops_by_group,
        ),
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_payment_type_by_grouped_shop')
    payment_type_buckets = result['aggregations']['payment_type']['buckets']
    payment_type_buckets = [b for b in payment_type_buckets if b['key'] != 'wire_transfer']
    result['aggregations']['payment_type']['buckets'] = payment_type_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 4  # 2 payment types, 2 groups

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 6  # 3 payment types, 2 groups


def test_fill_missing_buckets_grouped_field_2():
    shops_by_group = {
        'group_a': range(1, 6),
        'group_b': range(6, 11),
    }
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        GroupedField(
            Sale.shop_id,
            groups=shops_by_group,
        ),
        Sale.payment_type,
    )
    fquery._configure_search()

    result = load_output('nb_sales_by_grouped_shop_by_payment_type')
    shop_group_buckets = result['aggregations']['shop_id']['buckets']
    shop_group_buckets = {
        key: bucket
        for key, bucket in shop_group_buckets.items()
        if key != 'group_a'
    }
    result['aggregations']['shop_id']['buckets'] = shop_group_buckets

    lines = fquery._flatten_result(result)
    assert len(lines) == 3  # 3 payment types, 1 group

    lines = fquery._add_missing_lines(result, lines)
    assert len(lines) == 6  # 3 payment types, 2 groups
