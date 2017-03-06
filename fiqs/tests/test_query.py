# -*- coding: utf-8 -*-

from collections import Counter
from datetime import datetime

import pytest

from fiqs import fields
from fiqs.aggregations import Sum, Count, Avg, DateHistogram, Addition, Ratio, Subtraction
from fiqs.exceptions import ConfigurationError
from fiqs.fields import DataExtendedField, FieldWithChoices
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

###################
# Flatten results #
###################

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
        shop_id = fields.IntegerField(choices=range(1, 11))
        price = fields.IntegerField()

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

    lines == fquery._add_missing_lines(result, lines)
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
    assert lines == fquery._add_missing_lines(result, lines)
    assert len(lines) == 31


def test_fill_missing_buckets_range():
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
    lines == fquery._add_missing_lines(result, lines)
    assert len(lines) == 62
