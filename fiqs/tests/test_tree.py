# -*- coding: utf-8 -*-

import six

from fiqs import flatten_result
from fiqs.tests.conftest import load_output
from fiqs.tree import ResultTree


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
        assert 'shop_id' in line
        assert type(line['shop_id']) == int


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
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
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
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type
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
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float

    # Ten lines for each payment type
    for payment_type in ['cash', 'wire_transfer', 'store_credit', ]:
        payment_type_lines = [l for l in lines if l['payment_type'] == payment_type]
        assert len(payment_type_lines) == 10

        # For each payment type, lines are ordered by doc_count
        assert payment_type_lines == sorted(
            payment_type_lines, key=(lambda l: l['doc_count']), reverse=True)


def test_total_sales_day_by_day():
    lines = flatten_result(load_output('total_sales_day_by_day'))

    assert len(lines) == 31  # Number of days in the data's month

    # Lines are sorted by date
    assert lines == sorted(lines, key=(lambda l: l['timestamp']))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # The aggregation and the metric are present
        assert 'timestamp' in line
        assert type(line['timestamp']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float


def test_total_sales_day_by_day_by_shop_and_by_payment():
    lines = flatten_result(load_output('total_sales_day_by_day_by_shop_and_by_payment'))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Day by day aggregation is always present
        assert 'timestamp' in line
        assert type(line['timestamp']) == int
        # Metric is always present
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        # Either shop aggregation or payment type aggregation is present
        assert ('shop_id' in line and 'payment_type' not in line)\
            or ('payment_type' in line and 'shop_id' not in line)
        if 'shop_id' in line:
            assert type(line['shop_id']) == int
        elif 'payment_type' in line:
            assert type(line['payment_type']) == six.text_type

    # Documents are counted once in the payment aggregations, once in the shop aggregation
    sum([line['doc_count']]) == 2 * 500

    # There are more than 31 buckets within the shop buckets
    assert len([l for l in lines if 'shop_id' in l]) >= 31
    # There are more than 31 buckets within the payment buckets
    assert len([l for l in lines if 'payment_type' in l]) >= 31


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
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        assert 'avg_sales' in line
        assert type(line['avg_sales']) == float


def test_total_sales_by_shop_and_by_payment():
    lines = flatten_result(load_output('total_sales_by_shop_and_by_payment'))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Metric is always present
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        # Either shop aggregation or payment type aggregation is present
        assert ('shop_id' in line and 'payment_type' not in line)\
            or ('payment_type' in line and 'shop_id' not in line)
        if 'shop_id' in line:
            assert type(line['shop_id']) == int
        elif 'payment_type' in line:
            assert type(line['payment_type']) == six.text_type

    # There are 3 payment lines, sorted by doc_count
    payment_lines = [l for l in lines if 'payment_type' in l]
    assert len(payment_lines) == 3
    assert sorted(payment_lines, key=lambda l: l['doc_count'], reverse=True) == payment_lines

    # There are 10 shop lines, sorted by doc_count
    shop_lines = [l for l in lines if 'shop_id' in l]
    assert len(shop_lines) == 10
    assert sorted(shop_lines, key=lambda l: l['doc_count'], reverse=True) == shop_lines


def test_total_sales():
    lines = flatten_result(load_output('total_sales'))

    assert len(lines) == 1

    line = lines[0]
    # Only metric is present
    assert list(line.keys()) == ['total_sales']
    assert type(line['total_sales']) == float


def test_total_sales_and_avg_sales():
    lines = flatten_result(load_output('total_sales_and_avg_sales'))

    assert len(lines) == 1

    line = lines[0]
    # Both metrics are present
    assert sorted(list(line.keys())) == sorted(['total_sales', 'avg_sales'])
    for key in ['total_sales', 'avg_sales']:
        assert type(line[key]) == float


##########
# Nested #
##########

def test_is_nested_node():
    node = {
        'products': {
            'doc_count': 100,
            'product': {
                'buckets': [
                    {
                        "doc_count": 38,
                        "key": "product_JQ0JVW20PV",
                        "parts": {
                            "doc_count": 100,
                            "part": {
                                "buckets": [
                                    {
                                        "doc_count": 10,
                                        "key": "part_10",
                                        "avg_part_price": {
                                            "value": 10.11
                                        },
                                    },
                                ],
                                "doc_count_error_upper_bound": 29,
                                "sum_other_doc_count": 1226,
                            },
                        },
                    },
                ],
                "doc_count_error_upper_bound": 29,
                "sum_other_doc_count": 1226,
            },
        },
    }

    tree = ResultTree({})
    assert tree._is_nested_node(node)
    assert tree._is_nested_node(node['products'])
    assert not tree._is_nested_node(node['products']['doc_count'])
    assert not tree._is_nested_node(node['products']['product'])
    assert not tree._is_nested_node(node['products']['product']['buckets'])
    assert not tree._is_nested_node(node['products']['product']['doc_count_error_upper_bound'])
    assert not tree._is_nested_node(node['products']['product']['sum_other_doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]['doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]['key'])
    assert tree._is_nested_node(node['products']['product']['buckets'][0]['parts'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['doc_count_error_upper_bound'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['sum_other_doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'][0])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'][0]['doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'][0]['key'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'][0]['avg_part_price'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]\
                ['parts']['part']['buckets'][0]['avg_part_price']['value'])


def test_is_nested_node_2():
    node = {
        'products': {
            'doc_count': 1540,
            'parts': {
                'doc_count': 8497,
                'part': {
                    'buckets': [
                        {
                            'avg_part_price': {
                                'value': 19.4,
                            },
                            'doc_count': 907,
                            'key': 'part_5',
                        },
                    ],
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                },
            },
        },
    }

    tree = ResultTree({})
    assert tree._is_nested_node(node)
    assert tree._is_nested_node(node['products'])
    assert not tree._is_nested_node(node['products']['doc_count'])
    assert tree._is_nested_node(node['products']['parts'])
    assert not tree._is_nested_node(node['products']['parts']['doc_count'])
    assert not tree._is_nested_node(node['products']['parts']['part'])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'])
    assert not tree._is_nested_node(node['products']['parts']['part']['doc_count_error_upper_bound'])
    assert not tree._is_nested_node(node['products']['parts']['part']['sum_other_doc_count'])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'][0])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'][0]\
                ['doc_count'])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'][0]\
                ['key'])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'][0]\
                ['avg_part_price'])
    assert not tree._is_nested_node(node['products']['parts']['part']['buckets'][0]\
                ['avg_part_price']['value'])


def test_remove_nested_aggregations():
    node = {
        "products": {
            "doc_count": 1540,
            "product_type": {
                "buckets": [
                    {
                        "avg_product_price": {
                            "value": 179.53889943074003
                        },
                        "doc_count": 527,
                        "key": "product_type_3"
                    },
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            },
        },
    }

    expected = {
        "doc_count": 1540,
        "product_type": {
            "buckets": [
                {
                    "avg_product_price": {
                        "value": 179.53889943074003
                    },
                    "doc_count": 527,
                    "key": "product_type_3"
                },
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
        },
    }

    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_2():
    node = {
        'products': {
            'doc_count': 100,
            'product': {
                'buckets': [
                    {
                        "doc_count": 38,
                        "key": "product_JQ0JVW20PV",
                        "parts": {
                            "doc_count": 100,
                            "part": {
                                "buckets": [
                                    {
                                        "doc_count": 10,
                                        "key": "part_10",
                                        "avg_part_price": {
                                            "value": 10.11
                                        },
                                    },
                                ],
                                "doc_count_error_upper_bound": 29,
                                "sum_other_doc_count": 1226,
                            },
                        },
                    },
                ],
                "doc_count_error_upper_bound": 29,
                "sum_other_doc_count": 1226,
            },
        },
    }

    expected = {
        'doc_count': 100,
        'product': {
            'buckets': [
                {
                    "key": "product_JQ0JVW20PV",
                    "doc_count": 38,
                    "part": {
                        "buckets": [
                            {
                                "doc_count": 10,
                                "key": "part_10",
                                "avg_part_price": {
                                    "value": 10.11
                                },
                            },
                        ],
                        "doc_count_error_upper_bound": 29,
                        "sum_other_doc_count": 1226,
                    },
                },
            ],
            "doc_count_error_upper_bound": 29,
            "sum_other_doc_count": 1226,
        },
    }

    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_3():
    node = {
        'products': {
            'doc_count': 1540,
            'parts': {
                'doc_count': 8497,
                'part': {
                    'buckets': [
                        {
                            'avg_part_price': {
                                'value': 19.4,
                            },
                            'doc_count': 907,
                            'key': 'part_5',
                        },
                    ],
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                },
            },
        },
    }

    expected = {
        'doc_count': 1540,
        'part': {
            'buckets': [
                {
                    'avg_part_price': {
                        'value': 19.4,
                    },
                    'doc_count': 907,
                    'key': 'part_5',
                },
            ],
            'doc_count_error_upper_bound': 0,
            'sum_other_doc_count': 0,
        },
    }


    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_avg_product_price_by_product_type():
    lines = flatten_result(load_output('avg_product_price_by_product_type'))

    assert len(lines) == 5  # One for each product type

    # Lines are sorted by doc_count
    assert lines == list(sorted(lines, key=(lambda l: l['doc_count']), reverse=True))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        assert 'avg_product_price' in line
        assert type(line['avg_product_price']) == float


def test_avg_part_price_by_part():
    lines = flatten_result(load_output('avg_part_price_by_part'))

    assert len(lines) == 10  # One for each part

    # Lines are sorted by doc_count
    assert lines == list(sorted(lines, key=(lambda l: l['doc_count']), reverse=True))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'part_id' in line
        assert type(line['part_id']) == six.text_type
        assert 'avg_part_price' in line
        assert type(line['avg_part_price']) == float


def test_avg_part_price_by_product():
    lines = flatten_result(load_output('avg_part_price_by_product'))

    assert len(lines) == 10  # Product agg reached the default 10 limit

    # Lines are sorted by doc_count
    assert lines == list(sorted(lines, key=(lambda l: l['doc_count']), reverse=True))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation and metric are present
        assert 'product_id' in line
        assert type(line['product_id']) == six.text_type
        assert 'avg_part_price' in line
        assert type(line['avg_part_price']) == float


def test_avg_part_price_by_product_by_part():
    lines = flatten_result(load_output('avg_part_price_by_product_by_part'))

    assert len(lines) == 10 * 10  # Product agg reached the default 10 limit and 10 parts

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Both aggregations and metric are present
        assert 'product_id' in line
        assert type(line['product_id']) == six.text_type
        assert 'part_id' in line
        assert type(line['part_id']) == six.text_type
        assert 'avg_part_price' in line
        assert type(line['avg_part_price']) == float


def test_avg_product_price_by_shop_by_product_type():
    lines = flatten_result(load_output('avg_product_price_by_shop_by_product_type'))

    assert len(lines) == 5 * 10  # 5 product types and 10 shops

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Both aggregations and metric are present
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        assert 'avg_product_price' in line
        assert type(line['avg_product_price']) == float


def test_avg_part_price_by_product_and_by_part():
    lines = flatten_result(load_output('avg_part_price_by_product_and_by_part'))

    assert len(lines) == 10 + 10  # Product agg reached the default 10 limit and 10 parts

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Metric is present
        assert 'avg_part_price' in line
        assert type(line['avg_part_price']) == float
        # Either product aggregation or part aggregation is present
        assert ('product_id' in line and not 'part_id' in line)\
            or ('part_id' in line and 'product_id' not in line)
        if 'product_id' in line:
            assert type(line['product_id']) == six.text_type
        elif 'part_id' in line:
            assert type(line['part_id']) == six.text_type

    # 10 payment lines, sorted by doc_count
    product_lines = [l for l in lines if 'product_id' in l]
    assert len(product_lines) == 10
    assert sorted(product_lines, key=lambda l: l['doc_count'], reverse=True) == product_lines
    # 10 part lines, sorted by doc_count
    part_lines = [l for l in lines if 'part_id' in l]
    assert len(part_lines) == 10
    assert sorted(part_lines, key=lambda l: l['doc_count'], reverse=True) == part_lines
