# -*- coding: utf-8 -*-

import six

from fiqs import flatten_result
from fiqs.tests.conftest import load_output
from fiqs.tree import ResultTree


def test_no_aggregate_no_metric():
    expected = []
    assert flatten_result(load_output('no_aggregate_no_metric')) == expected


def test_no_data():
    lines = flatten_result(load_output('no_data_nb_sales_by_day_of_week_by_shop'))
    assert lines == []


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


def test_total_sales_by_payment_type_by_shop_range():
    lines = flatten_result(load_output('total_sales_by_payment_type_by_shop_range'))

    assert len(lines) == 3 * 3  # 3 payment types, 3 shop ranges

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Metric is present
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        # Both group by are present
        assert 'shop_id' in line
        assert type(line['shop_id']) == six.text_type
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type

    # Three lines for each payment type
    for payment_type in ['cash', 'wire_transfer', 'store_credit', ]:
        payment_type_lines = [l for l in lines if l['payment_type'] == payment_type]
        assert len(payment_type_lines) == 3
        range_keys = ['1 - 5', '5 - 11', '11 - 15']
        assert sorted([l['shop_id'] for l in payment_type_lines]) == sorted(range_keys)


def test_total_sales_by_shop_range():
    lines = flatten_result(load_output('total_sales_by_shop_range'))

    assert len(lines) == 2  # 2 ranges

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Metric is present
        assert 'total_sales' in line
        assert type(line['total_sales']) == float
        # Group by is present
        assert 'shop_id' in line
        assert type(line['shop_id']) == six.text_type

    range_keys = ['1 - 5', '5+']
    assert sorted([l['shop_id'] for l in lines]) == range_keys


def test_nb_sales_by_payment_type_by_date_range():
    lines = flatten_result(load_output('nb_sales_by_payment_type_by_date_range'))

    assert len(lines) == 6  # 2 date periods, 3 payment types

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Date range is present
        assert 'timestamp' in line
        assert type(line['timestamp']) == six.text_type
        # Payment type is present
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type


def test_nb_sales_by_date_range_by_payment_type():
    lines = flatten_result(load_output('nb_sales_by_date_range_by_payment_type'))

    assert len(lines) == 6  # 2 date periods, 3 payment types

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Date range is present
        assert 'timestamp' in line
        assert type(line['timestamp']) == six.text_type
        # Payment type is present
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type


def test_nb_sales_by_shop_limited_size_add_others_line():
    lines = flatten_result(load_output('nb_sales_by_shop_limited_size'), add_others_line=True)

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


def test_nb_sales_by_shop_by_payment_type_limited_size_add_others_line():
    lines = flatten_result(
        load_output('nb_sales_by_shop_by_payment_type_limited_size'), add_others_line=True)

    assert len(lines) == 7  # size 2 * 2 plus 3 others

    full_lines = [l for l in lines if 'payment_type' in l and l['payment_type'] != 'others']
    assert len(full_lines) == 4
    for line in full_lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by are both present
        assert 'shop_id' in line
        assert type(line['shop_id']) == int
        assert 'payment_type' in line
        assert type(line['payment_type']) == six.text_type

    other_shops_line = [l for l in lines if 'payment_type' not in l]
    assert len(other_shops_line) == 1
    other_shops_line = other_shops_line[0]
    assert 'doc_count' in other_shops_line
    assert type(other_shops_line['doc_count']) == int
    assert 'shop_id' in line
    assert other_shops_line['shop_id'] == 'others'

    other_payments_lines = [l for l in lines if l.get('payment_type') == 'others']
    assert len(other_payments_lines) == 2
    for line in other_payments_lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Shop group by is present
        assert 'shop_id' in line
        assert type(line['shop_id']) == int

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
    assert tree._is_nested_node(node['products'])
    assert not tree._is_nested_node(node['products']['doc_count'])
    assert not tree._is_nested_node(node['products']['product'])
    assert not tree._is_nested_node(node['products']['product']['buckets'])
    assert not tree._is_nested_node(node['products']['product']['doc_count_error_upper_bound'])
    assert not tree._is_nested_node(node['products']['product']['sum_other_doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]['doc_count'])
    assert not tree._is_nested_node(node['products']['product']['buckets'][0]['key'])
    # Naive call
    assert tree._is_nested_node(node['products']['product']['buckets'][0]['parts'])
    # In situation call
    assert tree._is_nested_node(
        node['products']['product']['buckets'][0]['parts'],
        parent_is_root=False,
        same_level_keys=['parts', 'key', 'doc_count', ],
    )
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


def test_is_nested_node_standard_node():
    # This is not a nested node
    node = {
        "shop_id": {
            "buckets": [
                {
                    "doc_count": 59,
                    "key": 7,
                    "total_sales": {
                        "value": 28194.0,
                    },
                },
            ]
        }
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['shop_id'])
    assert not tree._is_nested_node(node['shop_id']['buckets'])
    assert not tree._is_nested_node(node['shop_id']['buckets'][0])
    assert not tree._is_nested_node(node['shop_id']['buckets'][0]['total_sales'])
    assert not tree._is_nested_node(node['shop_id']['buckets'][0]['total_sales']['value'])


def test_is_nested_node_ranges_node():
    # This is not a nested node, but it contains ranges
    node = {
        "payment_type": {
            "buckets": [
                {
                    "doc_count": 172,
                    "key": "wire_transfer",
                    "shop_id": {
                        "buckets": {
                            "1 - 5": {
                                "doc_count": 61,
                                "from": 1.0,
                                "to": 5.0,
                                "total_sales": {
                                    "value": 29523.0,
                                },
                            },
                        },
                    },
                },
            ],
        },
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['payment_type'])
    assert not tree._is_nested_node(node['payment_type']['buckets'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets']['1 - 5'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets']['1 - 5']['total_sales'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets']['1 - 5']['total_sales']['value'])


def test_is_nested_node_ranges_node_2():
    node = {
        "shop_id": {
            "buckets": {
                "1 - 5": {
                    "doc_count": 181,
                    "from": 1.0,
                    "payment_type": {
                        "buckets": [
                            {
                                "doc_count": 68,
                                "key": "store_credit",
                                "total_sales": {
                                    "value": 33595.0,
                                },
                            },
                        ],
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                    },
                    "to": 5.0,
                },
            },
        },
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['shop_id'])
    assert not tree._is_nested_node(node['shop_id']['buckets'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5']\
                ['payment_type'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5']\
                ['payment_type']['buckets'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5']\
                ['payment_type']['buckets'][0])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5']\
                ['payment_type']['buckets'][0]['total_sales'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['1 - 5']\
                ['payment_type']['buckets'][0]['total_sales']['value'])


def test_is_nested_node_ranges_node_3():
    node = {
        "shop_id": {
            "buckets": {
                "5+": {
                    "doc_count": 181,
                    "payment_type": {
                        "buckets": [
                            {
                                "doc_count": 68,
                                "key": "store_credit",
                                "total_sales": {
                                    "value": 33595.0,
                                },
                            },
                        ],
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                    },
                    "from": 5.0,
                },
            },
        },
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['shop_id'])
    assert not tree._is_nested_node(node['shop_id']['buckets'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+']\
                ['payment_type'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+']\
                ['payment_type']['buckets'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+']\
                ['payment_type']['buckets'][0])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+']\
                ['payment_type']['buckets'][0]['total_sales'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['5+']\
                ['payment_type']['buckets'][0]['total_sales']['value'])


def test_is_nested_node_filters_aggregation():
    node = {
        "shop_id": {
            "buckets": {
                "group_a": {
                    "doc_count": 238,
                    "payment_type": {
                        "buckets": [
                            {
                                "doc_count": 86,
                                "key": "store_credit",
                            },
                        ],
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                    },
                },
            },
        },
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['shop_id'])
    assert not tree._is_nested_node(node['shop_id']['buckets'])

    # Naive call
    assert tree._is_nested_node(node['shop_id']['buckets']['group_a'])
    # In-situation call
    assert not tree._is_nested_node(
        node['shop_id']['buckets']['group_a'],
        parent_is_root=False,
        same_level_keys=['group_a', ],
    )

    assert not tree._is_nested_node(node['shop_id']['buckets']['group_a']\
                ['payment_type'])
    assert not tree._is_nested_node(node['shop_id']['buckets']['group_a']\
                ['payment_type']['buckets'])


def test_is_nested_node_filters_aggregation_2():
    node = {
        "payment_type": {
            "buckets": [
                {
                    "doc_count": 172,
                    "key": "wire_transfer",
                    "shop_id": {
                        "buckets": {
                            "group_a": {
                                "doc_count": 84,
                            },
                            "group_b": {
                                "doc_count": 88,
                            },
                        },
                    },
                },
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
        },
    }

    tree = ResultTree({})
    assert not tree._is_nested_node(node['payment_type'])
    assert not tree._is_nested_node(node['payment_type']['buckets'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets'])
    assert not tree._is_nested_node(node['payment_type']['buckets'][0]\
                ['shop_id']['buckets']['group_a'])


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


def test_remove_nested_aggregations_standard_node():
    result = load_output('total_sales_by_shop')
    node = result['aggregations']
    expected = node

    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_standard_node_2():
    node = {
        "payment_type": {
            "buckets": [
                {
                    "doc_count": 172,
                    "key": "wire_transfer",
                    "shop_id": {
                        "buckets": {
                            "1 - 5": {
                                "doc_count": 61,
                                "from": 1.0,
                                "to": 5.0,
                                "total_sales": {
                                    "value": 29523.0,
                                },
                            },
                            "5+": {
                                "doc_count": 123,
                                "from": 5.0,
                                "total_sales": {
                                    "value": 123.456,
                                },
                            },
                        },
                    },
                },
            ],
        },
    }

    expected = node
    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_ranges_node():
    node = {
        "shop_id": {
            "buckets": {
                "1 - 5": {
                    "doc_count": 181,
                    "from": 1.0,
                    "payment_type": {
                        "buckets": [
                            {
                                "doc_count": 68,
                                "key": "store_credit",
                                "total_sales": {
                                    "value": 33595.0,
                                },
                            },
                        ],
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                    },
                    "to": 5.0,
                },
            },
        },
    }

    expected = node
    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_reverse_nested():
    # Reverse nested aggregations need to begin with 'reverse_nested'
    # Otherwise fiqs cannot distinguish nested nodes from reverse nested ones
    node = {
        "products": {
            "doc_count": 1540,
            "product_type": {
                "buckets": [
                    {
                        "doc_count": 527,
                        "key": "product_type_3",
                        "reverse_nested_root": {
                            "doc_count": 332,
                        },
                    },
                ],
            },
        },
    }

    expected = {
        "doc_count": 1540,
        "product_type": {
            "buckets": [
                {
                    "doc_count": 527,
                    "key": "product_type_3",
                    "reverse_nested_root": {
                        "doc_count": 332,
                    },
                },
            ],
        },
    }
    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_aggregations_reverse_nested_2():
    # Reverse nested aggregations need to begin with 'reverse_nested'
    # Otherwise fiqs cannot distinguish nested nodes from reverse nested ones
    node = {
        "products": {
            "doc_count": 1540,
            "product_type": {
                "buckets": [
                    {
                        "doc_count": 527,
                        "key": "product_type_3",
                        "reverse_nested_root": {
                            "avg_sales": {
                                "value": 495.18674698795184,
                            },
                            "doc_count": 332,
                            "total_sales": {
                                "value": 164402.0,
                            },
                        },
                    },
                ],
            },
        },
    }

    expected = {
        "doc_count": 1540,
        "product_type": {
            "buckets": [
                {
                    "doc_count": 527,
                    "key": "product_type_3",
                    "reverse_nested_root": {
                        "avg_sales": {
                            "value": 495.18674698795184,
                        },
                        "doc_count": 332,
                        "total_sales": {
                            "value": 164402.0,
                        },
                    },
                },
            ],
        },
    }
    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_filters_aggregations():
    node = {
        "shop_id": {
            "buckets": {
                "group_a": {
                    "doc_count": 238,
                    "payment_type": {
                        "buckets": [
                            {
                                "doc_count": 86,
                                "key": "store_credit",
                            },
                        ],
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                    },
                },
            },
        },
    }
    expected = node

    result = ResultTree({})._remove_nested_aggregations(node)

    assert expected == result


def test_remove_nested_filters_aggregations_2():
    node = {
        "payment_type": {
            "buckets": [
                {
                    "doc_count": 172,
                    "key": "wire_transfer",
                    "shop_id": {
                        "buckets": {
                            "group_a": {
                                "doc_count": 84,
                            },
                            "group_b": {
                                "doc_count": 88,
                            },
                        },
                    },
                },
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
        },
    }
    expected = node

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


def test_nb_sales_by_product_type():
    lines = flatten_result(load_output('nb_sales_by_product_type'))

    assert len(lines) == 5  # 5 product types
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        # Reverse nested doc count is present
        assert 'reverse_nested_root__doc_count' in line
        assert type(line['reverse_nested_root__doc_count']) == int


def test_nb_sales_by_product_type_by_part_id():
    lines = flatten_result(load_output('nb_sales_by_product_type_by_part_id'))

    assert len(lines) == 5 * 10 # 5 product types, 10 part ids
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by are present
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        assert 'part_id' in line
        assert type(line['part_id']) == six.text_type
        # Reverse nested doc count is present
        assert 'reverse_nested_root__doc_count' in line
        assert type(line['reverse_nested_root__doc_count']) == int


def test_total_and_avg_sales_by_product_type():
    lines = flatten_result(load_output('total_and_avg_sales_by_product_type'))

    assert len(lines) == 5  # 5 product types
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        # Reverse nested doc count is present
        assert 'reverse_nested_root__doc_count' in line
        assert type(line['reverse_nested_root__doc_count']) == int
        # Both reverse nested metrics are present
        assert 'reverse_nested_root__total_sales' in line
        assert type(line['reverse_nested_root__total_sales']) == float
        assert 'reverse_nested_root__avg_sales' in line
        assert type(line['reverse_nested_root__avg_sales']) == float


def test_avg_product_price_and_avg_sales_by_product_type():
    lines = flatten_result(load_output('avg_product_price_and_avg_sales_by_product_type'))

    assert len(lines) == 5  # 5 product types
    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Group by is present
        assert 'product_type' in line
        assert type(line['product_type']) == six.text_type
        # Reverse nested doc count is present
        assert 'reverse_nested_root__doc_count' in line
        assert type(line['reverse_nested_root__doc_count']) == int
        # Reverse nested metric is present
        assert 'reverse_nested_root__avg_sales' in line
        assert type(line['reverse_nested_root__avg_sales']) == float
        # Standard metric is present
        assert 'avg_product_price' in line
        assert type(line['avg_product_price']) == float


def test_force_not_remove_nested_aggregation():
    lines = flatten_result(load_output('nb_sales_by_shop'), remove_nested_aggregations=False)

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


def test_nb_sales_by_grouped_shop():
    lines = flatten_result(load_output('nb_sales_by_grouped_shop'))

    assert len(lines) == 2

    # Lines are not sorted by doc_count but by the order of the groups
    assert lines == sorted(lines, key=(lambda l: l['shop_id']))

    for line in lines:
        # Doc count is present
        assert 'doc_count' in line
        assert type(line['doc_count']) == int
        # Aggregation is present
        assert 'shop_id' in line
        # Shop id was a string in the GroupedField
        assert type(line['shop_id']) == six.text_type

    # Two groups of shop id
    assert sorted([line['shop_id'] for line in lines]) == sorted(['group_a', 'group_b'])


def test_nb_sales_by_grouped_shop_by_payment_type():
    lines = flatten_result(load_output('nb_sales_by_grouped_shop_by_payment_type'))

    assert len(lines) == 6  # 2 groups, 3 payment types

    # Lines are not sorted by doc_count but by the order of the groups
    assert lines == sorted(lines, key=(lambda l: l['shop_id']))

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


def test_nb_sales_by_payment_type_by_grouped_shop():
    lines = flatten_result(load_output('nb_sales_by_payment_type_by_grouped_shop'))

    assert len(lines) == (2 * 3)  # 2 groups, 3 payment types

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


def test_avg_sales_by_grouped_shop():
    lines = flatten_result(load_output('avg_sales_by_grouped_shop'))

    assert len(lines) == 2

    # lines are sorted by the order of the groups
    assert lines == sorted(lines, key=(lambda l: l['shop_id']))

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
