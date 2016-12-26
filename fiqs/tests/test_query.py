# -*- coding: utf-8 -*-

from fiqs.aggregations import Sum, Count, Avg
from fiqs.fields import DataExtendedField
from fiqs.query import FQuery

from fiqs.testing.models import Sale
from fiqs.testing.utils import get_search


def test_one_metric():
    expected = get_search().aggs.metric(
        'total_sales', 'sum', field='price',
    ).to_dict()

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    )
    search = fquery.configure_search(metric)

    assert expected == search.to_dict()


def test_one_aggregation():
    expected = get_search().aggs.metric(
        'shop_id', 'terms', field='shop_id',
    ).to_dict()

    fquery = FQuery(get_search())
    metric = fquery.metric(
        Count(Sale),
    ).group_by(
        Sale.shop_id,
    )
    search = fquery.configure_search(metric)

    assert expected == search.to_dict()


def test_one_aggregation_one_metric():
    search = get_search()
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
        avg_sales=Avg(Sale.price),
    ).group_by(
        Sale.shop_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
        avg_sales=Avg(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        avg_product_price=Avg(Sale.product_price),
    ).group_by(
        Sale.products,
        Sale.product_type,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.products,
        Sale.product_id,
        Sale.parts,
        Sale.part_id,
    )
    fsearch = fquery.configure_search(metric)

    assert search.to_dict() == fsearch.to_dict()


def test_nested_parent_automatically_added():
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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery.configure_search(metric)

    assert search.to_dict() == fsearch.to_dict()

    fquery = FQuery(get_search())
    metric = fquery.metric(
        avg_part_price=Avg(Sale.part_price),
    ).group_by(
        Sale.products,
        Sale.product_id,
        Sale.part_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search(), default_size=5)
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        DataExtendedField(Sale.shop_id, size=5),
        Sale.client_id,
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    ).order_by(
        {'total_sales': 'desc'}
    )
    fsearch = fquery.configure_search(metric)

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

    fquery = FQuery(get_search())
    metric = fquery.metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    ).order_by(
        {'total_sales': 'desc'}
    )
    fsearch = fquery.configure_search(metric)

    assert fsearch.to_dict() == expected


def test_order_by_count():
    pass
