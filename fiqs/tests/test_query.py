# -*- coding: utf-8 -*-

from fiqs.aggregations import Sum, Count, Avg
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
