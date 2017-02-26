# -*- coding: utf-8 -*-

import json
import os
import time

from elasticsearch.helpers import bulk

from elasticsearch_dsl import Mapping, Nested

import pytest

from fiqs.testing.utils import get_client


SALE_INDEX_NAME = 'test_sale'
TRAFFIC_INDEX_NAME = 'test_traffic'

SALE_FIXTURE_PATH = 'fiqs/testing/fixtures/shop_fixture.json'
TRAFFIC_FIXTURE_PATH = 'fiqs/testing/fixtures/traffic_fixture.json'

SALE_DOC_TYPE = 'sale'
TRAFFIC_DOC_TYPE = 'traffic_count'


def sale_mapping():
    m = Mapping(SALE_DOC_TYPE)

    m.meta('dynamic', 'strict')

    m.field('id', 'integer')
    m.field('shop_id', 'integer')
    m.field('client_id', 'keyword')
    m.field('timestamp', 'date')
    m.field('price', 'integer')
    m.field('payment_type', 'keyword')

    products = Nested()
    products.field('product_id', 'keyword')
    products.field('product_type', 'keyword')
    products.field('product_price', 'integer')

    parts = Nested()
    parts.field('part_id', 'keyword')
    parts.field('warehouse_id', 'keyword')
    parts.field('part_price', 'integer')

    products.field('parts', parts)
    m.field('products', products)

    return m


def traffic_mapping():
    m = Mapping(TRAFFIC_DOC_TYPE)

    m.meta('dynamic', 'strict')

    m.field('id', 'integer')
    m.field('shop_id', 'integer')
    m.field('timestamp', 'date')
    m.field('duration', 'integer')
    m.field('incoming_traffic', 'integer')
    m.field('outgoing_traffic', 'integer')

    return m


def create_index(client, index_name, mapping):
    request_body = {'mappings': mapping.to_dict()}
    return client.indices.create(index=index_name, body=request_body, ignore=400)


def delete_index(client, index_name):
    return client.indices.delete(index=index_name)


def insert_documents(client, index_name, fixture_path, doc_type):
    with open(fixture_path) as f:
        lines = f.readlines()

    events = [json.loads(line) for line in lines]

    actions = []
    for event in events:
        actions.append({
            '_index': index_name,
            '_type': 'sale',
            '_source': event,
            '_id': event['id'],
        })

    return bulk(client, actions)


def insert_sale_documents(client):
    create_sale_index(client)
    insert_documents(client, SALE_INDEX_NAME, SALE_FIXTURE_PATH, SALE_DOC_TYPE)


def insert_traffic_documents(client):
    create_traffic_index(client)
    insert_documents(client, TRAFFIC_INDEX_NAME, TRAFFIC_FIXTURE_PATH, TRAFFIC_DOC_TYPE)


def create_sale_index(client):
    create_index(client, SALE_INDEX_NAME, sale_mapping())


def create_traffic_index(client):
    create_index(client, TRAFFIC_INDEX_NAME, traffic_mapping())


def delete_sale_index(client):
    delete_index(client, SALE_INDEX_NAME)


def delete_traffic_index(client):
    delete_index(client, TRAFFIC_INDEX_NAME)


@pytest.fixture
def elasticsearch_sale(request):
    client = get_client()
    insert_sale_documents(client)

    request.addfinalizer(lambda: delete_sale_index(client))
    time.sleep(1)

    return client


@pytest.fixture
def elasticsearch_traffic(request):
    client = get_client()
    insert_traffic_documents(client)

    request.addfinalizer(lambda: delete_traffic_index(client))
    time.sleep(1)

    return client


BASE_PATH = 'fiqs/testing/outputs/'


def write_output(search, name):
    result = search.execute()

    path = os.path.join(BASE_PATH, '{}.json'.format(name))
    with open(path, 'w') as f:
        d = result._d_
        d.pop('took', None)  # Not used and may change between calls

        json.dump(result._d_, f, indent=4, ensure_ascii=False, encoding='utf-8', sort_keys=True)


def write_fquery_output(fquery, name):
    result = fquery.eval(flat=False)

    path = os.path.join(BASE_PATH, '{}.json'.format(name))
    with open(path, 'w') as f:
        d = result._d_
        d.pop('took', None)  # Not used and may change between calls

        json.dump(result._d_, f, indent=4, ensure_ascii=False, encoding='utf-8', sort_keys=True)


def load_output(name):
    path = os.path.join(BASE_PATH, '{}.json'.format(name))

    with open(path, 'r') as f:
        output = json.load(f)

    return output
