# -*- coding: utf-8 -*-

import json
import time

from elasticsearch.helpers import bulk

from elasticsearch_dsl import Mapping

import pytest

from fiqs.testing.utils import get_client


FIXTURE_PATH = 'fiqs/testing/fixtures/shop_fixture.json'
INDEX_NAME = 'test_shop'
DOC_TYPE = 'sale'


def sale_mapping():
    m = Mapping('sale')

    m.meta('dynamic', 'strict')

    m.field('id', 'integer')
    m.field('shop_id', 'integer')
    m.field('product_id', 'keyword')
    m.field('timestamp', 'date')
    m.field('price', 'integer')
    m.field('payment_type', 'keyword')

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


def insert_shop_documents(client):
    create_shop_index(client)
    insert_documents(client, INDEX_NAME, FIXTURE_PATH, DOC_TYPE)


def create_shop_index(client):
    create_index(client, INDEX_NAME, sale_mapping())


def delete_shop_index(client):
    delete_index(client, INDEX_NAME)


@pytest.fixture
def elasticsearch(request):
    client = get_client()
    insert_shop_documents(client)

    request.addfinalizer(lambda: delete_shop_index(client))
    time.sleep(1)

    return client
