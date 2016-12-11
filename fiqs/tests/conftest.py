# -*- coding: utf-8 -*-

import json
import time

from elasticsearch.helpers import bulk

import pytest

from fiqs.testing.utils import get_client


FIXTURE_PATH = 'fiqs/testing/fixtures/shop_fixture.json'
INDEX_NAME = 'test_shop'
EVENT_TYPE = 'sale'


def create_index(client, index_name):
    return client.indices.create(index=index_name)


def delete_index(client, index_name):
    return client.indices.delete(index=index_name)


def insert_events(client, index_name, fixture_path, event_type):
    with open(fixture_path) as f:
        lines = f.readlines()

    events = [json.loads(line) for line in lines]

    actions = []
    for event in events:
        actions.append({
            '_index': index_name,
            '_type': 'watcher',
            '_source': event,
            '_id': event['id'],
        })

    return bulk(client, actions)


@pytest.fixture
def elasticsearch(request):
    client = get_client()
    insert_events(client, INDEX_NAME, FIXTURE_PATH, EVENT_TYPE)

    request.addfinalizer(lambda: delete_index(client, INDEX_NAME))

    time.sleep(1)

    return client
