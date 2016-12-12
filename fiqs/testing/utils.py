# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


def get_client():
    return Elasticsearch(['http://localhost:8200'], timeout=60)


def get_search(client=None, indices=None):
    indices = indices or '*'
    client = client or get_client()
    return Search(using=client, index=indices)
