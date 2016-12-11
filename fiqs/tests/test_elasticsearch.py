# -*- coding: utf-8 -*-

from fiqs.testing.utils import get_search


def test_count(elasticsearch):
    assert get_search().count() == 500
