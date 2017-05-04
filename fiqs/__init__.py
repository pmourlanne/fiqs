# -*- coding: utf-8 -*-

from .tree import ResultTree


def flatten_result(es_result, **kwargs):
    return ResultTree(es_result).flatten_result(**kwargs)
