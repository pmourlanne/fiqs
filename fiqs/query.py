# -*- coding: utf-8 -*-

from collections import OrderedDict

from fiqs.aggregations import Aggregate
from fiqs.fields import NestedField, ReverseNestedField, Field


def calc_group_by_keys(group_by_fields, nested=True):
    ret = []
    for field in group_by_fields:
        if isinstance(field, Aggregate):
            ret.append(field.field.key)
        elif isinstance(field, NestedField):
            if nested:
                ret.append(field.key)
        elif isinstance(field, ReverseNestedField):
            continue
        else:
            ret.append(field.key)
    return ret


def group_by_fields_and_keys(group_by_fields):
    ret = []
    for field in group_by_fields:
        if isinstance(field, Aggregate):
            ret.append((field, field.field.key))
        else:
            ret.append((field, field.key))
    return ret


class QueryMetric(object):
    def __init__(self, query, expressions):
        self._query = query
        self._expressions = expressions
        self._group_by = []
        self._order_by = {}

    def _check_nested_parents_are_present(self):
        nested_fields_to_add = {}

        for idx, field in enumerate(self._group_by):
            parent_field = field.get_parent_field()
            if not parent_field:
                continue

            if parent_field in self._group_by:
                continue

            if parent_field in nested_fields_to_add.values():
                continue

            nested_fields_to_add[idx] = parent_field

        indices = sorted(nested_fields_to_add.keys(), reverse=True)
        for idx in indices:
            self._group_by.insert(idx, nested_fields_to_add[idx])

    def group_by(self, *args):
        self._group_by = list(args)

        self._check_nested_parents_are_present()

        return self

    def order_by(self, order_dict):
        self._order_by = order_dict

        return self

    def eval(self):
        return self._query.evaluate_metric(self)

    def add(self, name, expression):
        self._expressions[name] = expression
        return self

    def group_by_keys(self, nested=True):
        return calc_group_by_keys(self._group_by, nested)


class Query(object):
    def metric(self, *expressions, **named_expressions):
        # /!\ named_expressions may not be correctly ordered
        # It may break some Operation fields
        exps = OrderedDict()
        for exp in expressions:
            exps[str(exp)] = exp

        exps.update(named_expressions)

        metric = QueryMetric(self, exps)

        return metric

    def evaluate_metric(self, metric):
        return NotImplemented


class FQuery(Query):
    def __init__(self, search, default_size=None):
        self.search = search

        if default_size == 0:
            default_size = 2 ** 32 - 1
        self.default_size = default_size

    def configure_search(self, metric):
        search = self.search

        agg = self.configure_aggregations(search, metric)
        self.configure_metrics(agg, metric)

        return search

    def evaluate_metric(self, metric):
        search = self.configure_search(metric)
        return search.execute()

    def configure_aggregations(self, search, metric):
        current_agg = search.aggs
        last_idx = len(metric._group_by) - 1

        for idx, field_or_exp in enumerate(metric._group_by):
            if isinstance(field_or_exp, Aggregate):
                current_agg = current_agg.bucket(**field_or_exp.agg_params())
                continue

            if field_or_exp.is_range():
                current_agg = current_agg.bucket(**field_or_exp.range_params())

            elif isinstance(field_or_exp, NestedField)\
                or isinstance(field_or_exp, ReverseNestedField):
                current_agg = current_agg.bucket(**field_or_exp.nested_params())

            elif field_or_exp.choices:
                params = field_or_exp.bucket_params()
                params['agg_type'] = 'terms'
                if self.default_size:
                    params['default_size'] = self.default_size
                current_agg = current_agg.bucket(**params)

            elif isinstance(field_or_exp, Field):
                params = field_or_exp.bucket_params()

                # /!\ TODO: not working with two group_by
                if metric._order_by:
                    # If we're at the latest group_by, we can order by
                    if idx == last_idx:
                        params['order'] = metric._order_by
                    # If the order_by is a doc count, we can add it at any level
                    elif metric._order_by.keys() == ['_count']:
                        params['order'] = metric._order_by

                params['agg_type'] = 'terms'
                if self.default_size:
                    params['size'] = self.default_size
                current_agg = current_agg.bucket(**params)

            else:
                raise NotImplementedError

        return current_agg

    def configure_metrics(self, agg, metric):
        for key, expression in metric._expressions.items():
            if expression.is_field_agg():
                op = expression.__class__.__name__.lower()
                es_metric_name = key
                agg.metric(
                    es_metric_name,
                    op,
                    field=expression.field.get_storage_field(),
                    **expression.params
                )
