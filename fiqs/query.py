# -*- coding: utf-8 -*-

from collections import OrderedDict
from itertools import product

from fiqs import flatten_result
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
        while True:
            nested_fields_to_add = {}

            for idx, field in enumerate(self._group_by):
                if not isinstance(field, Field):
                    continue

                parent_field = field.get_parent_field()
                if not parent_field:
                    continue

                if parent_field in self._group_by:
                    continue

                if parent_field in nested_fields_to_add.values():
                    continue

                nested_fields_to_add[idx] = parent_field

            if not nested_fields_to_add:
                break

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

    def eval(self, **kwargs):
        return self._query.evaluate_metric(self, **kwargs)

    def add(self, name, expression):
        self._expressions[name] = expression
        return self

    def group_by_keys(self, nested=True):
        return calc_group_by_keys(self._group_by, nested)


class FQuery(object):
    def __init__(self, search, default_size=None):
        self.search = search

        if default_size == 0:
            default_size = 2 ** 32 - 1
        self.default_size = default_size

    def metric(self, *expressions, **named_expressions):
        # /!\ named_expressions may not be correctly ordered
        # It may break some Operation fields
        exps = OrderedDict()
        for exp in expressions:
            exps[str(exp)] = exp

        exps.update(named_expressions)

        metric = QueryMetric(self, exps)

        return metric

    def configure_search(self, metric):
        search = self.search

        agg = self.configure_aggregations(search, metric)
        self.configure_metrics(agg, metric)

        return search

    def _flatten_result(self, metric, result):
        lines = flatten_result(result)

        key_to_field = {}
        for key, exp in metric._expressions.items():
            key_to_field[key] = exp
        for field_or_exp in metric._group_by:
            if isinstance(field_or_exp, Aggregate):
                key_to_field[field_or_exp.field.storage_field] = field_or_exp
            else:
                key_to_field[field_or_exp.storage_field] = field_or_exp

        pretty_lines = []
        for line in lines:
            pretty_line = line.copy()

            for key, value in pretty_line.items():
                if key in key_to_field:
                    field = key_to_field[key]
                    pretty_line[key] = field.get_casted_value(value)

            pretty_lines.append(pretty_line)

        return pretty_lines

    def _get_field_enums(self, metric, result, lines):
        enums = []

        for field in metric._group_by:
            if isinstance(field, Aggregate):
                if field.choice_keys():
                    enums.append(field.choice_keys())
                elif field.field.choices:
                    enums.append(field.field.choice_keys())
                else:
                    # We just add the lines' values
                    key = field.field.key
                    values = set([line[key] for line in lines])
                    values = sorted(list(values))
                    enums.append(values)

            elif field.is_range():
                enums.append(field.choice_keys())

            else:
                if field.choices:  # range / keyed?
                    enums.append(field.choice_keys())

                else:
                    # /!\ uncommon terms (beyond 10 first) will not be fetched by default
                    if 'custom_choices' in field.data:  #  and self.default_size == 0
                        # We add the custom choices
                        enums.append([c for c in field.data['custom_choices']])
                    else:
                        # We add the lines' values
                        key = field.key
                        values = set([line[key] for line in lines])
                        values = sorted(list(values))
                        enums.append(values)

        return enums

    def _add_missing_lines(self, metric, result, lines):
        enums = self._get_field_enums(metric, result, lines)

        keys = list(product(*enums)) if enums else []
        if len(keys) == len(lines):
            return lines

        group_by_keys_without_nested = metric.group_by_keys(nested=False)
        # We cast everything as str for easier matching, it won't change the type of keys in lines
        treated_hashes = [
            u','.join([unicode(line[key]) for key in group_by_keys_without_nested])
            for line in lines
        ]
        treated_hashes = set(treated_hashes)
        missing_keys = [
            key
            for key in keys
            if u','.join([unicode(k) for k in key]) not in treated_hashes
        ]

        lines += self._create_missing_lines(
            missing_keys,
            group_by_keys_without_nested,
            metric._expressions,
        )

        return lines

    def _create_empty_line(self, base_line, expressions):
        empty_line = base_line.copy()

        for key, expression in expressions.items():
            empty_line[key] = None

        empty_line['doc_count'] = 0

        return empty_line

    def _create_missing_lines(self, missing_keys, group_by_keys, expressions):
        lines = []

        for missing_key in missing_keys:
            base_line = {}
            for idx, group_by_key in enumerate(group_by_keys):
                current_key = group_by_keys[idx]
                value = missing_key[idx]
                if hasattr(value, 'original_value'):  # In case of Choices
                    value = value.original_value
                base_line[current_key] = value

            lines.append(self._create_empty_line(base_line, expressions))

        return lines

    def evaluate_metric(self, metric, flat=True, fill_missing_buckets=True):
        search = self.configure_search(metric)
        result = search.execute()

        if flat:
            lines = self._flatten_result(metric, result)

            if fill_missing_buckets:
                lines = self._add_missing_lines(metric, result, lines)

            return lines
        else:
            return result

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
