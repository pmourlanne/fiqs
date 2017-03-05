# -*- coding: utf-8 -*-

from collections import OrderedDict
from itertools import product
from six import text_type as unicode

from fiqs import flatten_result
from fiqs.aggregations import Aggregate
from fiqs.exceptions import ConfigurationError
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


class FQuery(object):
    def __init__(self, search, default_size=None):
        self.search = search

        if default_size == 0:
            default_size = 2 ** 32 - 1
        self.default_size = default_size

        self._expressions = OrderedDict()
        self._group_by = []
        self._order_by = {}

    def values(self, *expressions, **named_expressions):
        # /!\ named_expressions may not be correctly ordered
        # It may break some Operation fields
        exps = OrderedDict()
        for exp in expressions:
            exps[str(exp)] = exp

        exps.update(named_expressions)
        self._expressions.update(exps)

        self._check_exps_for_computed_are_present()

        return self

    def group_by(self, *args):
        self._group_by += args

        self._check_nested_parents_are_present()

        return self

    def order_by(self, order_dict):
        self._order_by.update(order_dict)

        return self

    def eval(self, flat=True, fill_missing_buckets=True):
        # Raise if computed fields are present, and we are not in flat mode
        if not flat:
            for expression in self._expressions.values():
                if expression.is_computed():
                    raise ConfigurationError(u'Cannot use computed fields in non-flat mode')

        search = self._configure_search()
        result = search.execute()

        if flat:
            lines = self._flatten_result(result)

            if fill_missing_buckets:
                lines = self._add_missing_lines(result, lines)

            return lines
        else:
            return result

    ################
    # Internal API #
    ################
    def _check_exps_for_computed_are_present(self):
        while True:
            exps_to_add = {}
            expression_keys = [str(exp) for exp in self._expressions.values()]

            for key, expression in self._expressions.items():
                if not expression.is_computed():
                    continue

                operands = expression.operands
                for op in operands:
                    if str(op) not in expression_keys:
                        exps_to_add[str(op)] = op
                        expression_keys.append(str(op))

            if not exps_to_add:
                break

            self._expressions.update(exps_to_add)

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

    def _configure_search(self):
        agg = self._configure_aggregations()
        self._configure_values(agg)

        return self.search

    def _configure_aggregations(self):
        current_agg = self.search.aggs
        last_idx = len(self._group_by) - 1

        for idx, field_or_exp in enumerate(self._group_by):
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
                if self._order_by:
                    # If we're at the latest group_by, we can order by
                    if idx == last_idx:
                        params['order'] = self._order_by
                    # If the order_by is a doc count, we can add it at any level
                    elif self._order_by.keys() == ['_count']:
                        params['order'] = self._order_by

                params['agg_type'] = 'terms'
                if self.default_size:
                    params['size'] = self.default_size
                current_agg = current_agg.bucket(**params)

            else:
                raise NotImplementedError

        return current_agg

    def _configure_values(self, agg):
        for key, expression in self._expressions.items():
            if expression.is_field_agg():
                op = expression.__class__.__name__.lower()
                es_metric_name = key
                agg.metric(
                    es_metric_name,
                    op,
                    field=expression.field.get_storage_field(),
                    **expression.params
                )

    def _flatten_result(self, result):
        lines = flatten_result(result)

        key_to_field = {}
        for key, exp in self._expressions.items():
            key_to_field[key] = exp
        for field_or_exp in self._group_by:
            if isinstance(field_or_exp, Aggregate):
                key_to_field[field_or_exp.field.storage_field] = field_or_exp
            else:
                key_to_field[field_or_exp.storage_field] = field_or_exp

        pretty_lines = []
        for line in lines:
            pretty_line = line.copy()
            self._add_computed_results(pretty_line)

            for key, value in pretty_line.items():
                if key in key_to_field:
                    field = key_to_field[key]
                    pretty_line[key] = field.get_casted_value(value)

            pretty_lines.append(pretty_line)

        return pretty_lines

    def _add_computed_results(self, line):
        computed_expressions = []

        while True:
            expressions_to_compute = []

            for key, expression in self._expressions.items():
                if not expression.is_computed():
                    continue

                if key in computed_expressions:
                    continue

                expressions_to_compute.append((key, expression))

            if not expressions_to_compute:
                break

            for key, expression in expressions_to_compute:
                try:
                    line[key] = expression.compute_one(line)
                    computed_expressions.append(key)
                except KeyError:
                    pass

    def _add_missing_lines(self, result, lines):
        enums = self._get_field_enums(result, lines)

        keys = list(product(*enums)) if enums else []
        if len(keys) == len(lines):
            return lines

        group_by_keys_without_nested = self._group_by_keys(nested=False)
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
        )

        return lines

    def _get_field_enums(self, result, lines):
        enums = []

        for field in self._group_by:
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

    def _group_by_keys(self, nested=True):
        return calc_group_by_keys(self._group_by, nested)

    def _create_missing_lines(self, missing_keys, group_by_keys):
        lines = []

        for missing_key in missing_keys:
            base_line = {}
            for idx, group_by_key in enumerate(group_by_keys):
                current_key = group_by_keys[idx]
                value = missing_key[idx]
                if hasattr(value, 'original_value'):  # In case of Choices
                    value = value.original_value
                base_line[current_key] = value

            lines.append(self._create_empty_line(base_line))

        return lines

    def _create_empty_line(self, base_line):
        empty_line = base_line.copy()

        for key, expression in self._expressions.items():
            empty_line[key] = None

        empty_line['doc_count'] = 0

        return empty_line
