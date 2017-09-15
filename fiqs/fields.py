# -*- coding: utf-8 -*-

import copy
from datetime import datetime

from fiqs.i18n import _


class Field(object):
    def __init__(self, type, key=None, verbose_name=None, storage_field=None,
                 unit=None, choices=None, data=None, parent=None, model=None):
        verbose_name = verbose_name or key
        storage_field = storage_field or key
        choices = choices or ()
        data = data or {}

        self.key = key
        self.type = type
        self.verbose_name = verbose_name
        self.storage_field = storage_field
        self.unit = unit
        self.choices = choices
        self.data = data
        self.parent = parent
        if model:
            self.model = model

    def get_copy(self):
        return self.__class__(
            key=self.key, verbose_name=self.verbose_name, storage_field=self.storage_field,
            unit=self.unit, choices=self.choices, data=self.data, parent=self.parent,
        )

    def __repr__(self):
        if hasattr(self, 'model'):
            return '<{}: {}.{}>'.format(
                self.__class__.__name__,
                self.model.__name__,
                self.key,
            )

        return '<{}: {}>'.format(
                self.__class__.__name__,
                self.key,
        )

    def _set_key(self, key):
        self.key = key
        if not self.verbose_name:
            self.verbose_name = key
        if not self.storage_field:
            self.storage_field = key

    def get_storage_field(self):
        parent_field = self.get_parent_field()
        if not parent_field:
            return self.storage_field

        return '{}.{}'.format(
            parent_field.get_storage_field(),
            self.storage_field,
        )

    def bucket_params(self):
        d = {
            'name': self.key,
            'field': self.get_storage_field(),
            'agg_type': 'terms',
        }

        if 'script' in self.data:
            # should we remove field?
            d['script'] = self.data['script'].format('_value')

        if 'size' in self.data:
            size = self.data['size']
            if size == 0:
                size = 2 ** 31 - 1
            d['size'] = size

        return d

    def is_range(self):
        return 'ranges' in self.data

    def _has_pretty_choices(self):
        return self.choices and isinstance(self.choices[0], tuple)

    def _get_ranges_as_dict(self):
        if 'ranges' not in self.data:
            return None

        if isinstance(self.data['ranges'][0], dict):
            return self.data['ranges']

        if isinstance(self.data['ranges'][0], tuple) or\
             isinstance(self.data['ranges'][0], list):
            ranges = []
            for start, end in self.data['ranges']:
                ranges.append({
                    'from': start,
                    'to': end,
                    'key': '{} - {}'.format(start, end),
                })
            return ranges

        raise NotImplementedError()

    def range_params(self):
        params = self.bucket_params()

        params['agg_type'] = 'range'
        params['keyed'] = True

        if 'ranges' in self.data:
            params['ranges'] = self._get_ranges_as_dict()

        else:
            raise NotImplementedError()

        return params

    def choice_keys(self):
        if self._has_pretty_choices():
            return [choice[0] for choice in self.choices]

        if self.choices:
            return self.choices

        if self.is_range():
            return [r['key'] for r in self._get_ranges_as_dict()]

        raise NotImplementedError()

    def min_key(self):
        if self.data and 'min' in self.data:
            return self.data['min']

        if self.choices:
            return min(self.choice_keys())

    def max_key(self):
        if self.data and 'max' in self.data:
            return self.data['max']

        if self.choices:
            return max(self.choice_keys())

    def get_parent_field(self):
        if not self.parent:
            return None
        return getattr(self.model, self.parent)

    def get_casted_value(self, v):
        return v


class TextField(Field):
    def __init__(self, **kwargs):
        super(TextField, self).__init__('text', **kwargs)


class KeywordField(Field):
    def __init__(self, **kwargs):
        super(KeywordField, self).__init__('keyword', **kwargs)


class DateField(Field):
    def __init__(self, **kwargs):
        super(DateField, self).__init__('date', **kwargs)

    def get_casted_value(self, v):
        # Careful, we lose the milliseconds here
        return datetime.utcfromtimestamp(v / 1000)


class BaseIntegerField(Field):
    def __init__(self, **kwargs):
        super(BaseIntegerField, self).__init__(self.type, **kwargs)

    def get_casted_value(self, v):
        return int(v) if v is not None else v


class LongField(BaseIntegerField):
    type = 'long'


class IntegerField(BaseIntegerField):
    type = 'integer'


class ShortField(BaseIntegerField):
    type = 'short'


class ByteField(BaseIntegerField):
    type = 'byte'


class BaseFloatField(Field):
    def __init__(self, **kwargs):
        super(BaseFloatField, self).__init__(self.type, **kwargs)

    def get_casted_value(self, v):
        return float(v) if v is not None else v


class DoubleField(BaseFloatField):
    type = 'double'


class FloatField(BaseFloatField):
    type = 'float'


def get_weekdays():
    return [
        (0, _('Monday')),
        (1, _('Tuesday')),
        (2, _('Wednesday')),
        (3, _('Thursday')),
        (4, _('Friday')),
        (5, _('Saturday')),
        (6, _('Sunday')),
    ]


def get_iso_weekdays():
    return [
        (1, _('Monday')),
        (2, _('Tuesday')),
        (3, _('Wednesday')),
        (4, _('Thursday')),
        (5, _('Friday')),
        (6, _('Saturday')),
        (7, _('Sunday')),
    ]


class DayOfWeekField(ByteField):
    def __init__(self, iso=True, **kwargs):
        if iso:
            choices = get_iso_weekdays()
            data = {'min': 1, 'max': 7}
        else:
            choices = get_weekdays()
            data = {'min': 0, 'max': 6}

        kwargs['choices'] = choices
        kwargs['data'] = data

        super(DayOfWeekField, self).__init__(**kwargs)


class HourOfDayField(ByteField):
    def __init__(self, **kwargs):
        kwargs['choices'] = [(i, _('{hour}h').format(hour=i)) for i in range(24)]
        kwargs['data'] = {'min': 0, 'max': 23}

        super(HourOfDayField, self).__init__(**kwargs)


class BooleanField(Field):
    def __init__(self, **kwargs):
        super(BooleanField, self).__init__('boolean', **kwargs)


class NestedField(Field):
    def __init__(self, **kwargs):
        super(NestedField, self).__init__('nested', **kwargs)

    def nested_params(self):
        params = {
            'name': self.key,
            'agg_type': 'nested',
            'path': self.get_storage_field(),
        }
        return params


class FieldWithChoices(Field):
    def __init__(self, field, choices=None):
        choices = choices or ()
        data = copy.deepcopy(field.data)
        return super(FieldWithChoices, self).__init__(
            field.type,
            key=field.key,
            verbose_name=field.verbose_name,
            storage_field=field.storage_field,
            unit=field.unit,
            choices=choices,
            data=data,
            parent=field.parent,
            model=field.model,
        )


class FieldWithRanges(Field):
    def __init__(self, field, ranges=None):
        data = copy.deepcopy(field.data)

        if ranges is not None:
            data['ranges'] = ranges

        return super(FieldWithRanges, self).__init__(
            field.type,
            key=field.key,
            verbose_name=field.verbose_name,
            storage_field=field.storage_field,
            unit=field.unit,
            choices=field.choices,
            data=data,
            parent=field.parent,
            model=field.model,
        )


class DataExtendedField(Field):
    def __init__(self, field, **kwargs):
        data = copy.deepcopy(field.data)
        data.update(**kwargs)
        return super(DataExtendedField, self).__init__(
            field.type,
            key=field.key,
            verbose_name=field.verbose_name,
            storage_field=field.storage_field,
            unit=field.unit,
            choices=field.choices,
            data=data,
            parent=field.parent,
            model=field.model,
        )


class GroupedField(Field):
    def __init__(self, field, groups):
        self.groups = groups

        return super(GroupedField, self).__init__(
            field.type,
            key=field.key,
            verbose_name=field.verbose_name,
            storage_field=field.storage_field,
            unit=field.unit,
            choices=list(self.groups),
            data=field.data,
            parent=field.parent,
            model=field.model,
        )

    def bucket_params(self):
        filters = {
            key: {
                'terms': {
                    self.get_storage_field(): field_values,
                }
            }
            for key, field_values in self.groups.items()
        }

        return {
            'name': self.key,
            'filters': filters,
            'agg_type': 'filters',
        }
