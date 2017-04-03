# -*- coding: utf-8 -*-

from datetime import timedelta

from fiqs.exceptions import MissingParameterException


class Metric(object):
    def is_range(self):
        return False

    def is_field_agg(self):
        return NotImplemented

    def is_doc_count(self):
        return False

    def is_computed(self):
        return False


class ModelMetric(Metric):
    def __init__(self, model):
        self.model = model

    def is_field_agg(self):
        return False


class Count(ModelMetric):
    def is_doc_count(self):
        return True

    def __str__(self):
        return 'doc_count'

    def order_by_key(self):
        return '_count'


class Aggregate(Metric):
    def reference(self):
        if hasattr(self, 'ref'):
            return self.ref
        return self.__class__.__name__.lower()

    def __init__(self, field, **kwargs):
        self.field = field
        self.params = kwargs

    def is_field_agg(self):
        return True

    def __str__(self):
        model = self.field.model.__name__.lower()
        op = self.__class__.__name__.lower()
        return '{}__{}__{}'.format(model, self.field.key, op)

    def agg_params(self):
        params = {
            'name': self.field.key,
            'field': self.field.get_storage_field(),
            'agg_type': self.reference(),
        }
        return params

    def choice_keys(self):
        return None

    def get_casted_value(self, v):
        return self.field.get_casted_value(v)


class Avg(Aggregate):
    def get_casted_value(self, v):
        """Average of an IntegerField does not have to be an integer"""
        return v


class Max(Aggregate): pass
class Min(Aggregate): pass
class Sum(Aggregate): pass
class Cardinality(Aggregate): pass


class Histogram(Aggregate):
    ref = 'histogram'

    def agg_params(self):
        params = super(Histogram, self).agg_params()
        params.update({
            'min_doc_count': 0,
        })

        if 'min' in self.params and 'max' in self.params:
            self.min = self.params.pop('min')
            self.max = self.params.pop('max')

            params['extended_bounds'] = {
                'min': self.min,
                'max': self.max,
            }

        params.update(self.params)

        if 'interval' not in params:
            raise MissingParameterException('missing interval parameter')

        self.interval = params['interval']

        return params


def get_timedelta_from_timestring(timestring):
    pass


TIME_UNIT_CONVERSION = {
    'd': 'days',
    'day': 'days',
    'H': 'hours',
    'h': 'hours',
    'hour': 'hours',
    'm': 'minutes',
    'minute': 'minutes',
    's': 'seconds',
    'second': 'seconds',
}


def get_timedelta_from_interval(interval):
    # Some intervals are still missing: year, quarter, month, week
    for key, param in TIME_UNIT_CONVERSION.items():
        if interval.endswith(key):
            value = interval.rstrip(key)
            if not value:
                value = '1'

            return timedelta(**{
                param: int(value),
            })

    return None


def get_rounded_date_from_interval(d, interval):
    kwargs = {
        'microsecond': 0,
    }

    if interval.endswith('d') or interval.endswith('day'):
        kwargs['minute'] = kwargs['hour'] = kwargs['second'] = 0
        return d.replace(**kwargs)

    if interval.endswith('H') or interval.endswith('h') or interval.endswith('hour'):
        kwargs['minute'] = kwargs['second'] = 0
        return d.replace(**kwargs)

    if interval.endswith('m') or interval.endswith('minute'):
        kwargs['second'] = 0
        return d.replace(**kwargs)

    if interval.endswith('s') or interval.endswith('second'):
        return d.replace(**kwargs)

    return d


class DateHistogram(Histogram):
    ref = 'date_histogram'

    def choice_keys(self):
        if not hasattr(self, 'min') or not hasattr(self, 'max'):
            return None

        delta = get_timedelta_from_interval(self.interval)
        if not delta:
            return None

        start = get_rounded_date_from_interval(self.min, self.interval)
        end = get_rounded_date_from_interval(self.max, self.interval)

        choice_keys = []
        current = start
        while current <= end:
            choice_keys.append(current)
            current += delta

        return choice_keys


class DateRange(Aggregate):
    ref = 'date_range'

    def agg_params(self):
        params = super(DateRange, self).agg_params()
        params.update(self.params)

        if 'ranges' not in params:
            raise MissingParameterException('missing ranges parameter')

        return params


class Operation(Metric):
    def is_field_agg(self):
        return False

    def is_computed(self):
        return True

    def compute_one(self, row):
        raise NotImplementedError

    def compute(self, results, key=None):
        raise NotImplementedError

    def get_casted_value(self, v):
        return v

    @property
    def operands(self):
        return self._operands

    def __init__(self, *args):
        self._operands = args


def div_or_none(a, b, percentage=False):
    base = 100.0 if percentage else 1.0
    if b and a is not None:
        return base * a / b
    return None


def add_or_none(operands):
    if any([op is None for op in operands]):
        return None
    return sum(operands)


def sub_or_none(a, b):
    if a is not None and b is not None:
        return a - b
    return None


class Ratio(Operation):
    def __init__(self, dividend, divisor):
        super(Ratio, self).__init__(dividend, divisor)

        self.dividend = dividend
        self.divisor = divisor

    def __str__(self):
        return '{}__div__{}'.format(self.dividend, self.divisor)

    def compute_one(self, row):
        dividend = row[str(self.dividend)]

        if not self.divisor.is_computed():
            divisor = row[str(self.divisor)]
        else:
            divisor = self.divisor.compute_one(row)

        return div_or_none(dividend, divisor, percentage=True)


class Addition(Operation):
    def __str__(self):
        return '__add__'.join(['{}'.format(op) for op in self.operands])

    def compute_one(self, row):
        keys = [str(op) for op in self.operands]
        return add_or_none([row[key] for key in keys])


class Subtraction(Operation):
    def __init__(self, minuend, subtraend):
        super(Subtraction, self).__init__(minuend, subtraend)

        self.minuend = minuend
        self.subtraend = subtraend

    def __str__(self):
        return '{}__sub__{}'.format(self.minuend, self.subtraend)

    def compute_one(self, row):
        key_a = str(self.minuend)
        key_b = str(self.subtraend)

        return sub_or_none(row[key_a], row[key_b])
