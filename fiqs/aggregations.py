# -*- coding: utf-8 -*-

from collections import OrderedDict
from datetime import timedelta, datetime

import six

from fiqs.exceptions import MissingParameterException
from fiqs.fields import Field
from fiqs.models import Model


class Metric(object):
    def is_field_agg(self):
        return NotImplemented

    def is_doc_count(self):
        return False

    def is_computed(self):
        return False


class Count(Metric):
    def __init__(self, model_or_field):
        if isinstance(model_or_field, Field):
            self.model = model_or_field.model
        else:
            self.model = model_or_field

    def is_doc_count(self):
        return True

    def is_field_agg(self):
        return False

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


def is_interval_standard(interval):
    for key, _ in TIME_UNIT_CONVERSION.items():
        if interval.endswith(key):
            return True

    return False


def is_interval_monthly(interval):
    # Naive approach
    return interval.endswith('M')


def is_interval_handled(interval):
    return is_interval_standard(interval) or is_interval_monthly(interval)


def get_timedelta_from_interval(interval):
    # Some intervals are still missing: year, month, week
    for key, param in TIME_UNIT_CONVERSION.items():
        if interval.endswith(key):
            value = interval.rstrip(key)
            if not value:
                value = '1'

            return timedelta(**{
                param: int(value),
            })

    return None


def get_timedelta_from_timestring(timestring):
    if timestring.startswith('-'):
        minus = True
        timestring = timestring.lstrip('-')
    elif timestring.startswith('+'):
        minus = False
        timestring = timestring.lstrip('+')
    else:
        minus = False

    timedelta = get_timedelta_from_interval(timestring)
    if minus:
        return -timedelta
    else:
        return timedelta


def get_offset_date(d, timestring):
    timedelta = get_timedelta_from_timestring(timestring)

    return d + timedelta


def get_rounded_date_from_interval(d, interval):
    if is_interval_standard(interval):
        return get_rounded_date_from_timedelta(d, get_timedelta_from_interval(interval))

    if is_interval_monthly(interval):
        return d.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    return d


def get_rounded_date_from_timedelta(d, delta):
    epoch = datetime(1970, 1, 1)
    nb_seconds = int((d - epoch).total_seconds())
    delta_seconds = int(delta.total_seconds())

    rounded_nb_seconds = (nb_seconds // delta_seconds) * delta_seconds
    return datetime.utcfromtimestamp(rounded_nb_seconds)


class DateHistogram(Histogram):
    ref = 'date_histogram'

    def _choice_keys_standard(self, start, end, interval):
        delta = get_timedelta_from_interval(self.interval)

        choice_keys = []
        current = start
        while current <= end:
            choice_keys.append(current)
            current += delta

        return choice_keys

    def _choice_keys_monthly(self, start, end, interval):
        nb_months = int(interval.rstrip('M'))

        choice_keys = []
        current = start
        while current <= end:
            choice_keys.append(current)

            next_ = current
            for i in range(nb_months):
                next_ = next_ + timedelta(days=32)  # 32 days to be sure to change month
                next_ = next_.replace(day=current.day)

            current = next_

        return choice_keys

    def choice_keys(self):
        if not hasattr(self, 'min') or not hasattr(self, 'max'):
            return None

        if not is_interval_handled(self.interval):
            return None

        start = get_rounded_date_from_interval(self.min, self.interval)
        agg_params = self.agg_params()
        if 'offset' in agg_params:
            start = get_offset_date(start, agg_params['offset'])

        end = self.max

        if is_interval_standard(self.interval):
            return self._choice_keys_standard(start, end, self.interval)

        elif is_interval_monthly(self.interval):
            return self._choice_keys_monthly(start, end, self.interval)

        else:
            return None


class DateRange(Aggregate):
    ref = 'date_range'

    def agg_params(self):
        params = super(DateRange, self).agg_params()
        params.update(self.params)

        if 'ranges' not in params:
            raise MissingParameterException('missing ranges parameter')

        return params

    def _format_date(self, d):
        # We format the date like ES does
        return d.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _format_date_range(self, date_range):
        start, end = date_range['from'], date_range['to']
        return '{}-{}'.format(self._format_date(start), self._format_date(end))

    def choice_keys(self):
        keys = []

        for date_range in self.params['ranges']:
            if 'key' in date_range:
                keys.append(date_range['key'])
            else:
                keys.append(self._format_date_range(date_range))

        return keys

    def get_casted_value(self, v):
        return v


class ReverseNested(Metric):
    def __init__(self, path_or_field_or_model, *expressions, **named_expressions):
        # /!\ named_expressions may not be correctly ordered
        if isinstance(path_or_field_or_model, six.text_type)\
        or isinstance(path_or_field_or_model, str):
            self.path = path_or_field_or_model or 'root'
        elif isinstance(path_or_field_or_model, Field):
            self.path = path_or_field_or_model.get_storage_field()
        elif issubclass(path_or_field_or_model, Model):
            self.path = 'root'

        self._expressions = OrderedDict()
        for exp in expressions:
            self._expressions[str(exp)] = exp
        self._expressions.update(named_expressions)

    def __str__(self):
        keys = '__'.join(self._expressions.keys())
        return 'reverse_nested_{}__{}'.format(self.path, keys)

    def reverse_agg_params(self):
        params = {
            'name': 'reverse_nested_{}'.format(self.path),
            'agg_type': 'reverse_nested',
        }

        if self.path != 'root':
            params['path'] = self.path

        return params

    def get_casted_value(self, v):
        return v

    def configure_aggregations(self, agg):
        reverse_agg_params = self.reverse_agg_params()
        reverse_nested_bucket = agg.bucket(
            **reverse_agg_params
        )

        for key, expression in self._expressions.items():
            if expression.is_field_agg():
                op = expression.__class__.__name__.lower()
                reverse_nested_bucket.metric(
                    key,
                    op,
                    field=expression.field.get_storage_field(),
                    **expression.params
                )

    @property
    def expressions(self):
        return {
            'reverse_nested_{}__{}'.format(self.path, key): expression
            for key, expression in self._expressions.items()
        }

    def create_empty_line(self):
        line = {}

        for key, expression in self.expressions.items():
            if expression.is_doc_count():
                continue
            line[key] = None

        line['reverse_nested_{}__doc_count'.format(self.path)] = 0

        return line


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
