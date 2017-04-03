# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from fiqs.aggregations import DateHistogram

from fiqs.testing.models import Sale


def get_date_histogram(**kwargs):
    date_histogram = DateHistogram(
        Sale.timestamp,
        **kwargs
    )
    date_histogram.agg_params()  # To set min and max
    return date_histogram


def test_date_histogram_choice_keys_day_intervals():
    start = datetime(2016, 1, 1, 6)
    end = datetime(2016, 2, 1, 6)
    nb_days = (end - start).days + 1

    normalized_start = start.replace(hour=0)

    date_histogram = get_date_histogram(min=start, max=end, interval='1d')
    expected_keys = [normalized_start + timedelta(days=i) for i in range(0, nb_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys

    date_histogram = get_date_histogram(min=start, max=end, interval='day')
    expected_keys = [normalized_start + timedelta(days=i) for i in range(0, nb_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys

    date_histogram = get_date_histogram(min=start, max=end, interval='2d')
    expected_keys = [normalized_start + timedelta(days=i) for i in range(0, nb_days, 2)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_second_intervals():
    start = datetime(2016, 1, 1, 6, microsecond=123)
    end = start + timedelta(days=1)
    nb_seconds = int((end - start).total_seconds()) + 1

    normalized_start = start.replace(microsecond=0)

    date_histogram = get_date_histogram(min=start, max=end, interval='400s')
    expected_keys = [normalized_start + timedelta(seconds=i) for i in range(0, nb_seconds, 400)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys
