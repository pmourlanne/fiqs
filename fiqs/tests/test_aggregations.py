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
    delta_days = 1
    nb_days = (end - start).days + delta_days

    expected_start = start.replace(hour=0)

    date_histogram = get_date_histogram(min=start, max=end, interval='1d')
    expected_keys = [expected_start + timedelta(days=i) for i in range(0, nb_days, delta_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys

    date_histogram = get_date_histogram(min=start, max=end, interval='day')
    expected_keys = [expected_start + timedelta(days=i) for i in range(0, nb_days, delta_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_day_intervals_2():
    start = datetime(2016, 1, 1, 6)
    end = datetime(2016, 2, 1, 6)
    delta_days = 2
    nb_days = (end - start).days + delta_days

    expected_start = datetime(2015, 12, 31)

    date_histogram = get_date_histogram(min=start, max=end, interval='2d')
    expected_keys = [expected_start + timedelta(days=i) for i in range(0, nb_days, delta_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_day_intervals_offset():
    start = datetime(2016, 1, 1, 6)
    end = datetime(2016, 2, 1, 6)
    delta_days = 1
    nb_days = (end - start).days + delta_days

    expected_start = start

    date_histogram = get_date_histogram(min=start, max=end, interval='1d', offset='+6h')
    expected_keys = [expected_start + timedelta(days=i) for i in range(0, nb_days, delta_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_day_intervals_negative_offset():
    start = datetime(2015, 1, 1)
    end = datetime(2016, 2, 1)
    delta_days = 1
    nb_days = (end - start).days + delta_days

    expected_start = start - timedelta(hours=6)

    date_histogram = get_date_histogram(min=start, max=end, interval='1d', offset='-6h')
    expected_keys = [expected_start + timedelta(days=i) for i in range(0, nb_days, delta_days)]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_second_intervals():
    start = datetime(2016, 1, 1, 6, microsecond=123)
    end = start + timedelta(days=1)
    delta_seconds = 433
    nb_seconds = int((end - start).total_seconds()) + delta_seconds

    expected_start = datetime(2016, 1, 1, 5, 55, 37)

    date_histogram = get_date_histogram(min=start, max=end, interval='433s')
    expected_keys = [
        expected_start + timedelta(seconds=i)
        for i in range(0, nb_seconds, delta_seconds)
    ]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys


def test_date_histogram_choice_keys_month_intervals_offset():
    start = datetime(2016, 1, 1, 6)
    end = datetime(2016, 6, 1, 6)

    date_histogram = get_date_histogram(min=start, max=end, interval='2M', offset='+6h')
    expected_keys = [
        datetime(2016, 1, 1, 6),
        datetime(2016, 3, 1, 6),
        datetime(2016, 5, 1, 6),
    ]
    keys = date_histogram.choice_keys()
    assert expected_keys == keys
