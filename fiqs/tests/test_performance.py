# -*- coding: utf-8 -*-

from datetime import datetime

import pytest

from fiqs.aggregations import Count, DateHistogram
from fiqs.tests.conftest import write_fquery_output, load_output
from fiqs.query import FQuery
from fiqs.testing.models import Sale
from fiqs.testing.utils import get_search


start = datetime(2016, 1, 1)
end = datetime(2016, 1, 31)


@pytest.mark.docker
@pytest.mark.performance
def test_generate_performance_output(elasticsearch_sale):
    write_fquery_output(
        FQuery(get_search()).values(
            Count(Sale),
        ).group_by(
            DateHistogram(
                Sale.timestamp,
                interval='15m',
                min=start,
                max=end,
            )
        ),
        'nb_sales_by_15_minutes_performance',
    )


@pytest.mark.performance
def test_flatten_performance():
    fquery = FQuery(get_search()).values(
        Count(Sale),
    ).group_by(
        DateHistogram(
            Sale.timestamp,
            interval='15m',
            min=start,
            max=end,
        ),
    )

    result = load_output('nb_sales_by_15_minutes_performance')
    fquery._flatten_result(
        result,
        remove_nested_aggregations=fquery._contains_nested_expressions(),
    )
