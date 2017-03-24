Making queries with fiqs
------------------------


The ``FQuery`` object
*********************

fiqs exposes a ``FQuery`` object which lets you write less verbose simple queries against ElasticSearch. It is built on top of the `elasticsearch-dsl Search object <http://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#the-search-object>`_. Here is a quick example of what ``FQuery`` can do, compared to elasticsearch-dsl::

    from elasticsearch_dsl import Search
    from fiqs.aggregations import Sum
    from fiqs.query import FQuery

    from .models import Sale

    # The elasticsearch-dsl way
    search = Search(...)
    search.aggs.bucket(
        'shop_id', 'terms', field='shop_id',
    ).bucket(
        'client_id', 'terms', field='client_id',
    ).metric(
        'total_sales', 'sum', field='price',
    )
    result = search.execute()

    # The FQuery way
    search = Search(...)
    metric = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    result = metric.eval()


Loss of expresiveness
^^^^^^^^^^^^^^^^^^^^^

Let's start with a warning :> FQuery may allow you to write cleaner and more re-usable queries, but at the cost of a loss of expresiveness. For example, you will not be able to have metrics at multiple aggregation levels. You may not be able to use FQuery for all your queries, and that's OK!


``FQuery`` options
^^^^^^^^^^^^^^^^^^

A ``FQuery`` object only needs an elasticsearch-dsl object to get started. You may also configure the following options:

    * ``default_size``: the `size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html#search-request-from-size>`_ used by default in aggregations built by this object.

    * ``fill_missing_buckets``: `True` by default. If `False`, FQuery will not try to fill the missing buckets. For more details see `Filling missing buckets`_.


Values
******

You need to call ``values`` on a FQuery object to specify the metrics you want to use in your request. values accepts both arguments and keyword arguments::

    from fiqs.aggregation import Sum, Avg

    from .models import Sale

    FQuery(search).values(
        Avg(Sale.price),
        total_sales=Sum(Sale.price),
    )

In this case, the nodes will contain two keys for the metrics: *total_sales*, and *sale__price__avg*, a string representation of the *Avg(Sale.price)* metric.
A ``values`` call returns the FQuery object, to allow chaining calls.

fiqs contains several classes, which all take a field as argument, to help you make these metric calls:


Avg
^^^

Used for the Elasticsearch `avg aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html>`_.

Cardinality
^^^^^^^^^^^

Used for the Elasticsearch `cardinality aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html>`_

Count
^^^^^

Used if you only want to count the documents present in your search. This aggregation does not change the Elasticsearh request, since it always returns the number of documents in the doc_count.

Max
^^^

Used for the Elasticsearch `max aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html>`_

Min
^^^

Used for the Elasticsearch `min aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html>`_

Sum
^^^

Used for the Elasticsearch `sum aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html>`_


Operations
^^^^^^^^^^

fiqs lets you query computed fields, created with operations on a model's fields. For example::

    from fiqs.aggregation import Sum

    from .models import TrafficCount

    FQuery(search).values(
        total_traffic=Addition(
            Sum(TrafficCount.in_count),
            Sum(TrafficCount.out_count),
        ),
        in_traffic_ratio=Ratio(
            Sum(TrafficCount.in_count),
            Addition(
                Sum(TrafficCount.in_count),
                Sum(TrafficCount.out_count),
            ),
        ),
    )

The three existing operations are Addition, Subtraction and Ratio. **Do note that these operations cannot be used if the FQuery was initialized with fill_missing_buckets at False.**


Group by
********

You can call ``group_by`` on a FQuery object to add aggregations. Like ``values``, ``group_by`` returns the FQuery object, to allow chaining. fiqs lets you build only one aggregation, which can be as deep as you need it to be. In a group_by call, you can use any fiqs Field, or Field subclass, object. fiqs also offers Field subclasses that help you configure your aggregation:


FieldWithChoices
^^^^^^^^^^^^^^^^

A ``FieldWithChoices`` takes as argument an existing field, and a list of choice::

    FieldWithChoices(Sale.shop_id, choices=(['Atlanta', 'Phoenix', 'NYC']))

This field is useful if you want to tune the capacity of FQuery to fill the missing buckets.

DataExtendedField
^^^^^^^^^^^^^^^^^

A ``DataExtendedField`` takes as argument an existing field, and a data dictionary::

    DataExtendedField(Sale.shop_id, size=5)

This field is useful if you want to to fine tune the aggregation. In the example we changed the ``size`` parameter that will be used in the Elasticsearch aggregation.


Order by
********

You can call ``order_by`` on a FQuery object, to order the Elasticsearch result as you want. ``order_by`` returns the FQuery object, to allow chaining. order_by expects a dictionary that will be directly used in the aggregation as a `sort <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html>`_::


    FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    ).order_by(
        {'total_sales': 'desc'},
    )

In this example, the Elasticsearch result will be ordered by total sales, in descending order.


Executing the query
*******************

Calling ``eval`` on the Fquery object will execute the Elasticsearch query and return the result.


Form of the result
^^^^^^^^^^^^^^^^^^

FQuery will automatically flatten the result returned by Elasticsearch, as detailed :doc:`here <tree>`. It will also cast the value, depending on your model's fields.

Each field may implement a ``get_casted_value`` method. FQuery will use this method to cast values returned by Elasticsearch. For example::

    class IntegerField(Field):
        def __init__(self, **kwargs):
            super(IntegerField, self).__init__('integer', **kwargs)

        def get_casted_value(self, v):
            return int(v) if v else v

As of today, only the following fields implement this method:

* IntegerField, ByteField and fields inheriting from them cast values as int
* FloatField cast values as float
* DateField cast values as datetime, **ignoring the milliseconds**

Filling missing buckets
^^^^^^^^^^^^^^^^^^^^^^^

By default, FQuery will try to add buckets missing from the Elasticsearch result. FQuery uses several heuristics to determine which buckets are missing, as we will see below. FQuery will fill the group_by values with the missing keys, and the metric values with ``None``.

* If a field in the group_by defines the ``choices`` attribute, FQuery will expect all the choices' keys to be present as keys in the Elasticsearch buckets::

    # Our model
    class Sale(Model):
        shop_id = fields.IntegerField(choices=(1, 2, 3, ))
        price = fields.IntegerField()

    # Our query
    results = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    ).eval()

    # Elasticsearch result, notice there is no bucket with shop_id 1
    # {
    #     [...],
    #     "aggregations": {
    #         "shop": {
    #             "buckets": [
    #                 {
    #                     "doc_count": 20,
    #                     "key": 2,
    #                     "total_sales": {
    #                         "value": 123,
    #                     },
    #                 },
    #                 {
    #                     "doc_count": 10,
    #                     "key": 3,
    #                     "total_sales": {
    #                         "value": 456,
    #                     },
    #                 },
    #             ],
    #             [...],
    #         },
    #     },
    # }

    # FQuery result, with the empty line added
    # [
    #     {
    #         'shop_id': 2,
    #         'doc_count': 20,
    #         'total_sales': 123,
    #     },
    #     {
    #         'shop_id': 3,
    #         'doc_count': 10,
    #         'total_sales': 456,
    #     },
    #     {
    #         'shop_id': 1,
    #         'doc_count': 0,
    #         'total_sales': None,
    #     },
    # ]


* If an aggregate in the group_by returns a value when calling ``choice_keys``, FQuery will expect all the keys to be present in the Elasticsearch buckets. Only available with daily DateHistogram for the time being.

* Finally, FQuery will look at all the values each key takes in the result buckets, and will expect all keys to be present in all buckets::

    # Our model
    class Sale(Model):
        shop_id = fields.IntegerField()
        price = fields.IntegerField()
        payment_type = fields.KeywordField(choices=('wire_transfer', 'cash', ))

    # Our query
    results = FQuery(search).values(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.payment_type,
        Sale.shop_id,
    ).eval()

    # Elasticsearch result
    # {
    #     [...],
    #     "aggregations": {
    #         "payment_type": {
    #             "buckets": [
    #                 {
    #                     "key": "wire_transfer",
    #                     "shop_id": {
    #                         "buckets": [
    #                             {
    #                                 doc_count: 10,
    #                                 "key": 1,
    #                                 "total_sales": {
    #                                     "value": 123,
    #                                 },
    #                             },
    #                         ],
    #                     },
    #                 },
    #                 {
    #                     "key": "cash",
    #                     "shop_id": {
    #                         "buckets": [
    #                             {
    #                                 doc_count: 20,
    #                                 "key": 2,
    #                                 "total_sales": {
    #                                     "value": 456,
    #                                 },
    #                             },
    #                         ],
    #                     },
    #                 },
    #             ],
    #         },
    #     },
    # }

    # FQuery result, with two empty lines added
    # [
    #     {
    #         'shop_id': 1,
    #         'doc_count': 10,
    #         'total_sales': 123,
    #         'payment_type': 'wire_transfer',
    #     },
    #     {
    #         'shop_id': 2,
    #         'doc_count': 0,
    #         'total_sales': None,
    #         'payment_type': 'wire_transfer',
    #     },
    #     {
    #         'shop_id': 2,
    #         'doc_count': 20,
    #         'total_sales': 456,
    #         'payment_type': 'cash',
    #     },
    #     {
    #         'shop_id': 1,
    #         'doc_count': 0,
    #         'total_sales': None,
    #         'payment_type': 'cash',
    #     },
    # ]
