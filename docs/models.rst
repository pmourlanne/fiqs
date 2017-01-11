Models
------


fiqs lets you create Model classes, a la Django, which automatically generate an elasticsearch mapping, and allows you to write cleaner queries.

A model is a class inheriting from ``fiqs.models.Model``. It needs to define a doc_type, an index and its fields::

    from fiqs import models

    class Sale(models.Model):
        index = 'sale_data'
        doc_type = 'sale'

        id = fields.IntegerField()
        shop_id = fields.IntegerField()
        client_id = fields.KeywordField()

        timestamp = fields.DateField()
        price = fields.IntegerField()
        payment_type = fields.KeywordField(choices=['wire_transfer', 'cash', 'store_credit'])


The ``doc_type`` will be used for the mapping, the ``index`` for the queries. Instead of defining these values as class attributes, you can override the class methods ``get_index`` and ``get_doc_type``::

    @classmethod
    def get_index(cls, *args, **kwargs):
        if not cls.index:
            raise NotImplementedError('Model class should define an index')

        return cls.index

    @classmethod
    def get_doc_type(cls, *args, **kwargs):
        if not cls.doc_type:
            raise NotImplementedError('Model class should define a doc_type')

        return cls.doc_type


Model fields
************

This section contains all the API references for fields, including the field options and the field types.


Field options
^^^^^^^^^^^^^

The following arguments are available to all field types. All are optional, except ``type``.

type
""""

This is a string that tells fiqs which `field datatype <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html>`_ will be used in Elasticsearch. This option is **mandatory**.


choices
"""""""

A list of possible values for the field. fiqs will use it to fill the missing buckets.

data
""""

A dictionary containing data used in the aggregations. For the time being, only ``size`` is used.

parent
""""""

Used for nested documents to define the name of the parent document. For example::

    from fiqs import models

    class Sale(models.Model):
        ...

        products = fields.NestedField()
        product_id = fields.KeywordField(parent='products')
        ...
        parts = fields.NestedField(parent='products')
        part_id = fields.KeywordField(parent='paths')


storage_field
"""""""""""""

The name of the field in your Elasticsearch cluster. By default fiqs will use the field's name.
In the case of nested fields, fiqs will use the ``storage_field`` as the path.

unit
""""

Not yet used.


verbose_name
""""""""""""

A human-readable name for the field. If the verbose name isn’t given, fiqs will use the field's name in the model. Not yet used.


Field types
^^^^^^^^^^^

TextField
"""""""""

A field with the ``text`` Elasticsearch data type.

KeywordField
""""""""""""

A field with the ``keyword`` Elasticsearch data type.

DateField
"""""""""

A field with the ``date`` Elasticsearch data type.

IntegerField
""""""""""""

A field with the ``integer`` Elasticsearch data type.

BooleanField
""""""""""""

A field with the ``boolean`` Elasticsearch data type.

IntegerField
""""""""""""

A field with the ``integer`` Elasticsearch data type.

NestedField
"""""""""""

A field with the ``nested`` Elasticsearch data type.


Mapping
*******

Model classes expose a ``get_mapping`` class method, that returns a strict and dynamic `elasticsearch-dsl Mapping object <http://elasticsearch-dsl.readthedocs.io/en/latest/persistence.html#mappings>`_. You can use it to create or update the mapping in your Elasticsearch cluster::

    from elasticsearch import Elasticsearch

    client = Elasticsearch(['http://my.cluster.com'])
    mapping = MyModel.get_mapping()
    client.indices.create(index='my_index', body={'mappings': mapping.to_dict()})


Queries
*******

The ``FQuery`` object
^^^^^^^^^^^^^^^^^^^^^

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
    metric = FQuery(search).metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
        Sale.client_id,
    )
    result = metric.eval()


``FQuery`` options
""""""""""""""""""

A ``FQuery`` only needs an elasticsearch-dsl object to get started. You may also configure the following options:

    * ``default_size``: the `size <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html#search-request-from-size>`_ used by default in aggregations built by this object.

    * ``fill_missing_buckets``: soon :>


Metric
^^^^^^

You need to call ``metric`` on a FQuery object to specify the metric you want to use in your request. metric accepts both arguments and keyword arguments::

    from fiqs.aggregation import Sum, Avg

    from .models import Sale

    FQuery(search).metric(
        Avg(Sale.price),
        total_sales=Sum(Sale.price),
    )

In this case, the nodes will contain two keys for the metrics: *total_sales*, and *sale__price__avg*, a string representation of the *Avg(Sale.price)* metric.

fiqs contains several classes, which all take a field as argument, to help you make these metric calls:


Avg
"""

Used for the Elasticsearch `avg aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html>`_.

Cardinality
"""""""""""

Used for the Elasticsearch `cardinality aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html>`_

Count
"""""

Used if you only want to count the documents present in your search. This aggregation does not change the Elasticsearh request, since it always returns the number of documents in the doc_count.

Max
"""

Used for the Elasticsearch `max aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html>`_

Min
"""

Used for the Elasticsearch `min aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html>`_

Sum
"""

Used for the Elasticsearch `sum aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html>`_


Operations
""""""""""

Soon :>

Group by
^^^^^^^^

Calling ``metric`` on a FQuery object returns a ``QueryMetric`` object, on which you can call ``group_by`` to add aggregations. fiqs lets you build only one aggregation, which can be as deep as you need it to be. In a group_by call, you can use any fiqs Field, or Field subclass, object. fiqs also offers Field subclasses that help you configure your aggregation:


FieldWithChoices
""""""""""""""""

A ``FieldWithChoices`` takes as argument an existing field, and a list of choice::

    FieldWithChoices(Sale.shop_id, choices=(['Atlanta', 'Phoenix', 'NYC']))

This field is useful if you want to tune the capacity of FQuery to fill the missing buckets.

DataExtendedField
"""""""""""""""""

A ``DataExtendedField`` takes as argument an existing field, and a data dictionary::

    DataExtendedField(Sale.shop_id, size=5)

This field is useful if you want to to fine tune the aggregation. In the example we changed the ``size`` parameter that will be used in the Elasticsearch aggregation.


Order by
^^^^^^^^

You can call ``order_by`` on a QueryMetric to order the Elasticsearch result as you want. order_by expects a dictionary that will be directly used in the aggregation as a `sort <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html>`_::

    FQuery(search).metric(
        total_sales=Sum(Sale.price),
    ).group_by(
        Sale.shop_id,
    ).order_by(
        {'total_sales': 'desc'},
    )

In this example, the Elasticsearch result will be ordered by total sales, in descending order.


Filling missing buckets
^^^^^^^^^^^^^^^^^^^^^^^

Soon :>

Loss of expresiveness
^^^^^^^^^^^^^^^^^^^^^

FQuery may allow you to write cleaner and more re-usable queries, but at the cost of a loss of expresiveness. For example, you will not be able to have metrics at multiple aggregation levels. 

