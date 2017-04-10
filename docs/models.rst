Models
------


fiqs lets you create Model classes, a la Django, which automatically generate an elasticsearch mapping, and allows you to write cleaner queries.

A model is a class inheriting from ``fiqs.models.Model``. It needs to define a doc_type, an index and its fields::

    from fiqs import fields, models

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

A list of possible values for the field. fiqs will use it to fill the missing buckets. It can also contains a list of tuples, where the first element is the key, and the second is a 'pretty key'::

    payment_type = fields.KeywordField(choices=[
        ('wire_transfer', _('Wire transfer')),
        ('cash', _('Cash')),
        ('store_credit', _('Store credit')),
    ])

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

LongField
"""""""""

A field with the ``long`` Elasticsearch data type.

IntegerField
""""""""""""

A field with the ``integer`` Elasticsearch data type.

ShortField
""""""""""

A field with the ``short`` Elasticsearch data type.

ByteField
"""""""""

A field with the ``byte`` Elasticsearch data type.

DoubleField
"""""""""""

A field with the ``double`` Elasticsearch data type.

FloatField
""""""""""

A field with the ``float`` Elasticsearch data type.

DayOfWeekField
""""""""""""""

A field inheriting from ByteField. It accepts ``iso`` as a keyword argument. Depending on the value of ``iso``, this field will have data and choices matching weekdays or isoweekdays.

HourOfDayField
""""""""""""""

A field inheriting from ByteField. By default, it will be able to contain values betweek 0 and 23.

BooleanField
""""""""""""

A field with the ``boolean`` Elasticsearch data type.

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
