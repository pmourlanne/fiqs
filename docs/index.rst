fiqs
====

fiqs helps you make queries against Elasticsearch, and more easily consume the results. It is built on top of the official `Python Elasticsearch client <https://elasticsearch-py.readthedocs.io>`_ and the great `Elasticsearch DSL <http://elasticsearch-dsl.readthedocs.io>`_ library.

You can still dive closer to the Elasticsearch JSON DSL by accessing the Elasticsearch DSL client or even the Elasticsearch python client.

fiqs can help you in the following ways:

* A helper function can flatten the result dictionary returned by Elasticsearch
* A model class, a la Django:

    * Automatically generate a mapping
    * Less verbose aggregations and metrics
    * Less verbose filtering
    * Automatically add missing buckets


Compatibility
-------------

fiqs is compatible with Elasticsearch 5.X


Contents
--------

.. toctree::
   :maxdepth: 2

   tree
