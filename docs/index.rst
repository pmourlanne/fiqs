fiqs
====

fiqs helps you make queries against Elasticsearch, and more easily consume the results. It is built on top of the official `Python Elasticsearch client <https://elasticsearch-py.readthedocs.io>`_ and the great `Elasticsearch DSL <http://elasticsearch-dsl.readthedocs.io>`_ library.

You can still dive closer to the Elasticsearch JSON DSL by accessing the Elasticsearch DSL client or even the Elasticsearch python client.

fiqs can help you in the following ways:

* A helper function can flatten the result dictionary returned by Elasticsearch
* A model class, a la Django:

    * Automatically generate a mapping
    * Less verbose aggregations and metrics
    * Less verbose filtering (soon)
    * Automatically add missing buckets (soon)


Compatibility
-------------

fiqs is compatible with Elasticsearch 6.X and works with Python3


Contributing
------------

The fiqs project is hosted on `GitHub <https://github.com/pmourlanne/fiqs>`_

To run the tests on your machine use this command: ``python setup.py test`` Some tests are used to generate results output from Elasticsearch. To run them you will need to run a docker container on your machine: ``docker run -d -p 8200:9200 -p 8300:9300 elasticsearch:6.8.6`` and then run ``pytest -k docker``.


Contents
--------

.. toctree::
   :maxdepth: 2

   tree
   models
   queries_fquery
