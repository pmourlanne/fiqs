Flatten tree result
===================

Consuming the results from an Elasticsearch query can be troublesome. fiqs exposes a ``flatten_result`` function that transforms an elasticsearch-dsl ``Result``, or a dictionary, into the list of its nodes. You will lose access to some data (``doc_count_error_upper_bound``, ``sum_other_doc_count``, the ``hits`` etc.) so beware.

Here is a basic example with an aggregation and a metric::

    print(flatten_result({
        "_shards": {
            ...
        },
        "hits": {
            ...
        },
        "aggregations": {
            "shop": {
                "buckets": [
                    {
                        "doc_count": 30,
                        "key": 1,
                        "total_sales": {
                            "value": 12345.0
                        },
                    },
                    {
                        "doc_count": 20,
                        "key": 2,
                        "total_sales": {
                            "value": 23456.0
                        },
                    },
                    {
                        "doc_count": 10,
                        "key": 3,
                        "total_sales": {
                            "value": 34567.0
                        },
                    },
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
            },
        },
    }))
    # [
    #     {
    #         "shop": 1,
    #         "doc_count": 30,
    #         "total_sales": 12345.0,
    #     },
    #     {
    #         "shop": 2,
    #         "doc_count": 20,
    #         "total_sales": 23456.0,
    #     },
    #     {
    #         "shop": 3,
    #         "doc_count": 10,
    #         "total_sales": 34567.0,
    #     },
    # ]

``flatten_result`` can handle multiple aggregations on the same level, and nested aggregations. It can also handled nested fields::

    print(flatten_result({
        ...
        "aggregations": {
            "products": {
                "doc_count": 1540,
                "product_type": {
                    "buckets": [
                        {
                            "avg_product_price": {
                                "value": 179.53889943074003,
                            },
                            "doc_count": 527,
                            "key": "product_type_3",
                        },
                        {
                            "avg_product_price": {
                                "value": 159.18296529968455,
                            },
                            "doc_count": 317,
                            "key": "product_type_2",
                        },
                        {
                            "avg_product_price": {
                                "value": 152.76785714285714,
                            },
                            "doc_count": 280,
                            "key": "product_type_1",
                        },
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                },
            },
        }
    }))
    # [
    #     {
    #         "avg_product_price": 179.53889943074003,
    #         "product_type": "product_type_3",
    #         "doc_count": 527,
    #     },
    #     {
    #         "avg_product_price": 159.18296529968455,
    #         "product_type": "product_type_2",
    #         "doc_count": 317,
    #     },
    #     {
    #         "avg_product_price": 152.76785714285714,
    #         "product_type": "product_type_1",
    #         "doc_count": 280,
    #     },
    # ]
