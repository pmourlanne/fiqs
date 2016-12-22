# -*- coding: utf-8 -*-


from fiqs.testing.models import Sale


def test_mapping_from_model():
    expected = {
        'sale': {
            'dynamic': 'strict',
            'properties': {
                'id': {'type': 'integer'},
                'shop_id': {'type': 'integer'},
                'client_id': {'type': 'keyword'},
                'timestamp': {'type': 'date'},
                'price': {'type': 'integer'},
                'payment_type': {'type': 'keyword'},
                'products': {
                    'properties': {
                        'product_id': {'type': 'keyword'},
                        'product_price': {'type': 'integer'},
                        'product_type': {'type': 'keyword'},
                        'parts': {
                            'properties': {
                                'part_id': {'type': 'keyword'},
                                'warehouse_id': {'type': 'keyword'},
                                'part_price': {'type': 'integer'},
                            },
                            'type': 'nested',
                        },
                    },
                    'type': 'nested',
                },
            },
        },
    }

    assert Sale.get_mapping().to_dict() == expected
