# -*- coding: utf-8 -*-

import json
import random

from fiqs.testing.gen_data import random_string, random_timestamp, random_shop_id


PRODUCT_TYPES = ['product_type_{}'.format(i) for i in xrange(5)]
PRODUCT_IDS = ['product_{}'.format(random_string(10)) for _ in xrange(50)]
PRODUCTS = [
    (product_id, random.choice(PRODUCT_TYPES))
    for product_id in PRODUCT_IDS
]


def gen_parts(product_price):
    parts = []
    nb_parts = random.randint(1, 10)

    for i in xrange(nb_parts):
        parts.append({
            'part_id': 'part_{}'.format(random.randint(1, 10)),
            'warehouse_id': 'warehouse_{}'.format(random.randint(1, 10)),
            'part_price': int(product_price / (nb_parts * 1.5)),
        })

    return parts


def gen_products(price):
    products = []
    nb_products = random.randint(1, 5)

    for i in xrange(nb_products):
        if i == nb_products - 1:
            product_price = price - sum(p['product_price'] for p in products)
        else:
            product_price = price / 10

        product_id, product_type = random.choice(PRODUCTS)

        products.append({
            'product_id': product_id,
            'product_type': product_type,
            'product_price': product_price,
            'parts': gen_parts(product_price),
        })

    return products


def gen_shop_data(size):
    for i in xrange(size):
        price = random.randint(10, 1000)
        print json.dumps({
            'id': i + 1,
            'shop_id': random_shop_id(),
            'client_id': 'client_{}'.format(random_string(10)),
            'timestamp': random_timestamp(),
            'price': price,
            'payment_type': random.choice(['wire_transfer', 'cash', 'store_credit', ]),
            'products': gen_products(price),
        })


if __name__ == '__main__':
    size = 500
    gen_shop_data(size)
