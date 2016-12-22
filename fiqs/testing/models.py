# -*- coding: utf-8 -*-

from fiqs import fields
from fiqs.models import Model


PAYMENT_TYPES = [
    'wire_transfer',
    'cash',
    'store_credit',
]


class Sale(Model):
    doc_type = 'sale'

    id = fields.IntegerField()
    shop_id = fields.IntegerField()
    client_id = fields.KeywordField()

    timestamp = fields.DateField()
    price = fields.IntegerField()
    payment_type = fields.KeywordField(choices=PAYMENT_TYPES)

    products = fields.NestedField()
    product_id = fields.KeywordField(parent='products')
    product_type = fields.KeywordField(parent='products')
    product_price = fields.IntegerField(parent='products')

    parts = fields.NestedField(parent='products')
    part_id = fields.KeywordField(parent='parts')
    warehouse_id = fields.KeywordField(parent='parts')
    part_price = fields.IntegerField(parent='parts')
