# -*- coding: utf-8 -*-

import json
import random
import string
import time
from datetime import datetime

from faker import Factory


fake = Factory.create()


def random_timestamp():
    start = datetime(2016, 1, 1)
    end = datetime(2016, 1, 31)

    return int(1000 * time.mktime(fake.date_time_between_dates(start, end).timetuple()))


def random_shop_id():
    return random.randint(1, 10)


def random_string(self, length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits + ' ') for _ in range(length))


def gen_shop_data(size):
    for i in xrange(size):
        print json.dumps({
            'id': i + 1,
            'shop_id': random_shop_id(),
            'client_id': 'client_{}'.format(random_string(10)),
            'timestamp': random_timestamp(),
            'price': random.randint(10, 1000),
            'payment_type': random.choice(['cc', 'cash', 'store_credit', ]),
        })


if __name__ == '__main__':
    size = 500
    gen_shop_data(size)
