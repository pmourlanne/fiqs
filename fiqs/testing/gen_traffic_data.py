# -*- coding: utf-8 -*-

import json
import random

from fiqs.testing.gen_data import random_timestamp, random_shop_id


def gen_traffic_data(size):
    for i in xrange(size):
        print json.dumps({
            'id': i + 1,
            'shop_id': random_shop_id(),
            'timestamp': random_timestamp(),
            'duration': random.choice([600, 900]),
            'incoming_traffic': random.randint(0, 200),
            'outgoing_traffic': random.randint(0, 200),
        })


if __name__ == '__main__':
    size = 500
    gen_traffic_data(size)
