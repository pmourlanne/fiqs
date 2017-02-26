# -*- coding: utf-8 -*-

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


def random_string(length=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits + ' ') for _ in range(length))


def random_shop_id():
    return random.randint(1, 10)
