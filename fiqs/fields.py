# -*- coding: utf-8 -*-

class Field(object):
    def __init__(self, type, key=None, verbose_name=None, storage_field=None,
                 unit=None, choices=None, data=None, parent=None):
        verbose_name = verbose_name or key
        storage_field = storage_field or key
        choices = choices or ()
        data = data or {}

        self.key = key
        self.type = type
        self.verbose_name = verbose_name
        self.storage_field = storage_field
        self.unit = unit
        self.choices = choices
        self.data = data
        self.parent = parent

    def _set_key(self, key):
        self.key = key
        if not self.verbose_name:
            self.verbose_name = key
        if not self.storage_field:
            self.storage_field = key


class TextField(Field):
    def __init__(self, **kwargs):
        super(TextField, self).__init__('text', **kwargs)


class KeywordField(Field):
    def __init__(self, **kwargs):
        super(KeywordField, self).__init__('keyword', **kwargs)


class DateField(Field):
    def __init__(self, **kwargs):
        super(DateField, self).__init__('date', **kwargs)


class IntegerField(Field):
    def __init__(self, **kwargs):
        super(IntegerField, self).__init__('integer', **kwargs)


class BooleanField(Field):
    def __init__(self, **kwargs):
        super(BooleanField, self).__init__('boolean', **kwargs)


class NestedField(Field):
    def __init__(self, **kwargs):
        super(NestedField, self).__init__('nested', **kwargs)
