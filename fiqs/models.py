# -*- coding: utf-8 -*-

from six import with_metaclass

from elasticsearch_dsl import Mapping, Nested

from fiqs.exceptions import FieldError
from fiqs.fields import Field, NestedField


class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        klass = super(ModelMetaClass, cls).__new__(cls, name, bases, attrs)

        # We initialize the current class fields
        fields = []
        field_keys = []
        for field_key, field in attrs.items():
            if isinstance(field, Field):
                # We set the field's key
                field._set_key(field_key)
                # The field should be able to access the model
                field.model = klass

                fields.append(field)
                field_keys.append(field_key)

        # We do the same for the fields from the parent
        parents = [b for b in bases if isinstance(b, ModelMetaClass)]
        for p in parents:
            for field in p._fields:
                if field.key in field_keys:
                    msg = 'Field {} from class {} clashes with field ' +\
                          'with the same name in base class {}'
                    msg = msg.format(field.key, klass, p)
                    raise FieldError(msg)

                copy_field = field.get_copy()
                copy_field.model = klass

                setattr(klass, field.key, copy_field)

                fields.append(copy_field)
                field_keys.append(copy_field.key)

        klass._fields = fields

        return klass


class Model(with_metaclass(ModelMetaClass, object)):
    index = None
    doc_type = None

    @classmethod
    def get_index(cls, *args, **kwargs):
        if not cls.index:
            raise NotImplementedError('Model class should define an index')

        return cls.index

    @classmethod
    def get_doc_type(cls, *args, **kwargs):
        if not cls.doc_type:
            raise NotImplementedError('Model class should define a doc_type')

        return cls.doc_type

    @classmethod
    def get_mapping(cls):
        m = Mapping(cls.get_doc_type())
        m.meta('dynamic', 'strict')

        nested_mappings = {}
        fields_to_nest = []

        for field in cls._fields:
            if isinstance(field, NestedField):
                nested_mappings[field.key] = (field, Nested())

            if field.parent:
                fields_to_nest.append(field)
            else:
                m.field(field.storage_field, field.type)

        for field in fields_to_nest:
            if field.parent not in nested_mappings:
                raise Exception(
                    'Nested field {} needs to be defined in {}'.format(
                        field.parent, str(cls),
                    )
                )

            _, nested_mapping = nested_mappings[field.parent]
            nested_mapping.field(field.storage_field, field.type)

        for field, nested_mapping in nested_mappings.values():
            if not field.parent:
                m.field(field.key, nested_mapping)
            else:
                _, parent_mapping = nested_mappings[field.parent]
                parent_mapping.field(field.key, nested_mapping)

        return m
