# -*- coding: utf-8 -*-

from elasticsearch_dsl import Mapping, Nested
from six import with_metaclass

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
                    msg = (
                        'Field {} from class {} clashes with field '
                        'with the same name in base class {}')
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

        nested_properties = {}
        fields_to_nest = []

        # First pass: treat "standard" fields and gather info for nested fields
        for field in cls._fields:
            if isinstance(field, NestedField):
                nested_properties[field.key] = (field, {})

            if field.parent:
                # Fields to nest, we'll deal with it later
                fields_to_nest.append(field)
            else:
                # "Standard" field, we're good to go
                m.field(field.storage_field, field.type)

        # We deal with nested fields now
        for field in fields_to_nest:
            # Sanity check
            if field.parent not in nested_properties:
                raise Exception(
                    'Nested field {} needs to be defined in {}'.format(
                        field.parent, str(cls)))

            _, properties = nested_properties[field.parent]
            properties[field.storage_field] = field.type

        # A first pass for deeply nested fields
        # FIXME: Not confident this works for all nested configurations :o
        for field, properties in nested_properties.values():
            if not field.parent:
                # First level nested field, ignore for now
                continue

            _, parent_properties = nested_properties[field.parent]
            parent_properties[field.key] = Nested(properties=properties)

        # Final pass for first level nested fields
        for field, properties in nested_properties.values():
            if field.parent:
                # Already dealt with deeply nested fields
                continue

            m.field(field.key, Nested(properties=properties))

        return m
