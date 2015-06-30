"""
A manager for providing a common interface
between cassandra and ripozo
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ripozo.exceptions import NotFoundException
from ripozo.manager_base import BaseManager
from ripozo.utilities import make_json_safe
from ripozo import fields

from cassandra.cqlengine.query import DoesNotExist, Token

import logging
import six

_LOGGER = logging.getLogger(__name__)

_COLUMN_FIELD_MAP = {
    'ascii': fields.StringField,
    'inet': fields.StringField,
    'text': fields.StringField,
    'varchar': fields.StringField,
    'timeuuid': fields.StringField,
    'uuid': fields.StringField,
    'bigint': fields.IntegerField,
    'counter': fields.IntegerField,
    'int': fields.IntegerField,
    'varint': fields.IntegerField,
    'boolean': fields.BooleanField,
    'double': fields.FloatField,
    'float': fields.FloatField,
    'decimal': fields.FloatField,
    'map': fields.DictField,
    'list': fields.ListField,
    'set': fields.ListField
}


class CQLManager(BaseManager):
    """
    Works with serializing the models as json and deserializing them to cqlengine models

    :param cassandra.cqlengine.models.Model model:
    """
    fail_create_if_exists = True
    allow_filtering = False

    @classmethod
    def get_field_type(cls, name):
        col = cls.model._columns[name]
        db_type = col.db_type
        if db_type in _COLUMN_FIELD_MAP:
            field_class = _COLUMN_FIELD_MAP[db_type]
            return field_class(col.db_type)
        return fields.BaseField(col.db_field_name)

    @property
    def queryset(self):
        return self.model.objects.all()

    def create(self, values, *args, **kwargs):
        """
        Creates an object using the specified values in the dict

        :param values: A dictionary with the attribute names as keys
            and the attribute values as values
        :type values: dict
        :return: Cassandra model object
        :rtype: cqlengine.Model
        """
        _LOGGER.info('Creating model of type %s', self.model.__name__)
        values = self.valid_fields(values, self.create_fields)
        if self.fail_create_if_exists:
            obj = self.model.if_not_exists().create(**values)
        else:
            obj = self.model.create(**values)
        return self.serialize_model(obj)

    def retrieve(self, lookup_keys, *args, **kwargs):
        """
        Retrieves an existing object using the lookupkeys

        :param lookup_keys: A dictionary with the attribute names
            as keys and the attribute values as values
        :type lookup_keys: dict
        :return: The specified model using the lookup keys
        :rtype: dict
        """
        _LOGGER.info('Retrieving model of type %s', self.model.__name__)
        obj = self._get_model(lookup_keys)
        return self.serialize_model(obj)

    def retrieve_list(self, filters, *args, **kwargs):
        """
        Retrieves a list of all models that match the specified filters

        :param filters: The named parameters to filter the models on
        :type filters: dict
        :return: tuple 0 index = a list of the models as dictionary objects
            1 index = the query args for retrieving the next in pagination
        :rtype: list
        """
        logger = logging.getLogger(__name__)
        logger.info('Retrieving list of models of type %s with '
                    'filters: %s', str(self.model), filters)
        obj_list = []
        models = self.queryset
        if self.allow_filtering:
            logger.debug('Allowing filtering on list retrieval')
            models = models.allow_filtering()

        pagination_count, filters = self.get_pagination_count(filters)
        last_pagination_pk, filters = self.get_pagination_pks(filters)
        if not last_pagination_pk:
            last_pagination_pk = []

        if filters is not None:
            for key, value in six.iteritems(filters):
                models = models.filter(getattr(self.model, key) == value)
        if self.order_by is not None:
            models = models.order_by(self.order_by)

        models = self.pagination_filtration(models,
                                            last_pagination_pk=last_pagination_pk,
                                            filters=filters)
        models = models.limit(pagination_count + 1)

        last_model = None
        # Handle the extra model used for finding the next batch
        if len(models) > pagination_count:
            last_model = models[-1]
            models = models[:pagination_count]

        for obj in models:
            obj_list.append(self.serialize_model(obj))
        if not pagination_count or not last_model:
            return obj_list, {self.pagination_pk_query_arg: None,
                              self.pagination_count_query_arg: pagination_count,
                              self.pagination_next: None}
        else:
            query_args, pagination_keys = self.get_next_query_args(last_model,
                                                                   pagination_count,
                                                                   filters=filters)
            return obj_list, {self.pagination_pk_query_arg: pagination_keys,
                              self.pagination_count_query_arg: pagination_count,
                              self.pagination_next: query_args}

    def update(self, lookup_keys, updates, *args, **kwargs):
        """
        Updates the model specified by the lookup_key with the specified updates

        :param lookup_keys:
        :type lookup_keys: dict
        :param updates:
        :type updates: dict
        :return:
        :rtype: cqlengine.Model
        """
        _LOGGER.info('Updating model of type %s', self.model.__name__)
        obj = self._get_model(lookup_keys)
        updates = self.valid_fields(updates, self.update_fields)
        for key, value in six.iteritems(updates):
            setattr(obj, key, value)
        obj.save()
        return self.serialize_model(obj)

    def delete(self, lookup_keys, *args, **kwargs):
        """
        Deletes the model specified by the lookup_keys

        :param lookup_keys: A dictionary of fields and values on model to filter by
        :type lookup_keys: dict
        """
        _LOGGER.info('Deleting model of type %s', self.model.__name__)
        obj = self._get_model(lookup_keys)
        obj.delete()
        return {}

    def _get_model(self, lookup_keys):
        """
        Gets the model specified by the lookupkeys

        :param lookup_keys: A dictionary of fields and values on the model to filter by
        :type lookup_keys: dict
        """
        queryset = self.queryset
        for key, value in six.iteritems(lookup_keys):
            queryset = queryset.filter(getattr(self.model, key) == value)
        try:
            obj = queryset.get()
            return obj
        except DoesNotExist:
            raise NotFoundException('The model {0} could not be found.  '
                                    'lookup_keys: {1}'.format(self.model.__name__, lookup_keys))

    def get_next_query_args(self, last_model, pagination_count, filters=None):
        filters = filters or {}
        if last_model is None:
            return None, None
        query_args = '{0}={1}'.format(self.pagination_count_query_arg, pagination_count)

        for filter_name, filter_value in six.iteritems(filters):
            query_args = '{0}&{1}={2}'.format(query_args, filter_name, filter_value)
        pagination_keys = []
        for p_name in last_model._primary_keys:
            value = getattr(last_model, p_name)
            query_args = '{0}&{1}={2}'.format(query_args, self.pagination_pk_query_arg, value)
            pagination_keys.append(value)
        return query_args, pagination_keys

    def pagination_filtration(self, queryset, last_pagination_pk=None, filters=None):
        if filters is None:
            return queryset
        if last_pagination_pk is None:
            last_pagination_pk = []
        if len(last_pagination_pk) == 0:
            return queryset
        partition_key_count = len(self.model._partition_keys)
        if len(dict(filters.items() + self.model._partition_keys.items())) < len(filters) + len(self.model._partition_keys):
            # There is some overlap between the partition keys filters
            # TODO make a better way to do filtering
            for i in range(len(self.model._partition_keys)):
                key = self.model._partition_keys.items()[i][0]
                if key in filters:
                    continue
                value = last_pagination_pk[i]
                queryset = queryset.filter(**{'{0}__gte'.format(key): value})
        else:
            queryset = queryset.filter(pk__token__gte=Token(last_pagination_pk))
        if len(self.model._primary_keys) <= partition_key_count:
            return queryset

        clustering_pagination = last_pagination_pk[partition_key_count:]
        for i in range(len(clustering_pagination)):
            key = self.model._clustering_keys.items()[i][0]
            if key in filters:
                continue
            value = clustering_pagination[i]
            queryset = queryset.filter(getattr(self.model, key) >= value)
        return queryset

    def serialize_model(self, obj, fields_list=None):
        """
        Takes a cqlengine.Model and jsonifies it.
        This got much easier recently.  It also,
        makes the dictionary safe to immediately call
        json.dumps on it.

        :param obj: The model instance to jsonify
        :type obj: cqlengine.Model
        :return: python dictionary with field names and values
        :rtype: dict
        """
        fields_list = fields_list or self.fields
        base = dict(obj)
        base = self.valid_fields(base, fields_list)
        return make_json_safe(base)
