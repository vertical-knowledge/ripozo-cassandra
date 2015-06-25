from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import create_keyspace, sync_table, delete_keyspace
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import DoesNotExist

from ripozo.exceptions import NotFoundException
from ripozo import fields

from ripozo_cassandra import CQLManager

import unittest2
import uuid


class TestBasicCassandra(unittest2.TestCase):
    keyspace = 'testkeyspace'

    def setUp(self):
        class MyModel(Model):
            id = columns.Text(max_length=63, primary_key=True, default=lambda: str(uuid.uuid4()))
            value = columns.Text(max_length=63)

        self.model = MyModel
        connection.setup(['127.0.0.1'], self.keyspace)
        connection.execute('DROP KEYSPACE {0}'.format(self.keyspace))
        create_keyspace(self.keyspace, strategy_class='SimpleStrategy', replication_factor=1)
        sync_table(MyModel)

        class MyManager(CQLManager):
            model = self.model
            fields = ('id', 'value',)
            create_fields = ('value',)
            update_fields = ('value',)
        self.manager_class = MyManager

    def tearDown(self):
        connection

    def test_create(self):
        """
        Tests a basic create
        """
        manager = self.manager_class()
        values = dict(value='blah')
        resp = manager.create(values)
        self.assertIn('id', resp)
        self.assertEqual(resp['value'], 'blah')
        model = self.model.objects.all().filter(id=resp['id']).get()
        self.assertEqual(model.value, 'blah')

    def test_create_extra_fields(self):
        """
        Tests a create with extra fields that should not
        be present.
        """
        manager = self.manager_class()
        values = dict(value='blah', fake='fake')
        resp = manager.create(values)
        self.assertIn('id', resp)
        self.assertNotIn('fake', resp)
        self.assertEqual(resp['value'], 'blah')
        model = self.model.objects.all().filter(id=resp['id']).get()
        self.assertEqual(model.value, 'blah')

    def test_retrieve(self):
        """
        Tests a simple retrieve
        """
        manager = self.manager_class()
        m = self.model.create(value='blah')
        resp = manager.retrieve(dict(id=m.id))
        self.assertDictEqual(dict(value='blah', id=m.id), resp)

    def test_update(self):
        """
        Tests that an update works appropriately.
        """
        manager = self.manager_class()
        m = self.model.create(value='blah')
        resp = manager.update(dict(id=m.id), dict(value='duh'))
        self.assertDictEqual(dict(value='duh', id=m.id), resp)
        m = self.model.filter(id=m.id).get()
        self.assertEqual(dict(value=m.value, id=m.id), resp)

    def test_missing_update_fields(self):
        """
        Tests that extra update fields don't get applied
        """
        manager = self.manager_class()
        m = self.model.create(value='blah')
        resp = manager.update(dict(id=m.id), dict(value='duh', id='blah'))
        self.assertDictEqual(dict(value='duh', id=m.id), resp)
        m = self.model.filter(id=m.id).get()
        self.assertDictEqual(dict(value=m.value, id=m.id), resp)
        self.assertRaises(DoesNotExist, self.model.filter(id='blah').get)

    def test_delete(self):
        """
        Tests that deleting a model works appropriately.
        """
        manager = self.manager_class()
        m = self.model.create(value='blah')
        resp = manager.delete(dict(id=m.id))
        self.assertDictEqual(resp, {})
        self.assertRaises(DoesNotExist, self.model.filter(id=m.id).get)

    def test_retrieve_not_exists(self):
        """
        Tests that a NotFoundException is raised
        if the model does not exist
        """
        manager = self.manager_class()
        self.assertRaises(NotFoundException, manager.retrieve, dict(id='blah'))

    def test_update_not_exists(self):
        """
        Tests that a NotFoundException is raised
        if the model does not exist
        """
        manager = self.manager_class()
        self.assertRaises(NotFoundException, manager.update, dict(id='blah'), {})

    def test_delete_not_exists(self):
        """
        Tests that a NotFoundException is raised
        if the model does not exist
        """
        manager = self.manager_class()
        self.assertRaises(NotFoundException, manager.delete, dict(id='blah'))

    def test_get_field_type(self):
        self.assertIsInstance(self.manager_class.get_field_type('id'), fields.StringField)
        self.assertIsInstance(self.manager_class.get_field_type('value'), fields.StringField)
        self.assertEqual(self.manager_class.get_field_type('id').name, 'id')
        self.assertEqual(self.manager_class.get_field_type('value').name, 'value')
