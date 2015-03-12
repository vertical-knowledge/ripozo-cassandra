from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cqlengine.models import Model
from cqlengine import columns, connection, ONE
from cqlengine.management import create_keyspace, sync_table, delete_keyspace
from uuid import uuid1

class Person(Model):
    id = columns.UUID(primary_key=True)
    first_name = columns.Text(max_length=100)
    last_name = columns.Text(max_length=100)

    def save(self):
        if self.id is None:
            self.id = uuid1()
        return super(Person, self).save()


keyspace_name = 'ripozo_sqlalchemy_test'

connection.setup(['192.168.56.102', '192.168.56.103', '192.168.56.104'],
                 keyspace_name, ONE)

def setup_cassandara():
    create_keyspace(keyspace_name, strategy_class='SimpleStrategy',
                    replication_factor=1)
    sync_table(Person)


def teardown_cassandra():
    delete_keyspace(keyspace_name)