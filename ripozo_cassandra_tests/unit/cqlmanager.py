from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ripozo_cassandra.cqlmanager import CQLManager

import mock
import unittest2


class TestCQLManager(unittest2.TestCase):
    def test_serialize_model(self):
        """
        Dumb test for explosions.
        """
        x = dict(x=1)
        resp = CQLManager().serialize_model(x, fields=['x'])
        self.assertDictEqual(x, resp)

    # def test_pagination_filtration(self):
    #     assert False
    #
    # def test_get_next_query_args(self):
    #     assert False
    #
    # def test_get_model(self):
    #     assert False
    #
    # def test_create(self):
    #     assert False
    #
    # def test_retrieve(self):
    #     assert False
    #
    # def test_retrieve_list(self):
    #     assert False
    #
    # def test_update(self):
    #     assert False
    #
    # def test_delete(self):
    #     assert False
    #
    # def test_queryset(self):
    #     assert False
    #
    # def test_get_field_type(self):
    #     assert False
