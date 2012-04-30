#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import logging
import random
import time
import unittest

import flockdb
import flockdb.test.utils

class Client(flockdb.test.utils.SilentLog):

    def setUp(self):
        self.client = flockdb.Client("localhost", 7915, {
            "follows": 1,
            "blocks": 2,
        })

    @staticmethod
    def random_id():
        milliseconds_since_epoch = long(time.time() * 1000)
        random_bits = random.getrandbits(20)
        return (milliseconds_since_epoch << 20) + random_bits

    def _add_all(self, edges):
        for edge in edges:
            self.client.add(*edge)
        # Since flockdb is eventually consistent, there needs to be a little
        # time before moving on to a get.
        time.sleep(0.2)

    def _add(self, *args):
        self._add_all([args])

    def assert_edge(self, source, graph, dest, k=None):
        result = self.client.get(source, graph, dest)
        self.assertTrue(dest or k in result,
                        "Edge should be in the result set")
        return result

    def assert_no_edge(self, *args):
        result = self.client.get(*args)
        self.assertEqual(0, len(result),
                         "This edge doesn't exist")

    def test_add(self):
        tests = [[self.random_id() for x in range(2)] for y in range(2)]

        source, dest = tests[0]
        result = self._add(source, "follows", dest)
        result = self.assert_edge(source, "follows", dest)
        self.assertEqual(1, len(result),
                         "Should only be one result")

        # Test multiple graphs
        source, dest = tests[1]
        self._add(source, "blocks", dest)
        result = self.assert_edge(source, "blocks", dest)
        self.assertEqual(
            1, len(result),
            "This is a different graph, should only be one result")

        # Missing!
        source, dest = tests[0]
        self.assert_no_edge(source, "blocks", dest)
        self.assert_no_edge(dest, "follows", source)

    def test_get(self):
        source = self.random_id()
        dest = self.random_id()

        # Straight get.
        query = (source, 'follows', dest)
        result = self._add(*query)
        self.assert_edge(*query)

        # What edges come off a node
        self.assert_edge(source, "follows", None, k=dest)

        # Edges incoming to a node
        self.assert_edge(None, "follows", dest, k=source)

    def test_get_all(self):
        tests = [(self.random_id(), "follows", self.random_id())
                 for y in range(5)]

        self._add_all(tests)

        for i, j in zip(tests, self.client.get_all(tests)):
            self.assertTrue(i[-1] in j,
                            "Edge should be in the result")

    def test_get_metadata(self):
        source = self.random_id()
        dest = self.random_id()
        query = (source, 'follows', dest)
        self._add(*query)
        self.assert_edge(*query)

        result = self.client.get_metadata(source, "follows")
        self.assertEqual(result.source_id, source,
                         "Got the right node back")
        self.assertEqual(result.state_id, 0,
                         "Still in a positive state")
        self.assertEqual(result.count, 1,
                         "Only one edge")
        self.assertEqual(result.updated_at, 0,
                         "Library looks broken")

        self._add(source, 'follows', self.random_id())
        result = self.client.get_metadata(source, "follows")
        self.assertEqual(result.count, 2,
                         "Two edges now")
        logging.info(result)

    def test_remove(self):
        source = self.random_id()
        dest = self.random_id()
        query = (source, 'follows', dest)
        self._add(*query)
        self.assert_edge(*query)

        self.client.remove(*query)
        time.sleep(0.2)
        self.assert_no_edge(*query)

    def test_transaction(self):
        source, dest1, dest2, dest3 = [self.random_id() for i in xrange(4)]

        with self.client.transaction() as t:
            t.add(source, 'follows', dest1)
            t.add(source, 'follows', dest2)

        time.sleep(0.2)
        self.assert_edge(source, 'follows', dest1)
        self.assert_edge(source, 'follows', dest2)

        with self.client.transaction() as t:
            t.remove(source, 'follows', dest2)
            t.add(source, 'blocks', dest2)
            t.add(source, 'follows', dest3)

        time.sleep(0.2)
        self.assert_no_edge(source, 'follows', dest2)
        self.assert_edge(source, 'blocks', dest2)
        self.assert_edge(source, 'follows', dest3)
