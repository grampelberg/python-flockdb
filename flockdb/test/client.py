#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import logging
import random
import time
import unittest

import flockdb.client
import flockdb.test.utils

class Client(flockdb.test.utils.SilentLog):

    def setUp(self):
        self.client = flockdb.client.Client("localhost", 7915, {
                "follow": 1,
                "block": 2,
                })

    def _add(self, *args):
        result = self.client.add(*args)
        # Since flockdb is eventually consistent, there needs to be a little
        # time before moving on to a get.
        time.sleep(0.2)
        return result

    def assert_edge(self, source, graph, dest):
        result = self.client.get(source, graph, dest)
        self.assertTrue(dest in result,
                        "Edge should be in the result set")
        return result

    def test_add(self):
        tests = [[random.randint(0, 1000) for x in range(2)] for y in range(2)]

        source, dest = tests[0]
        result = self._add(source, "follow", dest)
        result = self.assert_edge(source, "follow", dest)
        self.assertEqual(1, len(result),
                         "Should only be one result")

        # Test multiple graphs
        source, dest = tests[1]
        self._add(source, "block", dest)
        result = self.assert_edge(source, "block", dest)
        self.assertEqual(
            1, len(result),
            "This is a different graph, should only be one result")

        # Missing!
        source, dest = tests[0]
        result = self.client.get(source, "block", dest)
        self.assertEqual(0, len(result),
                         "This edge doesn't exist")
        result = self.client.get(dest, "follow", source)
        self.assertEqual(0, len(result),
                         "This edge doesn't exist")

    def test_get(self):
        source = random.randint(0, 1000)
        dest = random.randint(0, 1000)

        # Straight get.
        result = self._add(source, 'follow', dest)
        self.assert_edge(source, "follow", dest)

        # What edges come off a node
        result = self.client.get(source, "follow", None)
        self.assertTrue(dest in result,
                        "Edge should be in result set")

        # Edges incoming to a node
        result = self.client.get(None, "follow", dest)
        self.assertTrue(source in result,
                        "Edge should be in result set")
