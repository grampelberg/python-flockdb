#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import logging
from cStringIO import StringIO
import unittest

class SilentLog(unittest.TestCase):
    def run(self, result=None):
        logger = logging.getLogger()
        handler = logger.handlers[0]
        old_stream = handler.stream
        try:
            handler.stream = StringIO()
            logging.info("RUNNING TEST: " + str(self))
            old_error_count = len(result.failures) + len(result.errors)
            super(SilentLog, self).run(result)
            new_error_count = len(result.failures) + len(result.errors)
            if new_error_count != old_error_count:
                old_stream.write(handler.stream.getvalue())
        finally:
            handler.stream = old_stream
