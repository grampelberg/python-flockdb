#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import curses
import logging
import sys
import time
import unittest

LOG_LEVEL = 'debug'

TEST_MODULES = [
    'flockdb.test.client',
]

def enable_pretty_logging():
    try:
        curses.setupterm()
        if curses.tigetnum("colors") > 0:
            color = True
    except Exception:
        pass
    channel = logging.StreamHandler()
    channel.setFormatter(_LogFormatter())
    logging.getLogger().addHandler(channel)

class _LogFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        fg_color = unicode(curses.tigetstr("setaf") or
                           curses.tigetstr("setf") or "", "ascii")
        self._colors = {
            logging.DEBUG: unicode(curses.tparm(fg_color, 4), # Blue
                                   "ascii"),
            logging.INFO: unicode(curses.tparm(fg_color, 2), # Green
                                  "ascii"),
            logging.WARNING: unicode(curses.tparm(fg_color, 3), # Yellow
                                     "ascii"),
            logging.ERROR: unicode(curses.tparm(fg_color, 1), # Red
                                   "ascii"),
            }
        self._normal = unicode(curses.tigetstr("sgr0"), "ascii")

    def format(self, record):
        try:
            record.message = record.getMessage()
        except Exception, e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)
        record.asctime = time.strftime(
            "%y%m%d %H:%M:%S", self.converter(record.created))
        prefix = '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]' % \
            record.__dict__
        prefix = (self._colors.get(record.levelno, self._normal) +
                  prefix + self._normal)
        formatted = prefix + " " + record.message
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            formatted = formatted.rstrip() + "\n" + record.exc_text
        return formatted.replace("\n", "\n    ")

def all():
    return unittest.defaultTestLoader.loadTestsFromNames(TEST_MODULES)

if __name__ == '__main__':
    argv = sys.argv
    enable_pretty_logging()

    logging.getLogger().setLevel(getattr(logging, LOG_LEVEL.upper()))

    try:
        if len(argv) > 1:
            unittest.main(module=None, argv=argv)
        else:
            unittest.main(defaultTest="all", argv=argv)
    except SystemExit, e:
        if e.code == 0:
            logging.info('PASS')
        else:
            logging.error('FAIL')
