#!/usr/bin/env python
#
# Copyright (c) 2011 BitTorrent Inc.
#

__author__ = 'Thomas Rampelberg'
__author_email__ = 'thomas@bittorrent.com'

from setuptools import setup, find_packages

setup(
    name = "flockdb",
    version = "0.1",
    packages = find_packages(),
    author = __author__,
    author_email = __author_email__,
    description = "Flockdb from python",
    # pycurl, tornado, numpy, scipy and PIL are required but break chef in
    # stupid ways.
    install_requires = [ "thrift" ]
    )
