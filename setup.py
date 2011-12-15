#!/usr/bin/env python
#

__author__ = 'Thomas Rampelberg'
__author_email__ = 'thomas@saunter.org'

from setuptools import setup, find_packages

setup(
    name = "flockdb",
    version = "0.1",
    packages = find_packages(),
    author = __author__,
    author_email = __author_email__,
    description = "Flockdb from python",
    install_requires = [ "thrift" ]
    )
