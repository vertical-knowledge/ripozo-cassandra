from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

version = '0.1.0b1'

setup(
    name='ripozo-cassandra',
    version=version,
    packages=find_packages(exclude=['tests', 'tests.*']),
    url='',
    license='',
    author='Tim Martin',
    author_email='tim.martin@vertical-knowledge.com',
    description='',
    install_requires=['ripozo', 'cqlengine'],
    tests_require=['ripozo-tests'],
    test_suite='tests'
)