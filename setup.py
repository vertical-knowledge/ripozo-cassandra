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
    packages=find_packages(include=['ripozo_cassandra', 'ripozo_cassandra.*']),
    url='',
    license='',
    author='Tim Martin',
    author_email='tim.martin@vertical-knowledge.com',
    description=('Integrates cassandra/cqlengine with ripozo to'
                 ' easily create cassandra backed Hypermedia/HATEOAS/REST apis'),
    install_requires=[
        'ripozo',
        'cassandra-driver',
        'six',
    ],
    tests_require=[
        'unittest2',
        'mock'
    ],
    test_suite='ripozo_cassandra_tests',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3'
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)