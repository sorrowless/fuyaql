#!/usr/bin/env python
from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), 'r') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt'), 'r') as f:
    required_packages = f.read()

setup(name='fuelyaql',
      version='0.1.0',

      author="Stanislaw Bogatkin",
      author_email="sbogatkin@mirantis.com",

      description=("Fuel-YAQL is real-time console for evaluating YAQL " +
                   "queries on Fuel master node"),
      long_description=long_description,
      url='https://github.com/sorrowless/fuyaql',
      keywords="fuel openstack yaql",
      license="GPLv3",

      packages=['fuelyaql'],
      install_requires=required_packages,

      package_data={
        '': ['requirements.txt' 'test-requirements.txt', 'README.md'],
      },

      entry_points={
        'console_scripts': [
          'fuelyaql=fuelyaql.fuyaql:main',
        ],
      })
