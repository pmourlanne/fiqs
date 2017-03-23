# -*- coding: utf-8 -*-

from os.path import join, dirname
from setuptools import setup, find_packages


VERSION = (0, 3, 0)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))


f = open(join(dirname(__file__), 'README.md'))
long_description = f.read().strip()
f.close()


install_requires = [
    'urllib3>=1.8, <2.0',
    'elasticsearch==5.0.1',
    'elasticsearch-dsl==5.0.0',
]
tests_require = [
    'six==1.10.0',
    'Faker==0.7.3',
    'pytest==3.0.5',
    "pytest-cov",
]


setup(
    name='fiqs',
    description="Python client for Elasticsearch built on top of elasticsearch-dsl",
    license="MIT License",
    url="https://gitlab.com/pmourlanne/fiqs",
    long_description = long_description,
    version=__versionstr__,
    author="Pierre Mourlanne",
    author_email="pmourlanne@gmail.com",
    packages=find_packages(
        where='.',
    ),
    install_requires=install_requires,

    test_suite='fiqs.tests.run_tests.run_all',
    tests_require=tests_require,
)
