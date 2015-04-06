# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from setuptools import setup, find_packages

from jukoro import version


requires = [
    # 'Cython>=0.21.0',
    'redis>=2.10.0',
    'hiredis>=0.1.0',
    'psycopg2>=2.5.0',
    # 'pytz>=2014.10',
    'base32-crockford>=0.2.0',
    'arrow>=0.5.4',
]

setup(
    name='jukoro',
    version=version,
    description='Jukoro library (to keep code clean and DRY)',
    author='Egorov Yuri',
    author_email='ysegorov@gmail.com',
    url='https://github.com/ysegorov/jukoro.git',
    packages=find_packages(exclude=('tests', 'tests.*')),
    #package_data={'jukoro': []},
    include_package_data=True,
    install_requires=requires,
)
