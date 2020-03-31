# -*- coding: utf-8 -*-

import io
import os
import sys
from shutil import rmtree

from setuptools import setup, find_packages


# Package metadata
# ----------------

NAME = 'jetavator_cli'
DESCRIPTION = (
    'A tool for publishing derived data sets '
    'and data science models as OLAP cubes'
)
URL = 'https://github.com/jetavator/jetavator'
EMAIL = 'joetaylorconsulting@gmail.com'
AUTHOR = 'Joe Taylor'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = None

# What packages are required for this module to be executed?
REQUIRED = [
    'pandas>=0.23,<1',
    'pyodbc>=4.0,<5',
    'SQLAlchemy>=1.3,<1.4',
    'sqlalchemy-views>=0.2.3,<1',
    'PyYAML>=3.12,<4',
    'tabulate>=0.8,<1',
    'behave>=1.2,<2',
    'docopt>=0.6,<1',
    'keyring>=19.0,<20',
    'sqlparse>=0.3.0,<1',
    'semver>=2.8.1,<3',
    'databricks-dbapi>=0.3.0,<1',
    'future>=0.18.2,<1',
    'lazy-property>=0.0.1,<1',
    'databricks-cli>=0.9.1,<1',
    'nbformat>=5.0.3,<6'
]

# What packages are optional?
EXTRAS = {
    # 'some-feature': ['requirement'],
}

# Package setup
# -------------

# Import the README and use it as the long description

here = os.path.abspath(os.path.dirname(__file__))

try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Import the LICENSE

with open(os.path.join(here, 'LICENSE')) as f:
    license = f.read()

# Load the package's __version__.py module as a dictionary

about = {}
if not VERSION:
    with open(os.path.join(here, NAME, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license=license,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'
    ],
    entry_points={
        'console_scripts': [
            'jetavator=jetavator_cli.cli:main',
        ],
    }
)
