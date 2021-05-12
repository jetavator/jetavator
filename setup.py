# -*- coding: utf-8 -*-

import io
import os

from setuptools import setup, find_packages


# Package metadata
# ----------------

NAME = 'jetavator'
DESCRIPTION = (
    'A tool for publishing derived data sets '
    'and data science models as OLAP cubes'
)
URL = 'https://github.com/jetavator/jetavator'
EMAIL = 'joetaylorconsulting@gmail.com'
AUTHOR = 'Joe Taylor'
REQUIRES_PYTHON = '>=3.8.0'
VERSION = None

# What packages are required for this module to be executed?
REQUIRED = [
    'wysdom>=0.2.2,<1',
    'pandas>=1.1.0,<2',
    'SQLAlchemy>=1.3.22,<1.4',
    'sqlalchemy-views>=0.2.3,<1',
    'PyYAML>=5.3.1,<6',
    'tabulate>=0.8,<1',
    'behave>=1.2,<2',
    'sqlparse>=0.3.0,<1',
    'semver>=2.8.1,<3',
    'future>=0.18.2,<1',
    'lazy-property>=0.0.1,<1',
    'pyspark>=3.0.1,<3.1',
    'pyspark-asyncactions>=0.0.2,<3',
    'jinja2>=2.10.3,<3',
    'click>=8.0.0,<9',
    'python-dotenv>=0.17.1,<0.18'
]

# What packages are optional?
EXTRAS = {
    'dev': [
        'coverage>=5.5,<6',
        'wheel>=0.36.2,<0.37',
        'sphinx>=3.5,<4',
        'sphinx_rtd_theme>=0.5.2,<0.6',
        'behave>=1.2.6,<2',
        'freezegun>=1.1,<2'
    ],
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
    license_text = f.read()

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
    license=license_text,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7'
    ],
    entry_points={
        'console_scripts': [
            'jetavator=jetavator.cli:cli',
        ],
    }
)
