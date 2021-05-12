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
REQUIRED = None

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


requirements = {}
if not REQUIRED:
    with open(os.path.join(here, NAME, '__required__.py')) as f:
        exec(f.read(), requirements)
else:
    requirements['REQUIRED'] = REQUIRED


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
    install_requires=requirements['REQUIRED'],
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
