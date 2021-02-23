#!/usr/bin/env python3

from setuptools import setup, find_packages
from pdsando.version import __version__

setup(
    name="pdsando",
    url="https://github.com/natsunlee/pdsando",
    version=__version__,
    author="Nathan Lee",
    author_email="lee.nathan.sh@gmail.com",
    install_requires=["mplfinance", "pdpipe", "pandas", "numpy", "jsonschema"],
    packages=find_packages(),
    license="Apache 2.0",
    long_description="Pandas Sando.",
)