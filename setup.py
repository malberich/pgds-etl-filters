#!/usr/bin/env/python

from distutils.core import setup
from distutils.extension import Extension

setup(
    name="minteressa",
    version='0.1',
    packages=[
        "minteressa",
        "minteressa.etl",
        "minteressa.etl.connectors",
        "minteressa.etl.filters",
        "minteressa.etl.model"
    ]
)
