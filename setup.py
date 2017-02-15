#!/usr/bin/env python

from setuptools import setup

setup(
    name="flatutils",
    version="1.1.6",
    description="Simple, lightweight utility to sort and iterate through large pg_dumps",
    author="Adam Duston, Sam Paul",
    author_email="adam@docurated.com",
    packages=["flatutils"],
    install_requires=[
        "numpy",
        "pandas"
    ],
    platforms="Posix; MacOS X"
)
