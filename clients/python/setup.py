"""
Setup script for Python PTO client

Inspired by the PyPA sample package, https://github.com/pypa/sampleproject

"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ptoclient',
    version='3.0.0',
    description='MAMI Path Transparency Observatory client',
    long_description=long_description, 
    long_description_content_type='text/markdown', 
    url='https://github.com/mami-project/pto3-go',
    author='Brian Trammell',
    author_email='trammell@tik.ee.ethz.ch',
    py_modules=["ptoclient"],
    install_requires=['requests','pandas']
)