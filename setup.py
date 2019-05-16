# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name="sqlcell",
    version="0.2.0.9",
    description="run sql in jupyter notebooks or jupyter lab",
    license="MIT",
    author="Tim Dobbins",
    author_email="noneya@gmail.com",
    url="https://github.com/tmthyjames/SQLCell",
    packages=find_packages(),
    install_requires=[
        'ipython',
        'ipywidgets',
        'sqlalchemy',
        'pandas'
    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
    package_dir={'sqlcell': 'sqlcell'},
    py_modules=['sqlcell']
)