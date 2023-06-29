#coding=utf-8
from setuptools import setup, find_packages

setup(
    name='redis_filemem_cache',
    version='0.1.0',
    author='ashaya',
    author_email='',
    description='Redis and File cache for storing functions indexed by datetime',
    url="https://github.com/ashaya23/redis-filemem-cache",
    python_requires='>=3.6',
    packages=['filememcache'],
    install_requires=[line.strip() for line in openf("requirements.txt") if line.strip()],
    classifiers=[
        'Topic :: Utilities',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python'
    ],
)
