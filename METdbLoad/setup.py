from setuptools import setup, find_packages

setup(
    name='METdbLoad',
    version='0.1.0',
    description='Rewrite of Java MVLoad for METviewer and METexpress database load',
    author='Venita Hagerty',
    author_email='venita.hagerty@noaa.gov',
    packages=find_packages(exclude=['docs', 'tests']),
    long_description=open('README.md').read(),
)