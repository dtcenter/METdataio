from distutils.core import setup

setup(
    name='METdbLoad',
    version='0.1dev',
    description='Rewrite of Java MVLoad for METviewer and METexpress database load',
    author='Venita Hagerty',
    author_email='venita.hagerty@noaa.gov',
    packages=['METdbLoad','METdbLoad.ush','METdbLoad.tests'],
    long_description=open('README.md').read(),
)