import setuptools
from setuptools import setup, find_packages
from distutils.util import convert_path

with open("README.md", "r") as fh:
    long_description = fh.read()

main_ns = {}
version_path = convert_path('docs/version')
with open(version_path) as version_file:
    exec(version_file.read(), main_ns)

setuptools.setup(
    name="metdatadb",
    version=main_ns['__version__'],
    author="METplus",
    author_email="met-help@ucar.edu",
    description="METplus component that loads data into a database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dtcenter/METdataio",
    packages=setuptools.find_packages(),
    classifiers=[
         "Programming Language :: Python :: 3",
         "License :: Apache LICENSE-2.0",
         "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
