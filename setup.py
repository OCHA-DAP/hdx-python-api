# -*- coding: utf-8 -*-
import inspect
import sys
from os.path import join, abspath, realpath, dirname

from setuptools import setup, find_packages


# Sadly we cannot use the utilities here because of the typing module which isn't in Python < 3.5
def script_dir(pyobject, follow_symlinks=True):
    """Get current script's directory

    Args:
        pyobject (Any): Any Python object in the script
        follow_symlinks (Optional[bool]): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory
    """
    if getattr(sys, 'frozen', False):  # py2exe, PyInstaller, cx_Freeze
        path = abspath(sys.executable)
    else:
        path = inspect.getabsfile(pyobject)
    if follow_symlinks:
        path = realpath(path)
    return dirname(path)


def script_dir_plus_file(filename, pyobject, follow_symlinks=True):
    """Get current script's directory and then append a filename

    Args:
        filename (str): Filename to append to directory path
        pyobject (Any): Any Python object in the script
        follow_symlinks (Optional[bool]): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory and with filename appended
    """
    return join(script_dir(pyobject, follow_symlinks), filename)


def get_version():
    version_file = open(script_dir_plus_file(join('hdx', 'version.txt'), get_version))
    return version_file.read().strip()


requirements = ['ckanapi',
                'colorlog',
                'geonamescache',
                'ndg-httpsclient',
                'pyaml',
                'pyasn1',
                'pyOpenSSL',
                'python-dateutil',
                'requests',
                'scraperwiki',
                'typing'
                ]

setup(
    name='hdx-python-api',
    version=get_version(),
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    url='http://data.humdata.org/',
    license='PSF',
    author='Michael Rans',
    author_email='rans@email.com',
    description='HDX Python Library',
    install_requires=requirements,
    package_data={
        # Include version.txt and if any package contains *.yml files, include them:
        '': ['version.txt', '*.yml'],
    },
    include_package_data=True,
)
