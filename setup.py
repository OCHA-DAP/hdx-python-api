# -*- coding: utf-8 -*-
import inspect
import sys
from codecs import open
from distutils import log
from distutils.command.clean import clean as _clean
from os.path import join, abspath, realpath, dirname, exists
from shutil import rmtree

from setuptools import setup, find_packages


class CleanMore(_clean):
    """Custom implementation of ``clean`` setuptools command."""

    def run(self):
        """After calling the super class implementation, this function removes
        the dist directory."""
        super(CleanMore, self).run()
        dir_ = 'dist'
        if exists(dir_):
            log.info("removing '%s' (and everything under it)", dir_)
            rmtree(dir_)
        else:
            log.info("'%s' does not exist -- can't clean it", dir_)


# Sadly we cannot use the utilities here because of the typing module which isn't in Python < 3.5
def script_dir(pyobject, follow_symlinks=True):
    """Get current script's directory

    Args:
        pyobject (Any): Any Python object in the script
        follow_symlinks (bool): Follow symlinks or not. Defaults to True.

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
        follow_symlinks (bool): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory and with filename appended
    """
    return join(script_dir(pyobject, follow_symlinks), filename)


def get_version():
    version_file = open(script_dir_plus_file(join('src', 'hdx', 'version.txt'), get_version), encoding='utf-8')
    return version_file.read().strip()


def get_readme():
    readme_file = open(script_dir_plus_file('README.rst', get_readme), encoding='utf-8')
    return readme_file.read()


requirements = ['ckanapi>=4.1',
                'hdx-python-country>=2.0.2',
                'ndg-httpsclient',
                'pyasn1',
                'pyOpenSSL',
                'python-dateutil'
                ]

classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

setup(
    name='hdx-python-api',
    description='HDX Python Library',
    license='MIT',
    url='https://github.com/OCHA-DAP/hdx-python-api',
    version=get_version(),
    author='Michael Rans',
    author_email='rans@email.com',
    keywords=['HDX', 'API', 'library'],
    long_description=get_readme(),
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    zip_safe=True,
    classifiers=classifiers,
    install_requires=requirements,
    cmdclass={'clean': CleanMore}
)
