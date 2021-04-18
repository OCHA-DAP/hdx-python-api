# -*- coding: utf-8 -*-
from os.path import join

from hdx.utilities import CleanCommand, PackageCommand, PublishCommand
from hdx.utilities.loader import load_file_to_str
from setuptools import setup, find_packages

requirements = ['ckanapi >= 4.5',
                'hdx-python-country>=2.8.4',
                'ndg-httpsclient',
                'pyasn1',
                'pyOpenSSL',
                'quantulum>=0.1.13;python_version<"3"',
                'quantulum3>=0.7.6;python_version>="3"'
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

PublishCommand.version = load_file_to_str(join('src', 'hdx', 'version.txt'), strip=True)

setup(
    name='hdx-python-api',
    description='HDX Python Library',
    license='MIT',
    url='https://github.com/OCHA-DAP/hdx-python-api',
    version=PublishCommand.version,
    author='Michael Rans',
    author_email='rans@email.com',
    keywords=['HDX', 'API', 'library'],
    long_description=load_file_to_str('README.md'),
    long_description_content_type='text/markdown',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    zip_safe=True,
    classifiers=classifiers,
    install_requires=requirements,
    cmdclass={'clean': CleanCommand, 'package': PackageCommand, 'publish': PublishCommand},
)
