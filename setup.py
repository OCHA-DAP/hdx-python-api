from setuptools import setup, find_packages

from hdx.utilities.loader import script_dir_plus_file

requirements = ['ckanapi',
                'colorlog',
                'ndg-httpsclient',
                'pyasn1',
                'pyOpenSSL',
                'pyaml',
                'requests',
                'scraperwiki',
                'typing'
                ]

version_file = open(script_dir_plus_file('version.txt', requirements))
version = version_file.read().strip()

setup(
    name='hdx-python-api',
    version=version,
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    url='http://data.humdata.org/',
    license='PSF',
    author='Michael Rans',
    author_email='rans@email.com',
    description='HDX Python Library',

    install_requires=requirements,
    package_data={
        # If any package contains *.yml files, include them:
        '': ['*.yml'],
    },
)
