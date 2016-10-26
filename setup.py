from setuptools import setup, find_packages

from hdx.utilities.path import script_dir_plus_file


def get_version():
    version_file = open(script_dir_plus_file('version.txt', get_version))
    return version_file.read().strip()

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
)
