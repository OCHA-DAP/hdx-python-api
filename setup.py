from setuptools import setup, find_packages

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
    version='0.52',
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
