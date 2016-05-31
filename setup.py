from setuptools import setup, find_packages

setup(
    name='hdx-python-api',
    version='0.1',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']) + ['config'],
    url='http://data.humdata.org/',
    license='PSF',
    author='Michael Rans',
    author_email='rans@email.com',
    description='HDX Python Library',

    install_requires = ['colorlog>=2.6.3',
                        'ndg-httpsclient>=0.4.0',
                        'pyasn1>=0.1.9',
                        'pyOpenSSL>=16.0.0',
                        'pyaml>=15.8.2',
                        'requests>=2.9.1',
                        'scraperwiki>=0.5.1'
                        ],
    package_data = {
        # If any package contains *.txt or *.md files, include them:
        '': ['*.txt', '*.md'],
        # And include any *.yml files found in the 'config' package, too:
        'config': ['*.yml'],
    },
)
