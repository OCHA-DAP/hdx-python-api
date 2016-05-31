from setuptools import setup, find_packages
setup(
    name = "hdx-python-api",
    version = "0.1",
    packages = find_packages(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = ['colorlog>=2.6.3',
                        'ndg-httpsclient>=0.4.0',
                        'pyasn1>=0.1.9',
                        'pyOpenSSL>=16.0.0',
                        'pyaml>=15.8.2',
                        'requests>=2.9.1',
                        'scraperwiki>=0.5.1'
                        ],

    package_data = {
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.md'],
        # And include any *.yml files found in the 'config' package, too:
        'config': ['*.yml'],
    },

    # metadata for upload to PyPI
    author = "Michael Rans",
    author_email = "rans@email.com",
    description = "HDX Python Library",
    license = "PSF",
    keywords = "HDX Python API",
    url = "http://data.humdata.org/"   # project home page

)