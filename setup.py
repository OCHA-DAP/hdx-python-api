from os.path import join

from setuptools import setup

from hdx.utilities import CleanCommand, PackageCommand, PublishCommand
from hdx.utilities.loader import load_file_to_str

PublishCommand.version = load_file_to_str(join("src", "hdx", "version.txt"), strip=True)

setup(
    version=PublishCommand.version,
    cmdclass={
        "clean": CleanCommand,
        "package": PackageCommand,
        "publish": PublishCommand,
    },
)
