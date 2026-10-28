# Default Configuration for Facades

The easiest way to get started is to use the facades and configuration defaults. The
facades set up both logging and HDX configuration.

The default configuration loads an internal HDX configuration located within the
library, and assumes that there is an API key file called **.hdxkey** in the current
user's home directory **\~** and a YAML project configuration located relative to your
working directory at **config/project_configuration.yaml** which you must create. The
project configuration is used for any configuration specific to your project.

The default logging configuration reads a configuration file internal to the library
that sets up an coloured console handler outputting at INFO level and a file handler
writing to errors.log at ERROR level.
