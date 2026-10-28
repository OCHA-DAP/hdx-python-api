# Configure Logging

If you use a facade from **hdx.facades**, then logging will go to console and errors to
file. If you are not using a facade, you can call **setup_logging**  which takes
an argument error_file which is False by default. If set to True, errors will be written
to a file.

If not using facade:

    from hdx.utilities.easy_logging import setup_logging
    ...
    logger = logging.getLogger(__name__)
    setup_logging(console_log_level="DEBUG", log_file="output.log",
    file_log_level="INFO")

To use logging in your files, simply add the line below to the top of each Python file:

    logger = logging.getLogger(__name__)

Then use the logger like this:

    logger.debug("DEBUG message")
    logger.info("INFORMATION message")
    logger.warning("WARNING message")
    logger.error("ERROR message")
    logger.critical("CRITICAL error message")
