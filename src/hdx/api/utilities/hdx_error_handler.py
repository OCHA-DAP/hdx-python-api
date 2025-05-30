import logging
from os import getenv
from typing import Any, Tuple

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import dict_of_sets_add
from hdx.utilities.error_handler import ErrorHandler
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


class HDXErrorHandler(ErrorHandler):
    """Class that enables recording of errors and warnings.

    Errors and warnings can be logged by calling the `output` method or
    automatically logged on exit. Messages are output grouped by category and
    sorted.

    Args:
        should_exit_on_error (bool): Whether to exit with a 1 code if there are errors. Default is False.
        write_to_hdx (Any): Whether to write errors to HDX resources. Default is None (write errors).

    """

    def __init__(
        self,
        should_exit_on_error: bool = False,
        write_to_hdx: Any = None,
    ):
        super().__init__(should_exit_on_error)
        if write_to_hdx is None:
            write_to_hdx = getenv("ERR_TO_HDX", True)
        if write_to_hdx in (False, 0, "false", "False", "FALSE", "N", "n", ""):
            self._write_to_hdx = False
            logger.info("Errors won't be written to HDX")
        else:
            self._write_to_hdx = True
            logger.info("Errors will be written to HDX")
        self.shared_errors["hdx_error"] = {}

    @staticmethod
    def get_category(pipeline: str, identifier: str) -> str:
        """
        Get category from pipeline and identifier

        Args:
            pipeline (str): Name of the pipeline originating the error
            identifier (str): Identifier e.g. dataset name
        Returns:
            str: Category
        """
        return f"{pipeline} - {identifier}"

    def errors_to_hdx(
        self,
        pipeline: str,
        identifier: str,
        text: str,
        resource_name: str = "",
        err_to_hdx: bool = False,
    ) -> None:
        """
        Add a new message to the hdx_error type

        Args:
            pipeline (str): Name of the pipeline originating the error
            identifier (str): Identifier e.g. dataset name
            text (str): Text to use e.g. "sector CSS not found in table"
            resource_name (str): The resource name that the message applies to. Only needed if writing errors to HDX
            message_type (str): The type of message (error or warning). Default is "error"
            err_to_hdx (bool): Flag indicating if the message should be added to HDX metadata. Default is False
        Returns:
            None
        """
        if err_to_hdx:
            category = (pipeline, identifier, resource_name)
            dict_of_sets_add(self.shared_errors["hdx_error"], category, text)

    def add_message(
        self,
        pipeline: str,
        identifier: str,
        text: str,
        resource_name: str = "",
        message_type: str = "error",
        err_to_hdx: bool = False,
    ) -> None:
        """
        Add a new message (typically a warning or error) to a dictionary of messages in a
        fixed format:
            pipeline - identifier - {text}
        identifier is usually a dataset name.

        Args:
            pipeline (str): Name of the pipeline originating the error
            identifier (str): Identifier e.g. dataset name
            text (str): Text to use e.g. "sector CSS not found in table"
            resource_name (str): The resource name that the message applies to. Only needed if writing errors to HDX
            message_type (str): The type of message (error or warning). Default is "error"
            err_to_hdx (bool): Flag indicating if the message should be added to HDX metadata. Default is False
        Returns:
            None
        """
        self.add(text, self.get_category(pipeline, identifier), message_type)
        self.errors_to_hdx(pipeline, identifier, text, resource_name, err_to_hdx)

    def add_missing_value_message(
        self,
        pipeline: str,
        identifier: str,
        value_type: str,
        value: Any,
        resource_name: str = "",
        message_type: str = "error",
        err_to_hdx: bool = False,
    ) -> None:
        """
        Add a new message (typically a warning or error) concerning a missing value
        to a dictionary of messages in a fixed format:
            pipeline - identifier - {text}
        identifier is usually a dataset name.

        Args:
            pipeline (str): Name of the scaper originating the error
            identifier (str): Identifier e.g. dataset name
            value_type (str): Type of value e.g. "sector"
            value (Any): Missing value
            resource_name (str): The resource name that the message applies to. Only needed if writing errors to HDX
            message_type (str): The type of message (error or warning). Default is "error"
            err_to_hdx (bool): Flag indicating if the message should be added to HDX metadata. Default is False
        Returns:
            None
        """
        text = self.missing_value_message(value_type, value)
        self.add_message(
            pipeline,
            identifier,
            text,
            resource_name,
            message_type,
            err_to_hdx,
        )

    def add_multi_valued_message(
        self,
        pipeline: str,
        identifier: str,
        text: str,
        values: ListTuple,
        resource_name: str = "",
        message_type: str = "error",
        err_to_hdx: bool = False,
    ) -> bool:
        """
        Add a new message (typically a warning or error) concerning a list of
        values to a set of messages in a fixed format:
            pipeline - identifier - n {text}. First 10 values: n1,n2,n3...
        If less than 10 values, ". First 10 values" is omitted. identifier is usually
        a dataset name.

        Args:
            pipeline (str): Name of the scaper originating the error
            identifier (str): Identifier e.g. dataset name
            text (str): Text to use e.g. "negative values removed"
            values (ListTuple): The list of related values of concern
            resource_name (str): The resource name that the message applies to. Only needed if writing errors to HDX
            message_type (str): The type of message (error or warning). Default is "error"
            err_to_hdx (bool): Flag indicating if the message should be added to HDX metadata. Default is False
        Returns:
            bool: True if a message was added, False if not
        """
        text = self.multi_valued_message(text, values)
        if text is None:
            return False
        self.add_message(
            pipeline,
            identifier,
            text,
            resource_name,
            message_type,
            err_to_hdx,
        )
        return True

    def write_errors_to_hdx(self) -> None:
        """
        Write to HDX resources corresponding errors that have been flagged by
        setting err_to_hdx True when adding messages

        Returns:
            None
        """
        logger.info("Writing errors to HDX")
        for identifier, errors in self.shared_errors["hdx_error"].items():
            write_errors_to_resource(identifier, errors)

    def output_errors(self) -> None:
        """
        Log errors and warnings by category and sorted. Also write to HDX
        resources corresponding errors that have been flagged by setting
        err_to_hdx True when adding messages.

        Returns:
            None
        """
        self.log()
        if self._write_to_hdx:
            self.write_errors_to_hdx()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self._write_to_hdx:
            self.write_errors_to_hdx()
        super().__exit__(exc_type, exc_value, traceback)


def write_errors_to_resource(
    identifier: Tuple[str, str, str], errors: set[str]
) -> bool:
    """
    Writes error messages to a resource on HDX. If the resource already has an
    error message, it is only overwritten if the two messages are different.

    Args:
        identifier (Tuple[str, str, str]): Scraper, dataset, and resource names that the message applies to
        errors (set[str]): Set of errors to use e.g. "negative values removed"
    Returns:
        bool: True if a message was added, False if not
    """
    # We are using the names here because errors may be specified in the YAML by us
    _, dataset_name, resource_name = identifier
    error_text = ", ".join(sorted(errors))
    dataset = Dataset.read_from_hdx(dataset_name)
    try:
        success = dataset.add_hapi_error(error_text, resource_name=resource_name)
    except (HDXError, AttributeError):
        logger.error(f"Could not write error to {dataset_name}")
        return False
    if success:
        logger.info(f"Wrote error message to {dataset_name}")
    return success
