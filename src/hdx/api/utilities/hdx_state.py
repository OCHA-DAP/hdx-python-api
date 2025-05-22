"""Utility to save state to a dataset and read it back."""

import logging
from os.path import join
from typing import Any, Callable, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.loader import load_text
from hdx.utilities.saver import save_text
from hdx.utilities.state import State

logger = logging.getLogger(__name__)


class HDXState(State):
    """State class that allows the reading and writing of state to a given HDX
    dataset. Input and output state transformations can be supplied in read_fn and
    write_fn respectively. The input state transformation takes in a string
    while the output transformation outputs a string.

    Args:
        dataset_name_or_id (str): Dataset name or ID
        path (str): Path to temporary folder for state
        read_fn (Callable[[str], Any]): Input state transformation. Defaults to lambda x: x.
        write_fn: Callable[[Any], str]: Output state transformation. Defaults to lambda x: x.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(
        self,
        dataset_name_or_id: str,
        path: str,
        read_fn: Callable[[str], Any] = lambda x: x,
        write_fn: Callable[[Any], str] = lambda x: x,
        configuration: Optional[Configuration] = None,
    ) -> None:
        self._dataset_name_or_id = dataset_name_or_id
        self._resource = None
        self._configuration = configuration
        super().__init__(path, read_fn, write_fn)

    def read(self) -> Any:
        """Read state from HDX dataset

        Returns:
            Any: State
        """
        dataset = Dataset.read_from_hdx(
            self._dataset_name_or_id, configuration=self._configuration
        )
        self._resource = dataset.get_resource()
        _, path = self._resource.download()
        value = self.read_fn(load_text(path))
        logger.info(f"State read from {self._dataset_name_or_id} = {value}")
        return value

    def write(self) -> None:
        """Write state to HDX dataset

        Returns:
            None
        """
        logger.info(f"State written to {self._dataset_name_or_id} = {self.state}")
        filename = self._resource["name"]
        file_to_upload = join(self.path, filename)
        save_text(self.write_fn(self.state), file_to_upload)
        self._resource.set_file_to_upload(file_to_upload)
        self._resource.update_in_hdx()
