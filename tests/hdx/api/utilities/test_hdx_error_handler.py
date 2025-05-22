"""Errors on exit Tests"""

import logging

import pytest

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.easy_logging import setup_logging

setup_logging()


class TestHDXErrorHandler:
    def test_hdx_error_handler(self, caplog):
        with pytest.raises(SystemExit):
            with caplog.at_level(logging.ERROR):
                with HDXErrorHandler(should_exit_on_error=True) as errors:
                    errors.add_message(
                        "pipeline1",
                        "dataset1",
                        "error message",
                        "resource1",
                        err_to_hdx=True,
                    )
                    errors.add_missing_value_message(
                        "pipeline1",
                        "dataset1",
                        "field1",
                        123,
                        "resource1",
                        err_to_hdx=True,
                    )
                    errors.add_multi_valued_message(
                        "pipeline1",
                        "dataset1",
                        "following values changed",
                        [1, 2, 3, 4],
                        "resource1",
                        err_to_hdx=True,
                    )
                    assert (
                        len(errors.shared_errors["error"]["pipeline1 - dataset1"]) == 3
                    )
                    assert (
                        len(
                            errors.shared_errors["hdx_error"][
                                ("pipeline1", "dataset1", "resource1")
                            ]
                        )
                        == 3
                    )
                    errors.output_errors()
                assert "following values changed" in caplog.text
