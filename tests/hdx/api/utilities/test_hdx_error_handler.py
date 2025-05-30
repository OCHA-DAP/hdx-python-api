"""Errors on exit Tests"""

import logging

import pytest

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.easy_logging import setup_logging

setup_logging()


class TestHDXErrorHandler:
    def test_hdx_error_handler(self, caplog):
        error_handler = HDXErrorHandler()
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx=None)
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx="true")
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx="Y")
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx=True)
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx=1)
        assert error_handler._write_to_hdx is True
        error_handler = HDXErrorHandler(write_to_hdx="")
        assert error_handler._write_to_hdx is False
        error_handler = HDXErrorHandler(write_to_hdx="false")
        assert error_handler._write_to_hdx is False
        error_handler = HDXErrorHandler(write_to_hdx="FALSE")
        assert error_handler._write_to_hdx is False
        error_handler = HDXErrorHandler(write_to_hdx="n")
        assert error_handler._write_to_hdx is False
        error_handler = HDXErrorHandler(write_to_hdx=False)
        assert error_handler._write_to_hdx is False
        error_handler = HDXErrorHandler(write_to_hdx=0)
        assert error_handler._write_to_hdx is False
        with pytest.raises(SystemExit):
            with caplog.at_level(logging.ERROR):
                with HDXErrorHandler(
                    should_exit_on_error=True, write_to_hdx=False
                ) as errors:
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
