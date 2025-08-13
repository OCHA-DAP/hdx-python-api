from os.path import join

import pytest

from hdx.api.utilities.size_hash import get_size_and_hash


class TestSizeHash:
    @pytest.fixture(scope="class")
    def test_xlsx(self, fixturesfolder):
        return join(
            fixturesfolder,
            "size_hash",
            "ACLED-All-Africa-File_20170101-to-20170708.xlsx",
        )

    def test_get_size_and_hash(self, test_data, test_xlsx):
        size, hash = get_size_and_hash(test_data, "csv")
        assert size == 1548
        assert hash == "3790da698479326339fa99a074cbc1f7"

        size, hash = get_size_and_hash(test_xlsx, "xlsx")
        assert size == 23724
        assert hash == "6b8acf7e28d62685a1e829e7fa220d17"
