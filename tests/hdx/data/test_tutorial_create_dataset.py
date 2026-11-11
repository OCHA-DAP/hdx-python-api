"""Tests for the code in the "Create a Dataset from a CSV" tutorial"""

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from .test_vocabulary import vocabulary_mockshow
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.vocabulary import Vocabulary

tutorial_csv = Path("documentation") / "tutorials" / "assets"
tutorial_csv = tutorial_csv / "bel_example_indicators.csv"


class TestTutorialCreateDataset:
    @pytest.fixture(scope="function")
    def vocabulary_read(self):
        Vocabulary._approved_vocabulary = None
        Vocabulary._tags_dict = None

        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                return vocabulary_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    def build_dataset(self):
        dataset = Dataset(
            {
                "name": "bel-example-indicators",
                "title": "Belgium - Example Indicators",
                "notes": "Made-up indicators used to demonstrate dataset creation.",
                "dataset_source": "Example source",
                "methodology": "Other",
                "license_id": "cc-by",
                "private": True,
                "owner_org": "00000000-0000-0000-0000-000000000000",
                "maintainer": "00000000-0000-0000-0000-000000000001",
            }
        )
        dataset.set_expected_update_frequency("Every month")
        return dataset

    def test_tutorial_dry_run(
        self, configuration, locations, vocabulary_read, tmp_path
    ):
        dataset = self.build_dataset()
        assert dataset.get_expected_update_frequency() == "Every month"
        assert dataset.add_country_location("BEL")
        assert dataset["groups"] == [{"name": "bel"}]
        assert dataset.add_tag("economics") == (["economics"], [])
        assert dataset.add_tag("not-a-real-tag") == ([], ["not-a-real-tag"])
        assert dataset.get_tags() == ["economics"]

        with open(tutorial_csv, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        added, info = dataset.generate_resource(
            tmp_path,
            "bel_example_indicators.csv",
            rows,
            {
                "name": "bel_example_indicators.csv",
                "description": "Example indicators",
            },
            headers=list(rows[0]),
            datecol="date",
        )
        assert added is True
        assert info["startdate"] == datetime(2025, 1, 31, tzinfo=UTC)
        assert info["enddate"] == datetime(2025, 2, 28, tzinfo=UTC)
        time_period = dataset.get_time_period()
        assert time_period["startdate_str"] == "2025-01-31T00:00:00+00:00"
        assert time_period["enddate_str"] == "2025-02-28T23:59:59+00:00"
        assert [r["name"] for r in dataset.get_resources()] == [
            "bel_example_indicators.csv"
        ]
        assert (tmp_path / "bel_example_indicators.csv").exists()

        dataset.check_required_fields()

        del dataset["dataset_source"]
        with pytest.raises(HDXError, match="Field dataset_source is missing"):
            dataset.check_required_fields()
        dataset["dataset_source"] = "Example source"
        dataset.check_required_fields()
        assert "dataset_source" in dataset.data
