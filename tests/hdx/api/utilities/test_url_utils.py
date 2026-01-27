import pytest
from requests import RequestException, Session

from hdx.api.utilities.url_utils import follow_url, get_ckan_ready_session


class TestURLUtils:
    # --- Fixtures ---

    @pytest.fixture
    def mock_configuration(self, mocker):
        config = mocker.MagicMock()
        config.get_user_agent.return_value = "test-agent"
        return config

    @pytest.fixture
    def mock_session(self, mocker):
        return mocker.MagicMock(spec=Session)

    # --- Tests for get_ckan_ready_session ---

    def test_get_ckan_ready_session_with_apikey(self, mocker, mock_configuration):
        # Setup
        mock_configuration.get_api_key.return_value = "12345"
        mock_get_session = mocker.patch("hdx.api.utilities.url_utils.get_session")

        # Execution
        get_ckan_ready_session(mock_configuration)

        # Assertion
        mock_get_session.assert_called_once_with(
            full_agent="test-agent", use_env=False, headers={"Authorization": "12345"}
        )

    def test_get_ckan_ready_session_without_apikey(self, mocker, mock_configuration):
        # Setup
        mock_configuration.get_api_key.return_value = None
        mock_get_session = mocker.patch("hdx.api.utilities.url_utils.get_session")

        # Execution
        get_ckan_ready_session(mock_configuration)

        # Assertion
        mock_get_session.assert_called_once_with(
            full_agent="test-agent", use_env=False, headers=None
        )

    # --- Tests for follow_url ---

    def test_follow_url_validation_error(self):
        """Test that ValueError is raised if neither session nor config is provided."""
        with pytest.raises(ValueError, match="Either Session or Configuration"):
            follow_url("http://example.com")

    def test_follow_url_creates_session_if_missing(self, mocker, mock_configuration):
        """Test that a session is created from config if not provided."""
        url = "http://example.com/no-match"
        mock_get_ready = mocker.patch(
            "hdx.api.utilities.url_utils.get_ckan_ready_session"
        )

        follow_url(url, configuration=mock_configuration)

        mock_get_ready.assert_called_once_with(mock_configuration)

    def test_follow_url_google_sheets_rewrite(self, mock_session):
        # Regex requires 44 chars for the ID
        sheet_id = "a" * 44

        # Case 1: With GID
        url_gid = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid=12345"
        result_gid = follow_url(url_gid, session=mock_session)
        assert (
            result_gid
            == f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=12345"
        )

        # Case 2: Without GID
        url_no_gid = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
        result_no_gid = follow_url(url_no_gid, session=mock_session)
        assert (
            result_no_gid
            == f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        )

        # Case 3: Ignored if /pub is present
        url_pub = f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub"
        assert follow_url(url_pub, session=mock_session) == url_pub

    def test_follow_url_google_drive_file_rewrite(self, mock_session):
        # Regex requires 33 chars for the XLSX ID pattern check
        file_id = "b" * 33

        # Case 1: Standard file URL
        url = f"https://drive.google.com/file/d/{file_id}/view"
        result = follow_url(url, session=mock_session)
        assert result == f"https://drive.google.com/uc?export=download&id={file_id}"

        # Case 2: XLSX Pattern Match
        url_xlsx = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
        result_xlsx = follow_url(url_xlsx, session=mock_session)
        assert (
            result_xlsx == f"https://drive.google.com/uc?export=download&id={file_id}"
        )

    def test_follow_url_dropbox_rewrite(self, mock_session):
        # Regex requires 15 lowercase alphanumeric chars
        db_id = "abcdef123456789"

        # Case: dl=0 -> dl=1
        url = f"https://www.dropbox.com/s/{db_id}/test.csv?dl=0"
        result = follow_url(url, session=mock_session)
        assert result == f"https://www.dropbox.com/s/{db_id}/test.csv?dl=1"

    def test_follow_url_google_drive_open_redirect(self, mocker, mock_session):
        """Test handling of Google Drive 'open?id=' URLs via HEAD request."""
        url = "https://drive.google.com/open?id=123xyz"
        expected_url = "https://drive.google.com/uc?id=123xyz"

        # Mock the response object
        mock_response = mocker.Mock()
        mock_response.is_redirect = True
        mock_response.headers = {"Location": expected_url}

        mock_session.head.return_value = mock_response

        result = follow_url(url, session=mock_session)

        mock_session.head.assert_called_once_with(url)
        assert result == expected_url

    def test_follow_url_request_exception(self, mock_session):
        """Test that RequestException is caught and original URL returned."""
        url = "https://drive.google.com/open?id=fail"

        mock_session.head.side_effect = RequestException("Connection Error")

        result = follow_url(url, session=mock_session)
        assert result == url

    # --- Tests for CKAN/HDX Logic ---

    def test_follow_url_ckan_resource_specific(self, mocker, mock_session):
        """Test URL rewrite when a specific resource ID is in the URL."""
        resource_id = "12345678-1234-1234-1234-123456789012"
        url = f"https://data.humdata.org/dataset/some-dataset/resource/{resource_id}"
        expected_download_url = "http://download.url/data.csv"

        # Use mocker.patch instead of @patch decorator
        mock_read_resource = mocker.patch("hdx.data.resource.Resource.read_from_hdx")
        mock_read_resource.return_value = {"url": expected_download_url}

        result = follow_url(url, session=mock_session)

        mock_read_resource.assert_called_once_with(resource_id)
        assert result == expected_download_url

    def test_follow_url_ckan_dataset_general(self, mocker, mock_session):
        """Test URL lookup when only dataset ID is provided."""
        dataset_name = "test-dataset"
        url = f"https://data.humdata.org/dataset/{dataset_name}"
        expected_download_url = "http://download.url/resource1.csv"

        # Mock Dataset object and its resources
        mock_dataset = mocker.Mock()
        mock_resource_1 = {"url": expected_download_url}
        mock_dataset.get_resources.return_value = [mock_resource_1]

        mock_read_dataset = mocker.patch("hdx.data.dataset.Dataset.read_from_hdx")
        mock_read_dataset.return_value = mock_dataset

        result = follow_url(url, session=mock_session)

        mock_read_dataset.assert_called_once_with(dataset_name)
        assert result == expected_download_url

    def test_follow_url_ckan_dataset_no_resources(self, mocker, mock_session):
        """Test CKAN logic where dataset exists but has no resource URLs."""
        url = "https://data.humdata.org/dataset/empty-dataset"

        mock_dataset = mocker.Mock()
        mock_dataset.get_resources.return_value = []  # No resources

        mock_read_dataset = mocker.patch("hdx.data.dataset.Dataset.read_from_hdx")
        mock_read_dataset.return_value = mock_dataset

        # Should fall through to returning original URL
        result = follow_url(url, session=mock_session)
        assert result == url

    def test_follow_url_no_match(self, mock_session):
        """Test that non-matching URLs are returned as-is."""
        url = "http://example.com/random-page"
        result = follow_url(url, session=mock_session)
        assert result == url
