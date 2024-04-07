import os
from unittest.mock import patch

import pytest

from assignment2 import download_pdf


@pytest.fixture
def mock_requests_get(mocker):
    """Mocks the requests.get call to return a mock response with dummy content."""

    class MockResponse:
        def __init__(self, content):
            self.content = content

    return mocker.patch("requests.get", return_value=MockResponse(b"Dummy PDF content"))


def test_download_pdf(tmp_path, mock_requests_get):
    test_url = "http://example.com/test.pdf"
    expected_filename = "test.pdf"
    expected_content = b"Dummy PDF content"

    old_cwd = os.getcwd()

    docs_path = tmp_path / "docs"
    docs_path.mkdir()

    os.chdir(tmp_path)

    try:

        actual_filename = download_pdf(test_url)

        assert actual_filename == expected_filename

        expected_filepath = docs_path / expected_filename
        assert expected_filepath.is_file()

        with open(expected_filepath, "rb") as f:
            assert f.read() == expected_content

    finally:

        os.chdir(old_cwd)
