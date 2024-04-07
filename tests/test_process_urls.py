from unittest.mock import patch

import pandas as pd
import pytest

from assignment2 import process_urls


@pytest.fixture
def csv_file(tmp_path):
    data = pd.DataFrame(
        ["http://example.com/test1.pdf", "http://example.com/test2.pdf"]
    )
    file_path = tmp_path / "test_urls.csv"
    data.to_csv(file_path, index=False, header=False)
    return str(file_path)


@pytest.fixture
def mock_download_pdf(mocker):
    return mocker.patch("assignment2.download_pdf", return_value="test_pdf_path.pdf")


def test_process_urls(csv_file, mock_download_pdf):
    """Tests that process_urls correctly processes each URL from the CSV file."""

    result_df = process_urls(csv_file)

    assert mock_download_pdf.call_count == 2

    assert result_df.empty
