import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open

from app.services.gcs import GCS


# ---------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------
@pytest.fixture
def gcs():
    with patch("app.services.gcs.get_logger") as mock_logger:
        mock_logger.return_value = MagicMock()
        return GCS()


# ---------------------------------------------------------
# TEST: set_up_environment
# ---------------------------------------------------------
@patch("app.services.gcs.os.getenv")
@patch("app.services.gcs.load_dotenv")
def test_set_up_environment(mock_load, mock_getenv, gcs):
    mock_getenv.side_effect = ["gs://bucket/path", "/fake/creds.json"]

    gcs.set_up_environment()

    assert gcs.gcs_uri_prefix == "gs://bucket/path"
    assert gcs.gc_auth == "/fake/creds.json"


# ---------------------------------------------------------
# TEST: update_gcs_metadata
# ---------------------------------------------------------
@patch("app.services.gcs.storage.Client")
def test_update_gcs_metadata(mock_client, gcs):
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.bucket.return_value = mock_bucket

    gcs.update_gcs_metadata("gs://mybucket/file.parquet", {"k": "v"})

    mock_client.assert_called_once()
    mock_bucket.blob.assert_called_once_with("file.parquet")
    assert mock_blob.metadata == {"k": "v"}
    mock_blob.patch.assert_called_once()


# ---------------------------------------------------------
# TEST: _dataframe_hash
# ---------------------------------------------------------
def test_dataframe_hash(gcs):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    h1 = gcs._dataframe_hash(df)
    h2 = gcs._dataframe_hash(df)

    assert isinstance(h1, str)
    assert h1 == h2  # deterministic


# ---------------------------------------------------------
# TEST: _gcs_file_has_same_content
# ---------------------------------------------------------
def test_gcs_file_has_same_content_true(gcs):
    fs = MagicMock()
    fs.info.return_value = {"metadata": {"dataframe_hash": "abc"}}

    assert gcs._gcs_file_has_same_content(fs, "gs://x/y", "abc") is True


def test_gcs_file_has_same_content_false(gcs):
    fs = MagicMock()
    fs.info.return_value = {"metadata": {"dataframe_hash": "zzz"}}

    assert gcs._gcs_file_has_same_content(fs, "gs://x/y", "abc") is False


def test_gcs_file_has_same_content_missing_file(gcs):
    fs = MagicMock()
    fs.info.side_effect = FileNotFoundError

    assert gcs._gcs_file_has_same_content(fs, "gs://x/y", "abc") is False


# ---------------------------------------------------------
# TEST: _validate_csv
# ---------------------------------------------------------
@patch("app.services.gcs.SalesData")
def test_validate_csv(mock_sales, gcs):
    mock_sales.columns = ["a", "b"]
    mock_sales.convert_csv_types.return_value.to_row.return_value = (1, 2)

    fake_csv = "a,b\n1,2\n"
    with patch("builtins.open", mock_open(read_data=fake_csv)):
        df = gcs._validate_csv("fake.csv")

    assert df.shape == (1, 2)
    mock_sales.convert_csv_types.assert_called_once()
