import pytest
import time
from unittest.mock import MagicMock, patch

from app.models.benchmarking_report import (
    BenchmarkData,
    get_function_duration,
)


# ---------------------------------------------------------
# Test BenchmarkData._calculate_disk_space_savings
# ---------------------------------------------------------
def test_calculate_disk_space_savings():
    b = BenchmarkData()
    b.csv_size = 100
    b.parquet_size = 40

    assert b._calculate_disk_space_savings() == pytest.approx(60.0)


# ---------------------------------------------------------
# Test add_to_csv_size
# ---------------------------------------------------------
def test_add_to_csv_size():
    b = BenchmarkData()
    b.add_to_csv_size(500)
    b.add_to_csv_size(250)

    assert b.csv_size == 750


# ---------------------------------------------------------
# Test add_to_parquet_size (mocking fsspec)
# ---------------------------------------------------------
@patch("app.models.benchmarking_report.fsspec.filesystem")
def test_add_to_parquet_size(mock_fs):
    mock_fs.return_value.info.return_value = {"size": 1234}

    b = BenchmarkData()
    b.add_to_parquet_size("gs://bucket/file.parquet")

    assert b.parquet_size == 1234
    mock_fs.assert_called_once_with("gcs")
    mock_fs.return_value.info.assert_called_once()


# ---------------------------------------------------------
# Test get_function_duration decorator for upload speed
# ---------------------------------------------------------
def test_get_function_duration_upload_speed():
    b = BenchmarkData()

    @get_function_duration(b, flag="u")
    def fake_upload():
        time.sleep(0.01)
        return "done"

    result = fake_upload()

    assert result == "done"
    assert b.upload_speed > 0
    assert b.query_access_duration == 0


# ---------------------------------------------------------
# Test get_function_duration decorator for query duration
# ---------------------------------------------------------
def test_get_function_duration_query_duration():
    b = BenchmarkData()

    @get_function_duration(b, flag="q")
    def fake_query():
        time.sleep(0.01)
        return "ok"

    result = fake_query()

    assert result == "ok"
    assert b.query_access_duration > 0
    assert b.upload_speed == 0


# ---------------------------------------------------------
# Test create_audit_log (mock logger)
# ---------------------------------------------------------
@patch("app.models.benchmarking_report.get_logger")
def test_create_audit_log(mock_logger):
    mock_logger.return_value = MagicMock()

    b = BenchmarkData()
    b.csv_size = 1000000
    b.parquet_size = 500000
    b.upload_speed = 1.23
    b.query_access_duration = 0.45

    b.create_audit_log()

    # Ensure logger was created
    mock_logger.assert_called_once()

    # Ensure .info() was called with a report string
    logged_text = mock_logger.return_value.info.call_args[0][0]
    assert "BENCHMARKING REPORT" in logged_text
    assert "Disk Space Savings" in logged_text
    assert "Upload Speed" in logged_text
    assert "Query Access Duration" in logged_text
