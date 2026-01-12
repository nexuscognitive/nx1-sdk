"""Tests for NX1 SDK."""

import pytest
from unittest.mock import Mock, patch

from nx1_sdk import (
    NX1Client,
    IngestMode,
    IngestType,
    ColumnTransformation,
    SparkDataType,
    NX1ValidationError,
)


class TestColumnTransformation:
    """Tests for ColumnTransformation helper."""
    
    def test_cast_with_enum(self):
        result = ColumnTransformation.cast("date_col", SparkDataType.DATE)
        assert result == {
            "column": "date_col",
            "transformation_type": "cast",
            "target_type": "date"
        }
    
    def test_cast_with_string(self):
        result = ColumnTransformation.cast("amount", "decimal")
        assert result == {
            "column": "amount",
            "transformation_type": "cast",
            "target_type": "decimal"
        }
    
    def test_rename(self):
        result = ColumnTransformation.rename("old_name", "new_name")
        assert result == {
            "column": "old_name",
            "transformation_type": "rename",
            "new_name": "new_name"
        }
    
    def test_encrypt(self):
        result = ColumnTransformation.encrypt("ssn")
        assert result == {
            "column": "ssn",
            "transformation_type": "encrypt"
        }
    
    def test_encrypt_with_key(self):
        result = ColumnTransformation.encrypt("credit_card", encryption_key_name="pci_key")
        assert result == {
            "column": "credit_card",
            "transformation_type": "encrypt",
            "encryption_key_name": "pci_key"
        }


class TestNX1Client:
    """Tests for NX1Client initialization."""
    
    def test_init_without_api_key_raises(self):
        with pytest.raises(NX1ValidationError, match="API key required"):
            NX1Client(host="https://example.com")
    
    def test_init_without_host_raises(self):
        with pytest.raises(NX1ValidationError, match="Host URL required"):
            NX1Client(api_key="test-key")
    
    def test_init_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("NX1_API_KEY", "test-key")
        monkeypatch.setenv("NX1_HOST", "https://example.com")
        
        client = NX1Client()
        assert client.api_key == "test-key"
        assert "example.com" in client.host
    
    def test_init_with_params(self):
        client = NX1Client(api_key="test-key", host="https://example.com")
        assert client.api_key == "test-key"
        assert client.health is not None
        assert client.ingestion is not None
        assert client.apps is not None


class TestEnums:
    """Tests for SDK enums."""
    
    def test_ingest_mode_values(self):
        assert IngestMode.APPEND.value == "append"
        assert IngestMode.OVERWRITE.value == "overwrite"
        assert IngestMode.MERGE.value == "merge"
    
    def test_ingest_type_values(self):
        assert IngestType.FILE.value == "file"
        assert IngestType.JDBC.value == "jdbc"
        assert IngestType.LAKEHOUSE.value == "lakehouse"
    
    def test_spark_data_types(self):
        assert SparkDataType.STRING.value == "string"
        assert SparkDataType.DATE.value == "date"
        assert SparkDataType.TIMESTAMP.value == "timestamp"
        assert SparkDataType.DECIMAL.value == "decimal"


class TestDataIngestionClient:
    """Tests for DataIngestionClient."""
    
    def test_detect_file_format_csv(self):
        from nx1_sdk.services import DataIngestionClient
        assert DataIngestionClient.detect_file_format("data.csv") == "csv"
    
    def test_detect_file_format_parquet(self):
        from nx1_sdk.services import DataIngestionClient
        assert DataIngestionClient.detect_file_format("data.parquet") == "parquet"
    
    def test_detect_file_format_xlsx(self):
        from nx1_sdk.services import DataIngestionClient
        assert DataIngestionClient.detect_file_format("data.xlsx") == "xls"
    
    def test_detect_file_format_unsupported(self):
        from nx1_sdk.services import DataIngestionClient
        with pytest.raises(NX1ValidationError, match="Unsupported file format"):
            DataIngestionClient.detect_file_format("data.json")
    
    def test_build_file_options_csv(self):
        from nx1_sdk.services import DataIngestionClient
        options = DataIngestionClient.build_file_options(
            file_format="csv",
            delimiter="|",
            header="false"
        )
        assert options["delimiter"] == "|"
        assert options["header"] == "false"
        assert "inferSchema" in options
    
    def test_build_file_options_xls(self):
        from nx1_sdk.services import DataIngestionClient
        options = DataIngestionClient.build_file_options(
            file_format="xls",
            sheet_name="Sheet2"
        )
        assert options["sheet_name"] == "Sheet2"
        assert options["header"] == "true"
