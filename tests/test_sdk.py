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


class TestCredentialsClient:
    """Tests for CredentialsClient auth-header resolution and routing.

    `psk` uses a sentinel default, so three cases must stay distinguishable:
    not supplied (use the client's configured PSK), explicitly None (send no
    PSK header), and an explicit value. Collapsing None into "not supplied"
    would silently make an unauthenticated request authenticated.
    """

    def _client(self):
        from nx1_sdk.services.nx1_service import CredentialsClient
        return CredentialsClient(Mock())

    def test_auth_headers_default_leaves_configured_psk_alone(self):
        # None, not {} — BaseClient must not be handed an override at all.
        assert self._client()._auth_headers() is None

    def test_auth_headers_explicit_none_marks_psk_for_removal(self):
        headers = self._client()._auth_headers(psk=None)
        assert headers == {"Authorization-PSK": None}

    def test_auth_headers_explicit_psk_overrides(self):
        headers = self._client()._auth_headers(psk="other-psk")
        assert headers == {"Authorization-PSK": "other-psk"}

    def test_auth_headers_token_sends_bearer(self):
        headers = self._client()._auth_headers(token="tok123")
        assert headers == {"Authorization": "Bearer tok123"}

    def test_auth_headers_token_and_psk_removal_combine(self):
        headers = self._client()._auth_headers(psk=None, token="tok123")
        assert headers == {
            "Authorization-PSK": None,
            "Authorization": "Bearer tok123",
        }

    def test_vend_s3_with_bucket_routes_to_bucket_path(self):
        client = self._client()
        client.vend_s3("my-bucket")
        client._client.get.assert_called_once_with(
            "api", "s3", "credentials", "my-bucket", headers=None
        )

    def test_vend_s3_without_bucket_routes_to_default_path(self):
        client = self._client()
        client.vend_s3()
        client._client.get.assert_called_once_with(
            "api", "s3", "credentials", headers=None
        )

    def test_vend_s3_passes_auth_override_through(self):
        client = self._client()
        client.vend_s3("my-bucket", psk=None)
        _, kwargs = client._client.get.call_args
        assert kwargs["headers"] == {"Authorization-PSK": None}

    def test_whoami_routes_to_identity_path(self):
        client = self._client()
        client.whoami()
        client._client.get.assert_called_once_with(
            "api", "identity", "whoami", headers=None
        )


class TestBaseClientHeaders:
    """Tests for header merging in BaseClient._request.

    A None value removes the header. Every caller depends on this, so it is
    tested independently of the client that motivated it.
    """

    def _sent_headers(self, headers=None):
        """Return the headers BaseClient actually handed to requests."""
        from nx1_sdk.base import BaseClient

        response = Mock(status_code=200, content=b"{}")
        response.raise_for_status.return_value = None
        response.json.return_value = {}

        client = BaseClient(api_key="configured-psk", host="https://example.invalid")
        with patch("nx1_sdk.base.requests.request", return_value=response) as request:
            client.get("api", "thing", headers=headers)
        return request.call_args.kwargs["headers"]

    def test_default_headers_are_sent(self):
        sent = self._sent_headers()
        assert sent["Authorization-PSK"] == "configured-psk"
        assert sent["Content-Type"] == "application/json"

    def test_override_replaces_a_default_header(self):
        sent = self._sent_headers({"Authorization-PSK": "other-psk"})
        assert sent["Authorization-PSK"] == "other-psk"

    def test_none_value_removes_the_header(self):
        sent = self._sent_headers({"Authorization-PSK": None})
        assert "Authorization-PSK" not in sent
        # Unrelated defaults must survive the filtering.
        assert sent["Content-Type"] == "application/json"

    def test_added_header_coexists_with_defaults(self):
        sent = self._sent_headers({"Authorization": "Bearer tok123"})
        assert sent["Authorization"] == "Bearer tok123"
        assert sent["Authorization-PSK"] == "configured-psk"
