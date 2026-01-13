"""
NX1 SDK - Comprehensive Python Client for the NX1 NLP Agent API

Usage:
    from nx1_sdk import NX1Client, IngestMode, ColumnTransformation
    
    client = NX1Client(api_key="your-key", host="https://api.example.com")
    client.health.ping()
    
    # Or use profiles (~/.nx1/profiles)
    client = NX1Client(profile="dev")
"""

from nx1_sdk.client import NX1Client, create_client
from nx1_sdk.enums import (
    IngestType,
    IngestMode,
    JdbcType,
    JobStatus,
    ColumnTransformationType,
    SparkDataType,
    AppComponentType,
    AppVersionStatus,
)
from nx1_sdk.transformations import ColumnTransformation
from nx1_sdk.exceptions import (
    NX1Error,
    NX1APIError,
    NX1ValidationError,
    NX1TimeoutError,
)
from nx1_sdk.constants import FILE_FORMAT_OPTIONS, FILE_EXTENSION_MAP, CONTENT_TYPES
from nx1_sdk.profiles import (
    load_profiles,
    get_profile,
    list_profiles,
    save_profile,
    delete_profile,
    resolve_config,
)

__version__ = "1.0.0"
__author__ = "NX1 Team"
__email__ = "support@nx1cloud.com"

__all__ = [
    # Main client
    "NX1Client",
    "create_client",
    # Enums
    "IngestType",
    "IngestMode",
    "JdbcType",
    "JobStatus",
    "ColumnTransformationType",
    "SparkDataType",
    "AppComponentType",
    "AppVersionStatus",
    # Helpers
    "ColumnTransformation",
    # Exceptions
    "NX1Error",
    "NX1APIError",
    "NX1ValidationError",
    "NX1TimeoutError",
    # Constants
    "FILE_FORMAT_OPTIONS",
    "FILE_EXTENSION_MAP",
    "CONTENT_TYPES",
    # Profiles
    "load_profiles",
    "get_profile",
    "list_profiles",
    "save_profile",
    "delete_profile",
    "resolve_config",
    # Metadata
    "__version__",
]
