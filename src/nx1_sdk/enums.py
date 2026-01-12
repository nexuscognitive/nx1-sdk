"""Enumerations for the NX1 SDK."""

from enum import Enum


class IngestType(str, Enum):
    """Type of data source for ingestion."""
    FILE = "file"
    JDBC = "jdbc"
    LAKEHOUSE = "lakehouse"


class IngestMode(str, Enum):
    """Defines how data is written to the target table."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class JdbcType(str, Enum):
    """Type of JDBC data extraction."""
    TABLE = "table"
    QUERY = "query"


class JobStatus(str, Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ColumnTransformationType(str, Enum):
    """Type of column transformation to apply during ingestion."""
    CAST = "cast"
    RENAME = "rename"
    ENCRYPT = "encrypt"


class SparkDataType(str, Enum):
    """Supported Spark SQL data types for casting."""
    STRING = "string"
    INT = "int"
    INTEGER = "integer"
    LONG = "long"
    BIGINT = "bigint"
    SHORT = "short"
    SMALLINT = "smallint"
    BYTE = "byte"
    TINYINT = "tinyint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"


class AppComponentType(str, Enum):
    """Type of app component."""
    DAG = "dag"
    DEPENDENCY = "dependency"
    CONFIG = "config"


class AppVersionStatus(str, Enum):
    """Status of an app version."""
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"
