"""Column transformation helpers for data ingestion."""

from typing import Any, Dict, Optional, Union

from nx1_sdk.enums import SparkDataType


class ColumnTransformation:
    """
    Helper class for building column transformations during ingestion.
    
    Supports three transformation types:
    - cast: Convert column to a different data type
    - rename: Rename a column
    - encrypt: Encrypt column values using vault_encrypt UDF
    
    Usage:
        from nx1_sdk import ColumnTransformation, SparkDataType
        
        transformations = [
            ColumnTransformation.cast("hire_date", SparkDataType.DATE),
            ColumnTransformation.rename("emp_id", "employee_id"),
            ColumnTransformation.encrypt("ssn"),
        ]
        
        client.ingestion.ingest_local_file(
            file_path="data.csv",
            table="employees",
            schema_name="hr",
            column_transformations=transformations
        )
    """
    
    @staticmethod
    def cast(
        column: str,
        target_type: Union[str, SparkDataType]
    ) -> Dict[str, Any]:
        """
        Create a cast transformation to convert a column to a different type.
        
        Args:
            column: Name of the column to cast
            target_type: Target Spark SQL data type (string or SparkDataType enum)
        
        Returns:
            Transformation dictionary
        
        Example:
            ColumnTransformation.cast("amount", SparkDataType.DECIMAL)
            ColumnTransformation.cast("created_at", "timestamp")
        """
        return {
            "column": column,
            "transformation_type": "cast",
            "target_type": target_type.value if isinstance(target_type, SparkDataType) else target_type
        }
    
    @staticmethod
    def rename(column: str, new_name: str) -> Dict[str, Any]:
        """
        Create a rename transformation to change a column's name.
        
        Args:
            column: Current name of the column
            new_name: New name for the column
        
        Returns:
            Transformation dictionary
        
        Example:
            ColumnTransformation.rename("emp_id", "employee_id")
        """
        return {
            "column": column,
            "transformation_type": "rename",
            "new_name": new_name
        }
    
    @staticmethod
    def encrypt(
        column: str,
        encryption_key_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create an encrypt transformation to encrypt column values.
        
        Uses the vault_encrypt UDF to encrypt sensitive data.
        
        Args:
            column: Name of the column to encrypt
            encryption_key_name: Optional name of the encryption key in vault
        
        Returns:
            Transformation dictionary
        
        Example:
            ColumnTransformation.encrypt("ssn")
            ColumnTransformation.encrypt("credit_card", encryption_key_name="pci_key")
        """
        transform: Dict[str, Any] = {
            "column": column,
            "transformation_type": "encrypt"
        }
        if encryption_key_name:
            transform["encryption_key_name"] = encryption_key_name
        return transform
