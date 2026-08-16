"""
Enterprise Schema Validation Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from pyspark.sql import DataFrame


class SchemaValidator:
    """
    Enterprise reusable schema validator.
    """

    ####################################################################
    # Schema Validation
    ####################################################################

    @staticmethod
    def validate_columns(
        df: DataFrame,
        expected_columns: List[str],
    ) -> Tuple[bool, List[str]]:
        """
        Validate required columns exist.
        """

        actual_columns = set(df.columns)

        missing_columns = [
            column for column in expected_columns if column not in actual_columns
        ]

        return len(missing_columns) == 0, missing_columns

    @staticmethod
    def validate_schema(
        df: DataFrame,
        expected_schema: Dict[str, str],
    ) -> Tuple[bool, List[str]]:
        """
        Validate both column names and data types.
        """

        actual_schema = {
            field.name: field.dataType.simpleString() for field in df.schema.fields
        }

        mismatches = []

        for column, datatype in expected_schema.items():

            if column not in actual_schema:

                mismatches.append(f"Missing column: {column}")

            elif actual_schema[column] != datatype:

                mismatches.append(
                    f"{column}: expected {datatype}, found {actual_schema[column]}"
                )

        return len(mismatches) == 0, mismatches

    ####################################################################
    # Schema Utilities
    ####################################################################

    @staticmethod
    def get_schema(df: DataFrame) -> Dict[str, str]:
        """
        Return schema as dictionary.
        """

        return {field.name: field.dataType.simpleString() for field in df.schema.fields}

    @staticmethod
    def compare_schema(
        source_df: DataFrame,
        target_df: DataFrame,
    ) -> Tuple[bool, List[str]]:
        """
        Compare two DataFrame schemas.
        """

        source = SchemaValidator.get_schema(source_df)

        target = SchemaValidator.get_schema(target_df)

        differences = []

        all_columns = sorted(set(source.keys()) | set(target.keys()))

        for column in all_columns:

            source_type = source.get(column)

            target_type = target.get(column)

            if source_type != target_type:

                differences.append(
                    f"{column}: source={source_type}, target={target_type}"
                )

        return len(differences) == 0, differences

    @staticmethod
    def schema_exists(
        df: DataFrame,
        column: str,
    ) -> bool:
        """
        Check whether a column exists.
        """

        return column in df.columns
