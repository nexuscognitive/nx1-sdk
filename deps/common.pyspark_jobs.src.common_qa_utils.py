import json

import pyspark.sql.functions as f

from abc import ABC
from datetime import datetime, UTC
from typing import Optional

from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField


class MissingValuesFound(Exception):
    def __init__(self, file_path: str, partition: Optional[str] = None):
        self.file_path = file_path
        self.partition = partition
        self.message = f"Column(s) with missing values found in {file_path}"
        if partition is not None:
            self.message += f" for partition {partition}"
        super().__init__(self.message)


def schema_is_correct(spark_df: DataFrame, 
                      expected_df_schema: list[StructField]):
    if spark_df.schema.fields == expected_df_schema:
        return True
    return False


def has_missing_values(df: DataFrame, nullable_cols: list[str] = []):
    null_test = df.select(
        [f.count(f.when(f.isnan(c), c)).alias(c)
         for (c, c_type) in df.dtypes if c_type not in ("timestamp", "string", "date") and c not in nullable_cols] +
        [f.count(f.when(f.col(c).isNull(), c)).alias(c)
         for (c, c_type) in df.dtypes if c_type in ("timestamp", "string", "date") and c not in nullable_cols]
    ).collect()
    if null_test[0].count(0) == len(null_test[0]):
        return False
    return True


class QAMetadataGenerator(ABC):
    client: str
    qa_bucket: str

    @classmethod
    def get_env_attributes(cls):
        return {}

    @classmethod
    def generate_metadata_json(cls,
                               job_name: str, 
                               base_folder: str, 
                               run_start_time: datetime,
                               qa_json: dict,
                               output_key: str = None,
                               spark: SparkSession = None,):
        end_time = datetime.now(UTC)
        execution_time = round(((end_time - run_start_time).total_seconds() / 60), 2)

        # construct metadata json
        data = {
            "jobName": job_name,
            "client": cls.client
        }

        for k, v in cls.get_env_attributes().items():
            data[k] = v

        data["jobProcessingTime"] = run_start_time.strftime("%Y/%m/%d %H:%M:%S")
        data["executionTime"] = f"{execution_time} minutes"
        data["qa"] = qa_json

        # upload output to s3
        json_data = json.dumps(data, ensure_ascii=False).encode("UTF-8")
        file_run_date = run_start_time.strftime("%Y%m%d_%H%M%S")
        if output_key is None:
            output_key = f"{base_folder}/{job_name}/metadata_{file_run_date}.json"

        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        dbutils.fs.put(f"s3://{cls.qa_bucket}/{output_key}", json_data.decode())
