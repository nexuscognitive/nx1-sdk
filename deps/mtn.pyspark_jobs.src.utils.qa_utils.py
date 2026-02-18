from datetime import date

import pyspark.sql.functions as f

from pyspark.sql import DataFrame

from mtn.common.utils.constants import client, qa_bucket
from common.pyspark_jobs.src.common_qa_utils import QAMetadataGenerator


class MtnQAMetadataGenerator(QAMetadataGenerator):
    client = client
    qa_bucket = qa_bucket

    @staticmethod
    def get_delayed_records(data: DataFrame, file_date: date, max_delay_days: int):
        return data.filter(
            (f.col("tbl_dt") == f.lit(file_date)) & 
            ((f.unix_timestamp(f.col("tbl_dt"), 'yyyyMMdd') - 
              f.unix_timestamp(f.col("Begin_Dt"), 'yyyyMMdd'))/(60*60*24) > max_delay_days
             )
            )