import re
import sys
import argparse
import subprocess
from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime, timedelta
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType



def get_last_processed_date_from_output_table(spark: SparkSession, table_name: str) -> int:
    last_processed_date = None

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Unity Catalog table {table_name} not found: cannot read last processed date")
    else:
        df = spark.table(table_name)
        last_processed_date = df.agg(
            f.max(f.col('date'))
        ).collect()[0][0]
        print(f"last processed date read: {last_processed_date}")

    if last_processed_date is None:
        last_processed_date = int(datetime.min.strftime('%Y%m%d'))
        print(f"last processed date not available. Setting min value: {last_processed_silver_date}")
    else:
        last_processed_date = int(last_processed_date.strftime('%Y%m%d'))

    print(f"last processed date: {last_processed_date}")
    return last_processed_date


def data_exists_in_output_table(spark: SparkSession, table_name: str, check_date: date, date_col: str) -> bool:
    """Check if data exists in output table for a specific date."""
    if not spark.catalog.tableExists(table_name):
        return False
    
    df = spark.table(table_name)

    if date_col == "tbl_dt":
        ## convert to actual date type
        df = df.withColumn(date_col, f.to_date(f.col(date_col), "yyyyMMdd"))

    count = df.filter(f.col(date_col) == check_date).limit(1).count()
    return count > 0


def get_available_dates_from_source(spark: SparkSession, table_name: str, date_col: str) -> tuple[date, date]:
    """Get min and max dates available in source table based on tbl_dt column (event date)."""
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Unity Catalog table {table_name} not found: cannot get available dates")
    
    df = spark.table(table_name)

    if date_col == "tbl_dt":
        ## convert to actual date type
        df = df.withColumn(date_col, f.to_date(f.col(date_col), "yyyyMMdd"))
    
    # Get min and max dt values (event dates, not ingestion dates)
    min_max_result = df.agg(
        f.min(f.col(date_col)).alias('min_date'),
        f.max(f.col(date_col)).alias('max_date')
    ).collect()[0]
    
    min_dt = min_max_result['min_date']
    max_dt = min_max_result['max_date']
    
    if min_dt is None or max_dt is None:
        raise RuntimeError(f"Source table {table_name} is empty. Cannot determine available date range.")
    
    # Spark's min/max on DateType columns return Python date objects directly
    return min_dt, max_dt



def get_source_data_for_day(spark: SparkSession, check_date: date, table_name:str, date_col: str, time_window: int = None, table_exists: bool = True) -> DataFrame:
    """Fetch source data for a specific date based on source date column.
    
    Args:
        spark: SparkSession
        check_date: Date to filter by (yyyy-mm-dd)
        table_name: Name of source table e.g 'catalog.schema.table'
        date_col: Name of date column in source 
        time_window: Lookback period, for instances of aggregations - Set to None for snapshots
        table_exists: Whether table existence has already been verified (default: True)
    """
    if not table_exists and not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Unity Catalog table {table_name} not found: cannot read source data")
    
    if date_col == "tbl_dt": ## convert to actual date type from yyyyMMdd
        converted_date_col = f.to_date(f.col(date_col), "yyyyMMdd")
    else:
        converted_date_col = f.col(date_col)

    if time_window is not None:
        df = spark.table(table_name).filter(converted_date_col.between(f.lit(check_date)-timedelta(days=time_window), check_date))
    else:
        # Filter by date column in source table
        df = spark.table(table_name).filter(converted_date_col == f.lit(check_date))
    
    return df


def get_args(argv=None):
    DATE_FMT = "%Y-%m-%d"
    parser = argparse.ArgumentParser(description="Example script that accepts start/end dates")
    parser.add_argument("--start_date", default="",
                        help=f"Start date (inclusive) in {DATE_FMT} format.")
    parser.add_argument("--end_date", default="",
                        help=f"End date (inclusive) in {DATE_FMT} format.")
    parser.add_argument("--overwrite_existing", default="False",
                        help="Overwrite existing data for the specified date range.")
    parser.add_argument("--cutoff_days", type=int, default=60,
                        help="Number of days to look back when start_date and end_date are not provided (default: 60).")
    args = parser.parse_args(argv)
    return args


def parse_date(s: str) -> date:
    DATE_FMT = "%Y-%m-%d"
    try:
        return datetime.strptime(s, DATE_FMT).date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Not a valid date: '{s}'. Expected format: YYYY-MM-DD")

def is_none_empty(input_str: Optional[str]) -> bool:
    return input_str is None or input_str.strip() == ""

def generate_dates_to_process_from_args_and_source(spark:SparkSession, args, overwrite_existing: bool, table_name:str, date_col:str='tbl_dt') -> list:
    """
    Determines the list of dates to process based on CLI arguments and
    available source data.

    The function resolves the processing date range using the following rules:
    - If both start_date and end_date are provided, use them directly
      (validating start_date <= end_date).
    - If only start_date is provided, process from start_date to the
      maximum available date in the source.
    - If only end_date is provided, cap it to the maximum available date
      and calculate start_date using cutoff_days.
    - If neither date is provided, derive the range using cutoff_days
      from the maximum available date.

    Source data availability is queried only when required.
    Returns an inclusive list of dates to be processed.
    """
    # Validate that overwrite_existing requires both start_date and end_date
    if overwrite_existing and (is_none_empty(args.start_date) or is_none_empty(args.end_date)):
        raise ValueError("--overwrite_existing=True requires both --start_date and --end_date to be specified")
    
    # Calculate date range
    start_date_provided = not is_none_empty(args.start_date)
    end_date_provided = not is_none_empty(args.end_date)

    if start_date_provided and end_date_provided:
        # Both dates provided - no need to check available date range
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        if start_date > end_date:
            raise RuntimeError(f"--start date: {args.start_date} must be <= end date: {args.end_date}")
        print("Processing data based on start and end date")
    else:
        # Need to get available date range only when start_date or end_date is missing
        min_available_date, max_available_date = get_available_dates_from_source(spark, table_name, date_col)
        print(f"Available date range in source: {min_available_date} to {max_available_date}")
        
        if start_date_provided:
            # Only start_date provided - use it as start, max available date as end
            start_date = parse_date(args.start_date)
            end_date = max_available_date
            if start_date > end_date:
                raise RuntimeError(f"--start date: {args.start_date} must be <= max available date: {end_date}")
            print(f"Processing data from start_date to max available date: {start_date} to {end_date}")
        elif end_date_provided:
            # Only end_date provided - cap end_date to max available, then calculate start_date
            end_date = parse_date(args.end_date)
            # Cap end_date to max available date if it exceeds it
            if end_date > max_available_date:
                end_date = max_available_date
                print(f"Note: Provided end_date exceeded max available date, using {end_date} instead")
            # Calculate start_date: max(end_date - cutoff_days + 1, min_available_date)
            calculated_start = end_date - timedelta(days=args.cutoff_days - 1)
            start_date = max(calculated_start, min_available_date)
            print(f"Processing data based on end_date and cutoff_days: {start_date} to {end_date}")
        else:
            # Neither provided - use cutoff_days from max available date
            print(f"Processing data based on cutoff_days: {args.cutoff_days}")
            end_date = max_available_date
            # Calculate start_date: max(end_date - cutoff_days + 1, min_available_date)
            calculated_start = end_date - timedelta(days=args.cutoff_days - 1)
            start_date = max(calculated_start, min_available_date)
            print(f"Date range: {start_date} to {end_date}")
    
    # Generate list of dates to process
    dates_to_process = [start_date + timedelta(days=d) for d in range((end_date-start_date).days+1)]
    print(f"Total days to check: {len(dates_to_process)}")

    return dates_to_process


def write_to_output_table(
    spark: SparkSession,
    df: DataFrame,
    date_to_write: int, ## integer tbl_dt
    output_table: str,
    target_date_col: str,
    overwrite_existing: bool
) -> None:
    """
    Write aggregated data to the output table in a single operation.

    """
    
    # Verify table exists - fail if it doesn't (table should already exist)
    if not spark.catalog.tableExists(output_table):
        raise RuntimeError(
            f"Table {output_table} does not exist. "
            f"Cannot write data for {date_to_write}. Table must be created before running this job."
        )
    
    if overwrite_existing:
        # Use replaceWhere to overwrite only the specified dates' partitions
        print(f"Overwriting data for date: {date_to_write}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("replaceWhere", f"{target_date_col} = {date_to_write}") \
            .partitionBy(target_date_col) \
            .saveAsTable(output_table)
    else:
        # Append mode - Delta Lake will handle partition-level writes efficiently
        print(f"Appending data for date: {date_to_write}")
        df.write.format("delta") \
            .mode("append") \
            .partitionBy(target_date_col) \
            .saveAsTable(output_table)


def filter_by_lookback(df, date_col, max_date, lookback_period_days, date_format=None):
    """
    date_col: Name of the column that needs to be filtered
    max_date: string or int in the same format as date_format which represents the max date that needs to be in the data
    lookback_period_days: Integer number of days in the lookback window
    date_format: one of yyyyMMdd or "yyyy-MM-dd" 
    """
    max_date = str(max_date) ## ensure date is a string
    if date_format is None: ## detect date format when not provided
        if "-" in max_date:
            date_format = "yyyy-MM-dd"
        else:
            date_format = "yyyyMMdd"

    max_date_converted = f.to_date(f.lit(max_date), date_format)
    min_date = f.date_sub(max_date_converted, int(lookback_period_days))
    
    return df.filter(
        f.to_date(f.col(date_col).cast("string"), date_format).between(min_date, max_date_converted)
    )


def get_args_refresh(argv=None):
    DATE_FMT = "%Y-%m-%d"
    parser = argparse.ArgumentParser(description="Example script that accepts refresh date")
    parser.add_argument("--refresh_date", default="",
                        help=f"Refresh date to create the latest features in {DATE_FMT} format.")
    parser.add_argument("--overwrite_existing", default="False",
                        help="Overwrite existing data for the specified date range.")
    args = parser.parse_args(argv)
    return args
