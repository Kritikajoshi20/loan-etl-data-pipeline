from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import json

def create_spark(app_name="loan-etl"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    return spark

def fill_nulls_with_mode(df):
    cols = df.columns
    for c in cols:
        mode_row = df.groupBy(c).count().orderBy(F.desc("count")).limit(1).collect()
        if mode_row:
            mode_val = mode_row[0][0]
            df = df.withColumn(c, F.when(F.col(c).isNull(), F.lit(mode_val)).otherwise(F.col(c)))
    return df

def split_timestamp(df, timestamp_col="timestamp"):
    # try common formats; create parsed_ts then separate
    df = df.withColumn("parsed_ts", F.coalesce(
        F.to_timestamp(F.col(timestamp_col), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(timestamp_col), "MM/dd/yyyy HH:mm:ss"),
        F.to_timestamp(F.col(timestamp_col), "dd-MM-yyyy HH:mm:ss"),
    ))
    df = df.withColumn("date", F.date_format("parsed_ts", "yyyy-MM-dd"))
    df = df.withColumn("time", F.date_format("parsed_ts", "HH:mm:ss"))
    df = df.drop("parsed_ts")
    return df

def generate_insights(df):
    # example: loan count, avg loan amount per type
    insights = {}
    insights['total_loans'] = df.count()
    # numeric columns - example column 'loan_amount' adjust if different
    if 'loan_amount' in df.columns:
        insights['avg_loan_amount'] = df.select(F.mean(F.col('loan_amount'))).collect()[0][0]
    # group by example
    if 'loan_type' in df.columns:
        agg = df.groupBy('loan_type').agg(F.count('*').alias('count')).toPandas().to_dict(orient='records')
        insights['by_loan_type'] = agg
    return insights

def run_etl(input_path, output_path, timestamp_col="timestamp"):
    spark = create_spark()
    # support CSV or Excel - for Excel we require it to be pre-converted to CSV by the Drive download step
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df_clean = fill_nulls_with_mode(df)
    if timestamp_col in df_clean.columns:
        df_clean = split_timestamp(df_clean, timestamp_col)
    # write parquet to s3a (MinIO)
    df_clean.write.mode("overwrite").parquet(output_path)
    insights = generate_insights(df_clean)
    # Persist insights as JSON locally for QA (simple)
    import json, os
    os.makedirs("/opt/etl/insights", exist_ok=True)
    with open("/opt/etl/insights/insights.json", "w") as f:
        json.dump(insights, f, indent=2)
    spark.stop()
    return insights

if __name__ == "__main__":
    # called directly by bash if necessary
    in_path = sys.argv[1]
    out_path = sys.argv[2]
    ts_col = sys.argv[3] if len(sys.argv) > 3 else "timestamp"
    print(run_etl(in_path, out_path, ts_col))
