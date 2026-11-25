import argparse
import re
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F, types as T


ARCHIVE_SCHEMA = T.StructType([T.StructField("value_base64", T.StringType())])
TRANSFER_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("from_account_id", T.StringType()),
        T.StructField("to_account_id", T.StringType()),
        T.StructField("time", T.StringType()),
        T.StructField("amount_kopecks", T.LongType()),
    ]
)
RFC3339_MILLIS = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3}0*)?(?:Z|[+-]\d{2}:\d{2})$"
RUN_ID = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,18}[a-z0-9])?$")


def daily_recipients(spark, archive, feature_date):
    start = datetime.combine(feature_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    envelopes = spark.read.schema(ARCHIVE_SCHEMA).json(
        f"{archive.rstrip('/')}/partition-*/*.json"
    )
    transfers = (
        envelopes.select(
            F.from_json(
                F.expr("try_to_binary(value_base64, 'base64')").cast("string"),
                TRANSFER_SCHEMA,
            ).alias("transfer")
        )
        .select("transfer.*")
        .withColumn("occurred_at", F.try_to_timestamp("time"))
        .where(
            (F.length(F.trim("transaction_id")) > 0)
            & (F.length(F.trim("from_account_id")) > 0)
            & (F.length(F.trim("to_account_id")) > 0)
            & (F.col("amount_kopecks") > 0)
            & F.col("time").rlike(RFC3339_MILLIS)
            & F.col("occurred_at").isNotNull()
        )
        .drop("time")
        .dropDuplicates()
        .persist()
    )

    in_window = (F.col("occurred_at") >= F.lit(start)) & (F.col("occurred_at") < F.lit(end))
    conflicts = (
        transfers.groupBy("transaction_id")
        .agg(F.count("*").alias("variants"), F.max(in_window.cast("int")).alias("in_window"))
        .where((F.col("variants") > 1) & (F.col("in_window") == 1))
    )
    if conflicts.take(1):
        raise ValueError(f"Conflicting transfers with the same transaction_id for {feature_date}")

    return (
        transfers.where(in_window & (F.col("from_account_id") != F.col("to_account_id")))
        .groupBy(F.col("from_account_id").alias("account_id"))
        .agg(F.countDistinct("to_account_id").alias("unique_recipients"))
        .select(F.lit(feature_date).alias("feature_date"), "account_id", "unique_recipients")
    )


def publish(spark, args):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path
    root = f"{args.features.rstrip('/')}/unique-recipients"
    target = path(f"{root}/{args.date.isoformat()}/{args.run_id}")
    success = path(target, "_SUCCESS")
    fs = target.getFileSystem(conf)
    if fs.exists(success):
        print(f"Already published {target}", flush=True)
        return
    if fs.exists(target):
        raise RuntimeError(f"Output exists without _SUCCESS: {target}")

    staging = path(f"{root}/_staging/{args.run_id}-{uuid4().hex}")
    try:
        result = daily_recipients(spark, args.archive, args.date).persist()
        result.write.mode("error").parquet(staging.toString())
        if not fs.exists(path(staging, "_SUCCESS")):
            raise RuntimeError(f"Spark did not complete the staged result: {staging}")

        # Keep the serving file hidden from Spark's normal Parquet directory scan.
        json_dir = path(staging, "_serving")
        result.select("account_id", "unique_recipients").coalesce(1).write.json(json_dir.toString())
        output = path(staging, "_recipients.jsonl")
        files = [
            status.getPath()
            for status in fs.listStatus(json_dir)
            if status.isFile() and status.getPath().getName().endswith(".json")
        ]
        if not files:
            fs.create(output, False).close()
        elif len(files) != 1 or not fs.rename(files[0], output):
            raise RuntimeError(f"Could not prepare the serving file: {output}")
        if not fs.delete(json_dir, True):
            raise RuntimeError(f"Could not remove temporary JSON directory: {json_dir}")
        fs.setTimes(staging, int(datetime.now(timezone.utc).timestamp() * 1000), -1)

        fs.mkdirs(target.getParent())
        file_context = jvm.org.apache.hadoop.fs.FileContext.getFileContext(target.toUri(), conf)
        rename = jvm.org.apache.hadoop.fs.Options.Rename
        options = spark.sparkContext._gateway.new_array(rename, 1)
        options[0] = rename.NONE
        file_context.rename(staging, target, options)
        print(f"Published {target}", flush=True)
    finally:
        fs.delete(staging, True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, help="HDFS path to the Kafka topic archive")
    parser.add_argument("--features", required=True, help="HDFS root for published features")
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if not RUN_ID.fullmatch(args.run_id):
        parser.error("--run-id must contain 1–20 lowercase letters, digits or internal dashes")

    spark = (
        SparkSession.builder.appName("daily-unique-recipients")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        publish(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
