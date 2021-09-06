from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

from shared.udfs import get_year_udf


def _extract_expedia(spark, config):
    """ Load booking data from hdfs """
    return spark.read.format("com.databricks.spark.avro") \
        .load("hdfs://" + config['hdfs'] + config['expedia_data_path'])


def _convert_types(raw_df):
    """ Convert column types from string to date """
    return raw_df.withColumn("srch_ci", to_date("srch_ci", "yyyy-MM-dd")) \
        .withColumn("srch_co", to_date("srch_co", "yyyy-MM-dd"))


def _swap_ci_co(raw_df):
    """ Swap check-in and check-out date if check-in date later """
    return raw_df.withColumn("swap_ci_co", when(col("srch_co") < col("srch_ci"), True)) \
        .withColumn("tmp_ci", col("srch_ci")) \
        .withColumn("srch_ci", when(col("swap_ci_co"), col("srch_co")).otherwise(col("srch_ci"))) \
        .withColumn("srch_co", when(col("swap_ci_co"), col("tmp_ci")).otherwise(col("srch_co"))) \
        .drop("tmp_ci")


def _calculate_idle(raw_df):
    """ Calculate idle days """
    window_spec = Window.partitionBy("hotel_id").orderBy("srch_ci")
    return raw_df.withColumn("pred_ci", lag("srch_ci", 1).over(window_spec)) \
        .withColumn("idle_days", datediff("srch_ci", "pred_ci"))


def _extract_hotels_topic(spark, config):
    """ Load hotels messages from kafka """
    return spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", config['kafka']) \
        .option("subscribe", config['hotels_topic']) \
        .load()


def _extract_hotels_data(raw_df):
    """ Convert kafka messages"""
    schema = StructType() \
        .add("Id", StringType()) \
        .add("Name", StringType()) \
        .add("Country", StringType()) \
        .add("City", StringType()) \
        .add("Address", StringType()) \
        .add("Latitude", StringType()) \
        .add("Longitude", StringType()) \
        .add("Geohash", StringType())
    return raw_df.select(from_json(col("Value").cast("string"), schema).alias("data")) \
        .select('data.*')


def _join_hotels_data(df_exp, df_hotels):
    """ Join booking data with hotels data """
    return df_exp.join(
        df_hotels,
        df_exp.hotel_id == df_hotels.Id.cast(LongType()),
        "inner"
    ).drop(df_hotels.Id)


def _get_invalid_hotels(raw_df):
    """ List invalid hotels by condition """
    return raw_df.where((col("idle_days") > 2) & (col("idle_days") < 30)) \
        .groupBy("hotel_id").agg(max("idle_days").alias("max_idle_days"))


def _clear_invalid_data(raw_df, inv_df):
    """ Exclude invalid hotels data from bookings data """
    return raw_df.join(
        inv_df,
        raw_df.hotel_id == inv_df.hotel_id,
        "left_anti"
    )


def _show_invalid(inv_df):
    """ Show invalid hotels data """
    inv_df.select("hotel_id", "Name", "Country", "City", "Geohash", "max_idle_days").show()


def _show_stats(exp_df):
    """ Show booking counts by hotel and by city """
    exp_df.groupBy("Name").count().show()
    exp_df.groupBy("City").count().show()


def _load_data(config, transformed_df):
    """ Save data to parquet file """
    transformed_df.withColumn("year", get_year_udf("srch_ci")) \
        .write.partitionBy("year") \
        .mode("overwrite") \
        .parquet("hdfs://" + config['hdfs'] + config['output_data_path'])


def run_job(spark, config):
    """ Run job pipline """
    exp_df = _calculate_idle(_swap_ci_co(_convert_types(_extract_expedia(spark, config)))).persist()
    inv_df = _get_invalid_hotels(exp_df).persist()
    hotels_df = _extract_hotels_data(_extract_hotels_topic(spark, config)).persist()

    _show_invalid(_join_hotels_data(inv_df, hotels_df))

    cleaned_df = _join_hotels_data(_clear_invalid_data(exp_df, inv_df), hotels_df).persist()

    _show_stats(cleaned_df)
    _load_data(config, cleaned_df)
