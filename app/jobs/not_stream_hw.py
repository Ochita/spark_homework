from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from batch_hw import _extract_hotels_topic, _extract_hotels_data, _join_hotels_data, _extract_expedia, \
    _convert_types, _swap_ci_co
from shared.udfs import get_array_udf, get_geohash_udf


# def _extract_weather_topic(spark, config):
#     """ Load weather messages from kafka """
#     return spark.read.format("kafka") \
#         .option("kafka.bootstrap.servers", config['kafka']) \
#         .option("subscribe", config['weather_topic']) \
#         .load()
#
#
# def _extract_weather_data(raw_df):
#     """ Convert kafka messages"""
#     schema = StructType() \
#         .add("geohash", StringType()) \
#         .add("lng", DoubleType()) \
#         .add("lat", DoubleType()) \
#         .add("avg_tmpr_f", DoubleType()) \
#         .add("avg_tmpr_c", DoubleType()) \
#         .add("wthr_date", StringType()) \
#         .add("year", StringType()) \
#         .add("month", StringType()) \
#         .add("day", StringType())
#     return raw_df.select(from_json(col("Value").cast("string"), schema).alias("data")) \
#         .select('data.*')

def _extract_weather_data(spark, config):
    return spark.read.option("mergeSchema", "true") \
        .parquet("hdfs://" + config['hdfs'] + config["weather_data_path"]) \
        .withColumn("geohash", get_geohash_udf('lat', 'lng'))


def _group_weather_data(raw_df):
    """ Convert kafka messages"""
    return raw_df.groupBy("geohash", "wthr_date") \
        .agg(avg("avg_tmpr_f").alias("avg_tmpr_f"), avg("avg_tmpr_c").alias("avg_tmpr_c"),
             first(raw_df.year).alias("year"), first(raw_df.month).alias("month"), first(raw_df.day).alias("day"))


def _join_geohash_weather_data(df_hotels, df_weather):
    """ Join booking data with hotels data """
    return df_hotels.join(
        df_weather,
        "geohash",
        "left_outer"
    )


def _fiter_by_tmpr(raw_df):
    return raw_df.where(col("avg_tmpr_c") > 0)


def _explode_dates(raw_df):
    return raw_df.withColumn("days", get_array_udf(datediff("srch_co", "srch_ci"))) \
        .select("*", explode("days").alias("stayedDay")) \
        .withColumn("days", size(col("days"))) \
        .withColumn("stayedDay", expr("date_add(srch_ci, stayedDay)"))
    # date_add(col("srch_ci"), col("stayedDay")) by itself not worked cause waiting lit in second argument till v3.0


def _calc_avg_temp(raw_df):
    return raw_df.groupBy("id").agg(first(col("days")).alias("days"), avg("avg_tmpr_c").alias("avg_tmpr_c"),
                                    avg("avg_tmpr_f").alias("avg_tmpr_f"))


def _join_temps(avg_tmpr_df, exp_df):
    return avg_tmpr_df.join(
        exp_df,
        "id",
        "inner"
    )


def _add_stay_type(raw_df):
    return raw_df.withColumn("stayType", when(col("days") == 1, "short_stay")
                             .when((col("days") >= 2) & (col("days") < 7), "standard_stay")
                             .when((col("days") >= 7) & (col("days") < 14), "standard_extended_stay")
                             .when((col("days") >= 14) & (col("days") <= 30), "long_stay")
                             .otherwise("erroneous_data")
                             )


def _add_children_flag(raw_df):
    return raw_df.withColumn("with_children", when((col("srch_children_cnt") > 0), True).otherwise(False))


def _calc_stats(raw_df):
    window_spec = Window.partitionBy("Name")
    name_grp_df = raw_df.groupBy("Name", "stayType").agg(
            max(col("with_children")).alias("with_children"),
            count(col("stayType")).alias("count")
        ).withColumn("max_stay_count", max("count").over(window_spec)) \
        .withColumn("with_children", max("with_children").over(window_spec))

    pivot = name_grp_df.groupBy("Name", "with_children").pivot("stayType",  # boost performance by list possible values
                                                               ["short_stay", "standard_stay",
                                                                "standard_extended_stay", "long_stay",
                                                                "erroneous_data"]) \
        .agg(first(col("count"))).fill(0)  # replace nulls by zeros
    return pivot.join(
        name_grp_df.select("Name", col("stayType").alias("most_popular_stay_type"))
        .where(col("count") == col("max_stay_count")),
        "Name",
        "inner"
    )


def _load_data(result_df, config):
    """ Save data to csv file """
    result_df.select(col("Name").alias('name'),
                     "with_children",
                     col("erroneous_data").alias("erroneous_data_cnt"),
                     col("short_stay").alias("short_stay_cnt"),
                     col("standard_stay").alias("standard_stay_cnt"),
                     col("standard_extended_stay").alias("standard_extended_stay_cnt"),
                     col("long_stay").alias("long_stay_cnt"),
                     "most_popular_stay_type") \
        .write.mode("overwrite") \
        .csv("hdfs://" + config['hdfs'] + config['stats_path'])


def run_job(spark, config):
    """ Run job pipline """
    exp_df = _swap_ci_co(_convert_types(_extract_expedia(spark, config)))
    hotels_df = _extract_hotels_data(_extract_hotels_topic(spark, config))
    hotels_exp_dff = _join_hotels_data(exp_df, hotels_df).persist()  # persist before double use
    big_df = _explode_dates(hotels_exp_dff)
    weather_df = _group_weather_data(_extract_weather_data(spark, config))
    hotel_weather_df = _join_geohash_weather_data(big_df, weather_df)
    avg_tmp_df = _fiter_by_tmpr(_calc_avg_temp(hotel_weather_df))
    tmpr_exp_df = _join_temps(avg_tmp_df, hotels_exp_dff)
    extended_exp_df = _add_children_flag(_add_stay_type(tmpr_exp_df)).persist()  # persist before multiple uses
    result_df = _calc_stats(extended_exp_df)
    result_df.show(50)
    # _load_data(result_df, config)
