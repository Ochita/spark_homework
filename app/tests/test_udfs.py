import pandas as pd
from shared.udfs import get_year_udf
import datetime


def test_get_year_udf_str(spark_session):
    test_data = spark_session.createDataFrame(
        [(1, "2009-12-01"), (16, "2019-03-21")],
        ["id", "date"],
    )

    expected_data = spark_session.createDataFrame(
        [(1, "2009-12-01", "2009"), (16, "2019-03-21", "2019")],
        ["id", "date", "year"],
    ).toPandas()

    real_data = test_data.withColumn("year", get_year_udf("date")).toPandas()

    pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)


def test_get_year_udf_date(spark_session):
    test_data = spark_session.createDataFrame(
        [(1, datetime.date(2009, 12, 1)), (16, datetime.date(2019, 3, 21))],
        ["id", "date"],
    )

    expected_data = spark_session.createDataFrame(
        [(1, datetime.date(2009, 12, 1), "2009"), (16, datetime.date(2019, 3, 21), "2019")],
        ["id", "date", "year"],
    ).toPandas()

    real_data = test_data.withColumn("year", get_year_udf("date")).toPandas()

    pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)


def test_get_year_udf_unknown(spark_session):
    test_data = spark_session.createDataFrame(
        [(1, "2009-55-01"), (16, None)],
        ["id", "date"],
    )

    expected_data = spark_session.createDataFrame(
        [(1, "2009-55-01", "Unknown"), (16, None, "Unknown")],
        ["id", "date", "year"],
    ).toPandas()

    real_data = test_data.withColumn("year", get_year_udf("date")).toPandas()

    pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)
