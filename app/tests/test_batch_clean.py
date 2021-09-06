import os
import pandas as pd
import datetime
from jobs import batch_hw


def test_clear_invalid(spark_session):
    print(os.environ["JAVA_HOME"])
    full_data = spark_session.createDataFrame(
        [(1, "test"), (4, "survive"), (9, "valid"), (16, "text"), (15, "normal"), (19, "data")],
        ["hotel_id", "info"],
    )

    exclude_data = spark_session.createDataFrame(
        [(1,), (16,), (19,)],
        ["hotel_id"],
    )

    expected_data = spark_session.createDataFrame(
        [(4, "survive"), (9, "valid"), (15, "normal")],
        ["hotel_id", "info"],
    ).toPandas()

    real_data = batch_hw._clear_invalid_data(full_data, exclude_data).toPandas()

    pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)


class DFFunctionsMock:
    def __init__(self):
        self.steps = []
        self.current_step = None
        self.showed = False

    def __call__(self, *args, **kwargs):
        self.steps.append((self.current_step, list(args)))
        return self

    def __getattr__(self, name):
        self.current_step = name
        return self

    def show(self):
        self.showed = True


def test_show_invalid_columns_list():
    """ Some dumb staff """
    df = DFFunctionsMock()
    batch_hw._show_invalid(df)

    assert df.showed
    assert df.steps == [('select', ['hotel_id', 'Name', 'Country', 'City', 'Geohash', 'max_idle_days'])]


def test_half_job(spark_session, mocker):
    """ Dumb code to do some staff with mocker"""
    df = spark_session.createDataFrame(
        [(1, "2009-10-01", "2009-10-05", 10), (2, "2009-10-08", "2009-10-02", 10)],
        ["id", "srch_ci", "srch_co", "hotel_id"],
    )

    result = []

    mocker.patch.object(batch_hw, "_extract_expedia")
    batch_hw._extract_expedia.return_value = df

    def save_result(xdf):
        result.append(xdf)
        return xdf

    mocker.patch.object(batch_hw, "_get_invalid_hotels", new=save_result)

    # ignore other steps
    mocker.patch.object(batch_hw, "_extract_hotels_topic")
    batch_hw._extract_expedia.return_value = df

    mocker.patch.object(batch_hw, "_extract_hotels_data", new=lambda x: x)
    mocker.patch.object(batch_hw, "_join_hotels_data", new=lambda x, y: x)

    mocker.patch.object(batch_hw, "_show_invalid", new=lambda x: x)

    mocker.patch.object(batch_hw, "_clear_invalid_data", new=lambda x, y: x)

    mocker.patch.object(batch_hw, "_show_stats", new=lambda x: x)

    mocker.patch.object(batch_hw, "_load_data", new=lambda x, y: y)

    # run job
    batch_hw.run_job(spark_session, {})
    # check result of first part
    expected_df = spark_session.createDataFrame(
        [(1, datetime.date(2009, 10, 1), datetime.date(2009, 10, 5), 10, None, None, None),
         (2, datetime.date(2009, 10, 2), datetime.date(2009, 10, 8), 10, True, datetime.date(2009, 10, 1), 1)],
        ["id", "srch_ci", "srch_co", "hotel_id", "swap_ci_co", "pred_ci", "idle_days"],
    ).toPandas()

    assert len(result) == 1
    pd.testing.assert_frame_equal(result[0].toPandas(), expected_df, check_dtype=True)
