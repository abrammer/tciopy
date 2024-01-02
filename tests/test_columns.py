import pandas as pd
import numpy as np

from tciopy import converters


def test_num_column():
    col = converters.NumColumn()
    col.append("1")
    col.append("2")
    col.append("3")
    ret_series = col.pd_parse()
    exp_series = pd.Series([1, 2, 3], dtype=float)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_num_column_nan():
    col = converters.NumColumn()
    col.append("1")
    col.append("2")
    col.append("")
    col.append("nan")
    ret_series = col.pd_parse()
    exp_series = pd.Series([1, 2, np.nan, np.nan], dtype=float)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_string_column():
    col = converters.StringColumn()
    col.append("1")
    col.append("2")
    col.append("3")
    ret_series = col.pd_parse()
    exp_series = pd.Series(["1", "2", "3"], dtype=object)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_categorical_column():
    col = converters.CategoricalColumn()
    col.append("a")
    col.append("b")
    col.append("c")
    ret_series = col.pd_parse()
    exp_series = pd.Series(["a", "b", "c"], dtype="category")

    pd.testing.assert_series_equal(ret_series, exp_series)


def test_latlon_column():
    col = converters.LatLonColumn()
    col.append("100N")
    col.append("200N")
    col.append("300N")
    col.append("100S")
    col.append("200S")
    col.append("300S")

    ret_series = col.pd_parse()
    exp_series = pd.Series([10.0, 20.0, 30.0, -10, -20, -30], dtype=float)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_latlon_column():
    col = converters.LatLonColumn()
    col.append("100E")
    col.append("200E")
    col.append("300E")
    col.append("100W")
    col.append("200W")
    col.append("300W")

    ret_series = col.pd_parse()
    exp_series = pd.Series([10.0, 20.0, 30.0, -10, -20, -30], dtype=float)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_latlon_column_scaled():
    col = converters.LatLonColumn(scale=0.01)
    col.append("1001E")
    col.append("2001W")
    col.append("17310E")
    col.append("17310W")
    col.append("200W")
    col.append("300W")

    ret_series = col.pd_parse()
    exp_series = pd.Series([10.01, -20.01, 173.1, -173.1, -2, -3], dtype=float)
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_datetime_column_defaultfmt():
    col = converters.DatetimeColumn()
    col.append("2021010100")
    col.append("2021010106")
    col.append("2021010112")
    col.append("2021010118")
    col.append("2021010200")
    col.append("2021010206")

    ret_series = col.pd_parse()
    exp_series = pd.Series(
        [
            "2021-01-01 00:00:00",
            "2021-01-01 06:00:00",
            "2021-01-01 12:00:00",
            "2021-01-01 18:00:00",
            "2021-01-02 00:00:00",
            "2021-01-02 06:00:00",
        ],
        dtype="datetime64[ns]",
    )
    pd.testing.assert_series_equal(ret_series, exp_series)


def test_datetime_column_customfmt():
    col = converters.DatetimeColumn(datetime_format="%Y%m%d%H%M")
    col.append("202101010030")
    col.append("202101010630")
    col.append("202101011230")
    col.append("202101011830")
    col.append("202101020030")
    col.append("202101020630")

    ret_series = col.pd_parse()
    exp_series = pd.Series(
        [
            "2021-01-01 00:30:00",
            "2021-01-01 06:30:00",
            "2021-01-01 12:30:00",
            "2021-01-01 18:30:00",
            "2021-01-02 00:30:00",
            "2021-01-02 06:30:00",
        ],
        dtype="datetime64[ns]",
    )
    pd.testing.assert_series_equal(ret_series, exp_series)
