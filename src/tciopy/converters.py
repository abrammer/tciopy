""" ATCF Converter classes for parsing ATCF data into pandas Series 
    Rather than converting each value seperately, these classes accumulate input data into a list,
    and then convert the list to a pandas Series when pd_parse() is called.
    This allows for more efficient parsing of the ATCF data.
"""

# Note: For some reason it is faster to convert to a np.array as an intermediate step...

import numpy as np
import pandas as pd


class StringColumn(list):
    "Default parser list to hold a list of strings"

    def pd_parse(self) -> pd.Series:
        "Return a pandas series of the list"
        return pd.Series(self)


class NumericColumn(list):
    "Numeric Parser, will convert empty strings to NaNs and scale output by a optional scale factor"

    def __init__(self, *args, scale=1):
        super().__init__(args)
        self.scale = scale

    def pd_parse(self) -> pd.Series:
        "Return a pandas series of the list, converting empty strings to NaNs and scaling output"
        x = np.array(self, dtype=object)
        mask = (x == "") | (x == "nan")
        z = np.empty(
            x.shape,
            dtype=float,
        )
        z[~mask] = x[~mask].astype(float)
        z[mask] = np.nan
        return pd.Series(z * self.scale, dtype=float)


class CategoricalColumn(list):
    "Categorical Parser list"
    def pd_parse(self) -> pd.Series:
        "Return a pandas series, dtype='category'"
        return pd.Series(np.array(self, dtype=object), dtype="category")


class LatLonColumn(list):
    """LatLon Parser list, with optional scale factor.
       Converts list of ATCF strings to numerical value,
       default scaling is 0.1, so 100N becomes 10.0. 
       scale kwarg can be used to change this.
       """
    def __init__(self, *args, scale=0.1):
        super().__init__(args)
        self.scale = scale

    def pd_parse(self) -> pd.Series:
        "Return a pandas series of numerical lat lon values [degrees]"
        series = pd.Series(np.array(self, dtype=object))
        hemisign = 1 - series.str.endswith(("W", "S")) * 2
        ll = NumericColumn(*series.str[:-1]).pd_parse() * hemisign
        return ll * self.scale


class DatetimeColumn(list):
    """Datetime Parser list, with optional datetime_format kwarg.
       Converts list of ATCF strings to datetime objects,
       default datetime_format is '%Y%m%d%H', so 2019010100 becomes datetime.datetime(2019, 1, 1, 0, 0).
       datetime_format kwarg can be used to change this.
       """
    def __init__(self, *args, datetime_format="%Y%m%d%H"):
        super().__init__(args)
        self.datetime_format = datetime_format

    def pd_parse(self) -> pd.Series:
        "Return a pandas series of datetimes"
        return pd.to_datetime(pd.Series(self), format=self.datetime_format)


class int_converter:
    def __init__(self, scale=1):
        self.scale = scale

    def __call__(self, x):
        mask = (x == "") | (x == "nan")
        z = np.empty(
            x.shape,
            dtype=float,
        )
        z[~mask] = x[~mask].astype(float)
        z[mask] = np.nan
        return z * self.scale


class latlonconverter:
    def __init__(self, scale):
        self.scale = scale

    def __call__(self, series):
        series = pd.Series(series)
        hemisign = 1 - series.str.endswith(("W", "S")) * 2
        ll = int_converter()(series.str[:-1]) * hemisign
        return pd.Series(ll * self.scale)


class datetimeconverter:
    def __init__(self, datetime_format="%Y%m%d%H"):
        self.datetime_format = datetime_format

    def __call__(self, series):
        return pd.to_datetime(series, format=self.datetime_format)


class categoricalconverter:
    def __init__(self,):
        pass

    def __call__(self, series):
        return pd.Categorical(series)

