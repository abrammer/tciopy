import numpy as np
import pandas as pd



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

