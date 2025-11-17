""" ATCF Converter classes for parsing ATCF data into polars Series 
    Rather than converting each value seperately, these classes accumulate input data into a list,
    and then convert the list to a polars Series when pd_parse() is called.
    This allows for more efficient parsing of the ATCF data.
"""

# Note: For some reason it is faster to convert to a np.array as an intermediate step...

import numpy as np
import polars as pl


def tolatlon(dataframe: pl.DataFrame,
            lat: str = 'lat',
            lon: str = 'lon',
            scale:float = 0.1) -> pl.DataFrame:
    """Convert lat/lon columns to numerical values in degrees"""
    latnum = pl.col(lat).str.strip_chars('NS ').cast(pl.Float64, strict=False) * scale  # 64 is overkill but 32 creating floating point weirdness
    lonnum = pl.col(lon).str.strip_chars('EW ').cast(pl.Float64, strict=False) * scale
    return dataframe.with_columns([
            pl.when(pl.col(lat).str.ends_with('S'))
            .then(latnum * -1.0)
            .otherwise(latnum),
            pl.when(pl.col(lon).str.ends_with('W'))
            .then(lonnum * -1.0)
            .otherwise(lonnum)
    ])




class StringColumn(list):
    "Default parser list to hold a list of strings"

    def pd_parse(self) -> pl.Series:
        "Return a polars series of the list"
        return pl.Series(self, dtype=pl.String)


class NumericColumn(list):
    "Numeric Parser, will convert empty strings to NaNs and scale output by a optional scale factor"

    def __init__(self, *args, scale=1):
        super().__init__(args)
        self.scale = scale

    def pd_parse(self) -> pl.Series:
        "Return a polars series of the list, converting empty strings to NaNs and scaling output"
        x = np.array(self, dtype=object)
        mask = (x == "") | (x == "nan")
        z = np.empty(
            x.shape,
            dtype=float,
        )
        z[~mask] = x[~mask].astype(float)
        z[mask] = np.nan
        return pl.Series(z * self.scale, dtype=pl.Float64)


class CategoricalColumn(list):
    "Categorical Parser list"
    def pd_parse(self) -> pl.Series:
        "Return a polars series, dtype='Categorical'"
        return pl.Series(np.array(self, dtype=object), dtype=pl.Categorical)


class LatLonColumn(list):
    """LatLon Parser list, with optional scale factor.
       Converts list of ATCF strings to numerical value,
       default scaling is 0.1, so 100N becomes 10.0. 
       scale kwarg can be used to change this.
       """
    def __init__(self, *args, scale=0.1):
        super().__init__(args)
        self.scale = scale

    def pd_parse(self) -> pl.Series:
        "Return a polars series of numerical lat lon values [degrees]"
        # Create a dataframe to use with_columns expressions
        df = pl.DataFrame({"raw": np.array(self, dtype=object)})
        df = df.with_columns([
            pl.col("raw").cast(pl.String).alias("raw")
        ])
        df = df.with_columns([
            pl.col("raw").str.head(-1).cast(pl.Float64, strict=False).alias("num"),
            (pl.col("raw").str.ends_with("W") | pl.col("raw").str.ends_with("S")).alias("is_negative")
        ])
        df = df.with_columns([
            pl.when(pl.col("is_negative"))
            .then(pl.col("num") * -1.0 * self.scale)
            .otherwise(pl.col("num") * self.scale)
            .alias("result")
        ])
        return df["result"]


class DatetimeColumn(list):
    """Datetime Parser list, with optional datetime_format kwarg.
       Converts list of ATCF strings to datetime objects,
       default datetime_format is '%Y%m%d%H', so 2019010100 becomes datetime.datetime(2019, 1, 1, 0, 0).
       datetime_format kwarg can be used to change this.
       """
    def __init__(self, *args, datetime_format="%Y%m%d%H"):
        super().__init__(args)
        self.datetime_format = datetime_format

    def pd_parse(self) -> pl.Series:
        "Return a polars series of datetimes"
        s = pl.Series(self, dtype=pl.String)
        # Polars requires both hour and minute, so if format doesn't have %M, append '00' for minutes
        if '%M' not in self.datetime_format:
            s = s + '00'
            format_with_minutes = self.datetime_format + '%M'
        else:
            format_with_minutes = self.datetime_format
        return s.str.strptime(pl.Datetime, format=format_with_minutes, strict=False)


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
        df = pl.DataFrame({"raw": series})
        df = df.with_columns([
            pl.col("raw").cast(pl.String).alias("raw")
        ])
        df = df.with_columns([
            pl.col("raw").str.head(-1).cast(pl.Float64, strict=False).alias("num"),
            (pl.col("raw").str.ends_with("W") | pl.col("raw").str.ends_with("S")).alias("is_negative")
        ])
        df = df.with_columns([
            pl.when(pl.col("is_negative"))
            .then(pl.col("num") * -1.0 * self.scale)
            .otherwise(pl.col("num") * self.scale)
            .alias("result")
        ])
        return df["result"]


class datetimeconverter:
    def __init__(self, datetime_format="%Y%m%d%H"):
        self.datetime_format = datetime_format

    def __call__(self, series):
        s = pl.Series(series, dtype=pl.String)
        # Polars requires both hour and minute, so if format doesn't have %M, append '00' for minutes
        if '%M' not in self.datetime_format:
            s = s + '00'
            format_with_minutes = self.datetime_format + '%M'
        else:
            format_with_minutes = self.datetime_format
        return s.str.strptime(pl.Datetime, format=format_with_minutes, strict=False)


class categoricalconverter:
    def __init__(self,):
        pass

    def __call__(self, series):
        return pl.Series(series, dtype=pl.Categorical)

