""" ATCF Converter classes for parsing ATCF data into pandas Series 
    Rather than converting each value seperately, these classes accumulate input data into a list,
    and then convert the list to a pandas Series when pd_parse() is called.
    This allows for more efficient parsing of the ATCF data.
"""

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
