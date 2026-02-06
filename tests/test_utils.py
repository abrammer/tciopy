import polars as pl
import tciopy.utils as utils

import pytest

def test_storm_direction():
    lat = pl.Series("lat", [10.0, 11.0, 12.0, 13.0])
    lon = pl.Series("lon", [20.0, 21.0, 22.0, 23.0])
    result = utils.storm_direction(lat, lon).to_list()
    expected = [0.0, 45.0, 45.0, 45.0]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))

def test_direction_spread():
    lat = pl.Series("lat", [10.0, 10.0, 11.0, 12.0, 13.0, 13.0])
    lon = pl.Series("lon", [23.0, 20.0, 21.0, 22.0, 23.0, 20.0])
    direction = pl.Series("direction", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])+90
    test_frame = pl.DataFrame({
        "lat": lat,
        "lon": lon,
        "direction": direction
    })
    test_frame.with_columns(
        utils.direction_spread(pl.col("lat"), pl.col("lon"), pl.col("direction")).alias("par_spread"),
        utils.direction_spread(pl.col("lat"), pl.col("lon"), 90+pl.col("direction")).alias("perp_spread"),
    )
        
    result = utils.direction_spread(lat, lon, direction).to_list()
    expected = [0.0, 111.19492664455873, 222.38985328911746, 333.5847799336762]
    assert all(abs(r - e) < 1e-6 for r, e in zip(result, expected))