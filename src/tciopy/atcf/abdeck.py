"""
Read and write ATCF a or b deck files
"""
import gzip
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from tciopy.converters import (DatetimeConverter, IntConverter,
                               LatLonConverter, StrConverter)


def read_adeck(fname: str | Path):
    """Read adeck from filename into pandas dataframe"""
    # Tried versions of parsing colums in the read_csv func and they were much
    # slower
    if isinstance(fname, str):
        fname = Path(fname)

    # atcf_colnames = [
    #     "basin",
    #     "number",
    #     "datetime",
    #     "tnum",
    #     "tech",
    #     "tau",
    #     "lat",
    #     "lon",
    #     "vmax",
    #     "mslp",
    #     "type",
    #     "rad",
    #     "windcode",
    #     "rad1",
    #     "rad2",
    #     "rad3",
    #     "rad4",
    #     "pouter",
    #     "router",
    #     "rmw",
    #     "gusts",
    #     "eye",
    #     "subregion",
    #     "maxseas",
    #     "initials",
    #     "direction",
    #     "speed",
    #     "stormname",
    #     "depth",
    #     "seas",
    #     "seascode",
    #     "seas1",
    #     "seas2",
    #     "seas3",
    #     "seas4",
    #     "userdefined1",
    #     "userdata1",
    #     "userdefined2",
    #     "userdata2",
    #     "userdefined3",
    #     "userdata3",
    #     "userdefined4",
    #     "userdata4",
    #     "userdefined5",
    #     "userdata5",
    # ]
    # dtypes = {
    #     "basin": str,
    #     "datetime": str,
    #     "tech": str,
    #     "tau": float,
    #     "vmax": float,
    #     "mslp": float,
    #     "type": str,
    #     "rad": float,
    #     "windcode": str,
    #     "rad1": float,
    #     "rad2": float,
    #     "rad3": float,
    #     "rad4": float,
    # }

    # converters = {"lat": str2ll, "lon": str2ll}
    # n.b. ' *, *' takes care of stripping whitespace
    #  python engine allows for providing too many column names
    #  datetime as converter is super slow, str2ll is neglible time addition
    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open
    alldata = []
    with opener(fname, mode="rt", newline="\n") as io_file:
        for line in io_file:
            splitline = re.split(r",\s+", line)
            alldata.append(AdeckData(*splitline))
        # datum = pd.read_csv(
        #     io_file,
        #     sep=" *, *",
        #     engine="python",
        #     index_col=False,
        #     header=None,
        #     on_bad_lines="warn",
        #     names=atcf_colnames,
        #     dtype=dtypes,
        # )
    datum = pd.DataFrame(alldata)
    # for key, converter in converters.items():
    #     datum[key] = datum[key].apply(converter)
    datum = datum.loc[(datum["lat"] != 0) | (datum["lon"] != 0)]

    #  Quicker to process dates in series after than as a converter
    # datum["datetime"] = pd.to_datetime(datum["datetime"], format="%Y%m%d%H")
    best_lines = datum["tech"] == "BEST"
    datum.loc[best_lines, "datetime"] = (
        pd.to_timedelta(datum.loc[best_lines, "tnum"].fillna(0), unit="m")
        + datum.loc[best_lines, "datetime"]
    )
    datum.loc[best_lines, "tnum"] = np.NAN
    datum["validtime"] = datum["datetime"] + pd.to_timedelta(datum["tau"], unit="h")

    # This transposes mulitple rows for each radii, to a single row with multiple columns.
    for kt in 34, 50, 64:
        for qdc, quad in [("NEQ", "NEQ"), ("SEQ", "SEQ"), ("SWQ", "SWQ"), ("NWQ", "NWQ")]:
            datum[f"rad{kt}_{qdc}"] = datum[datum["rad"] == kt][f"radx_{quad}"]

    aggmethod = {
        "object": "first",
        "int64": "mean",
        "float64": "mean",
        "datetime64[ns]": "first",
    }
    agg_dict = {index: aggmethod.get(str(dtype)) for index, dtype in datum.dtypes.items()}
    decker = (
        datum.groupby(["basin", "number", "datetime", "tech", "tau"])
        .aggregate(agg_dict)
        .reset_index(drop=True)
    )
    decker.drop(
        columns=["rad", "windcode", "radx_NEQ", "radx_NWQ", "radx_SEQ", "radx_SWQ"], inplace=True
    )

    # stretch out the stormname, across neighboring rows.
    decker.loc[:, "stormname"].ffill(inplace=True)
    decker.loc[:, "stormname"].bfill(inplace=True)

    return decker.reset_index(drop=True)


def read_bdeck(fname: str | Path):
    """Read bdeck from filename into pandas dataframe"""
    return read_adeck(fname)


@dataclass
class AdeckData:
    basin: str = StrConverter()
    number: int = IntConverter()
    datetime: datetime = DatetimeConverter(datetime_format="%Y%m%d%H")
    tnum: int = IntConverter()
    tech: str = StrConverter()
    tau: int = IntConverter()
    lat: float = LatLonConverter(scale=0.1)
    lon: float = LatLonConverter(scale=0.1)
    vmax: int = IntConverter()
    mslp: int = IntConverter()
    type: str = StrConverter()
    rad: int = IntConverter()
    windcode: str = StrConverter()
    radx_NEQ: int = IntConverter()
    radx_SEQ: int = IntConverter()
    radx_SWQ: int = IntConverter()
    radx_NWQ: int = IntConverter()
    pouter: int = IntConverter()
    router: int = IntConverter()
    rmw: int = IntConverter()
    gusts: int = IntConverter()
    eye: int = IntConverter()
    subregion: str = StrConverter()
    maxseas: int = IntConverter()
    initials: str = StrConverter()
    direction: int = IntConverter()
    speed: int = IntConverter()
    stormname: str = StrConverter()
    depth: str = StrConverter()
    seas: int = IntConverter()
    seascode: str = StrConverter()
    seas1: int = IntConverter()
    seas2: int = IntConverter()
    seas3: int = IntConverter()
    seas4: int = IntConverter()
    userdefined1: str = StrConverter()
    userdata1: str = StrConverter()
    userdefined2: str = StrConverter()
    userdata2: str = StrConverter()
    userdefined3: str = StrConverter()
    userdata3: str = StrConverter()
    userdefined4: str = StrConverter()
    userdata4: str = StrConverter()
    userdefined5: str = StrConverter()
    userdata5: str = StrConverter()


def format_adeck_line(row):
    """Format a single row of a dataframe into an adeck line"""
    for kt in 34, 50, 64:
        rad1 = np.max([getattr(row, f"rad{kt}_1", 0), 0])
        rad2 = np.max([getattr(row, f"rad{kt}_2", 0), 0])
        rad3 = np.max([getattr(row, f"rad{kt}_3", 0), 0])
        rad4 = np.max([getattr(row, f"rad{kt}_4", 0), 0])
        if (kt > 34) and set([rad1, rad2, rad3, rad4]) == {0}:
            continue
        line = (
            f"{row.basin}, {row.number:2}, {row.datetime:%Y%m%d%H}, "
            f"{row.tnum:02.0f}, {row.tech}, {row.tau:3.0f}, "
            f"{abs(row.lat)*10:3.0f}{'N' if row.lat>0 else 'S'}, "
            f"{abs(row.lon)*10:4.0f}{'E' if row.lon>0 else 'W'},"
            f"{row.vmax:4.0f}, {row.mslp:4.0f}, {row.type if row.type else 'XX'}, "
            f"{kt:3}, NEQ, {rad1:4.0f}, {rad2:4.0f}, {rad3:4.0f}, {rad4:4.0f}, "
            f"{row.pouter:4.0f}, {row.router:4.0f}, {row.rmw:3.0f}, {row.gusts:3.0f}, "
            f"{row.eye:3.0f},{row.subregion:>3},{row.maxseas:3.0f}, {row.initials:>3}, "
            f"{row.direction:3.0f},{row.speed:3.0f}, {row.stormname:>10}"
        )
        yield line


def str2ll(x):
    """Convert atcf str to latlon -- internal single value only"""
    converters = {"N": 1, "S": -1, "W": -1, "E": 1}
    x = x.strip()
    if x == "0":
        ret = 0
    else:
        try:
            ret = (int(x[:-1]) * converters[x[-1]]) / 10
        except (ValueError, IndexError):
            return 0
    return ret


def main(input_filepath):
    """demo function of parsing single adeck file"""
    import time

    stime = time.time()
    deck = read_adeck(input_filepath)
    print(time.time() - stime)
    print(deck)
    # print(deck)
    # for key, storm in deck.groupby(['basin', 'number', 'datetime']):
    #     adeck_name = f"a{key[0].lower()}{key[1]}{key[2]:%Y}.dat"
    #     output_filepath = pathlib.Path(output_dir, adeck_name)
    #     with open(output_filepath, 'a') as f:
    #         for datarow in storm.itertuples():
    #             for line in format_adeck_line(datarow):
    #                 f.write(f"{line}\n")
    #     subprocess.call([
    #         "sort", "-u", "-k", "3,3n", "-k", "5,5", "-k", "6,6n", "-k",
    #         "12,12n", output_filepath, "-o", output_filepath
    #     ])


if __name__ == "__main__":
    main("/Users/abrammer/Downloads/bal092022.dat")
