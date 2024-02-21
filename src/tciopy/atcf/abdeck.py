"""
Read and write ATCF a or b deck files
"""
import gzip
import re
# from dataclasses import dataclass
# from datetime import datetime
from pathlib import Path
from itertools import zip_longest
import numpy as np
import pandas as pd

from tciopy.atcf.decks import ADeck
# from tciopy.converters import DatetimeConverter, IntConverter, LatLonConverter, StrConverter
from tciopy.converters import datetimeconverter, int_converter, latlonconverter


def read_adeck(fname: str):
    """Read adeck from filename into pandas dataframe"""
    # Tried versions of parsing colums in the read_csv func and they were much
    # slower
    if isinstance(fname, str):
        fname = Path(fname)

    # n.b. ' *, *' takes care of stripping whitespace
    #  python engine allows for providing too many column names
    #  datetime as converter is super slow, str2ll is neglible time addition
    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open
    alldata = ADeck()
    # alldata = AdeckEntry()
    with opener(fname, mode="rt", newline="\n") as io_file:
        for line in io_file:
            splitline = re.split(r",\s*", line.rstrip("\n"), maxsplit=44)
            alldata.append(splitline)

    datum = alldata.to_dataframe()

    # for key, converter in converters.items():
    #     datum[key] = datum[key].apply(converter)
    # datum = datum.loc[(datum["lat"] != 0) | (datum["lon"] != 0)]

    #  Quicker to process dates in series after than as a converter
    # datum["datetime"] = pd.to_datetime(datum["datetime"], format="%Y%m%d%H")
    best_lines = datum["tech"] == "BEST"
    datum.loc[best_lines, "datetime"] = (
        pd.to_timedelta(datum.loc[best_lines, "tnum"].fillna(0), unit="m")
        + datum.loc[best_lines, "datetime"]
    )
    datum.loc[best_lines, "tnum"] = datum.loc[best_lines, "tnum"].fillna(0)
    datum["validtime"] = datum["datetime"] + pd.to_timedelta(datum["tau"], unit="h")

    datum.loc[((datum[['rad_NWQ', 'rad_NEQ', 'rad_SEQ', 'rad_SWQ']] == 0).sum(axis=1) == 4), ['rad_NWQ', 'rad_NEQ', 'rad_SEQ', 'rad_SWQ']] =  np.nan
    def rename_quadrants(group):
        key = group.name
        key = pd.to_numeric(key)
        if key == 0:
            key=34
        ngroup = group.rename(columns={'rad_NEQ': f'rad{key:2.0f}_NEQ','rad_NWQ': f'rad{key:2.0f}_NWQ','rad_SEQ': f'rad{key:2.0f}_SEQ','rad_SWQ': f'rad{key:2.0f}_SWQ' })
        return ngroup.reset_index()
    datum = datum.groupby('rad', observed=True).apply(rename_quadrants)

    aggmethod = {
        "object": "first",
        "int64": "mean",
        "float64": "mean",
        "datetime64[ns]": "first",
    }
    agg_dict = {index: aggmethod.get(str(dtype), "first") for index, dtype in datum.dtypes.items()}
    decker = (
        datum.groupby(["basin", "number", "datetime", "tech", "tau"], observed=True)
        .aggregate(agg_dict)
        .reset_index(drop=True)
    )
    decker.drop(
        columns=["rad", "windcode", ], inplace=True
    )

    # stretch out the stormname, across neighboring rows.
    decker.loc[:, "stormname"].ffill(inplace=True)
    decker.loc[:, "stormname"].bfill(inplace=True)

    return decker.reset_index(drop=True)


def read_bdeck(fname: str):
    """Read bdeck from filename into pandas dataframe"""
    return read_adeck(fname)


class AdeckEntry:
    def __init__(self):
        self.data_store = {
            "basin": {
                "data": [],
            },
            "number": {
                "data": [],
                "converter": int_converter(),
            },
            "datetime": {
                "data": [],
                "converter": datetimeconverter(datetime_format="%Y%m%d%H"),
            },
            "tnum": {
                "data": [],
                "converter": int_converter(),
            },
            "tech": {
                "data": [],
            },
            "tau": {
                "data": [],
                "converter": int_converter(),
            },
            "lat": {
                "data": [],
                "converter": latlonconverter(scale=0.1),
            },
            "lon": {
                "data": [],
                "converter": latlonconverter(scale=0.1),
            },
            "vmax": {
                "data": [],
                "converter": int_converter(),
            },
            "mslp": {
                "data": [],
                "converter": int_converter(),
            },
            "type": {
                "data": [],
            },
            "rad": {
                "data": [],
                "converter": int_converter(),
            },
            "windcode": {
                "data": [],
            },
            "rad_NEQ": {
                "data": [],
                "converter": int_converter(),
            },
            "rad_SEQ": {
                "data": [],
                "converter": int_converter(),
            },
            "rad_SWQ": {
                "data": [],
                "converter": int_converter(),
            },
            "rad_NWQ": {
                "data": [],
                "converter": int_converter(),
            },
            "pouter": {
                "data": [],
                "converter": int_converter(),
            },
            "router": {
                "data": [],
                "converter": int_converter(),
            },
            "rmw": {
                "data": [],
                "converter": int_converter(),
            },
            "gusts": {
                "data": [],
                "converter": int_converter(),
            },
            "eye": {
                "data": [],
                "converter": int_converter(),
            },
            "subregion": {
                "data": [],
            },
            "maxseas": {
                "data": [],
                "converter": int_converter(),
            },
            "initials": {
                "data": [],
            },
            "direction": {
                "data": [],
                "converter": int_converter(),
            },
            "speed": {
                "data": [],
                "converter": int_converter(),
            },
            "stormname": {
                "data": [],
            },
            "depth": {
                "data": [],
            },
            "seas": {"data": [], "converter": int_converter()},
            "seascode": {
                "data": [],
            },
            "seas1": {
                "data": [],
                "converter": int_converter(),
            },
            "seas2": {
                "data": [],
                "converter": int_converter(),
            },
            "seas3": {
                "data": [],
                "converter": int_converter(),
            },
            "seas4": {
                "data": [],
                "converter": int_converter(),
            },
            "userdefined1": {
                "data": [],
            },
            "userdata1": {
                "data": [],
            },
            "userdefined2": {
                "data": [],
            },
            "userdata2": {
                "data": [],
            },
            "userdefined3": {
                "data": [],
            },
            "userdata3": {
                "data": [],
            },
            "userdefined4": {
                "data": [],
            },
            "userdata4": {
                "data": [],
            },
            "userdefined5": {
                "data": [],
            },
            "userdata5": {
                "data": [],
            },
        }

    def append(self, values):
        for key, val in zip_longest(self.data_store.keys(), values, fillvalue=""):
            self.data_store[key]["data"].append(val)

    def pd_parse(self, key, converter, raw_data):
        if "converter" in converter:
            raw_data[key] = converter['converter'](raw_data[key])

    def to_dataframe(self):
        raw_data = {
            key: np.array(self.data_store[key]["data"], dtype=object) for key in self.data_store
        }
        for key, converter in self.data_store.items():
            self.pd_parse(key, converter, raw_data)
            # if "converter" in converter:
                # raw_data[key] = self.pd_parse(converter, raw_data[key])
        return pd.DataFrame(raw_data)

def write_adeck(outf, deck):
    for row in deck.itertuples():
        for line in format_adeck_line(row):
            line = line[:95] + re.sub(r"(, )[\s,0]+$", ", ", line[95:])
            line = line.rstrip(r"\n")
            outf.write(f"{line}\n")
    
def fillnan(val, nafill=0):
    if pd.isna(val):
        return nafill
    return val

def format_adeck_line(row):
    """Format a single row of a dataframe into an adeck line"""
    for kt in 34, 50, 64:
        rad1 = np.max([getattr(row, f"rad{kt}_NEQ", 0), 0])
        rad2 = np.max([getattr(row, f"rad{kt}_SEQ", 0), 0])
        rad3 = np.max([getattr(row, f"rad{kt}_SWQ", 0), 0])
        rad4 = np.max([getattr(row, f"rad{kt}_NWQ", 0), 0])
        if (kt > 34) and ((set([rad1, rad2, rad3, rad4]) == {0}) or pd.isnull([rad1, rad2, rad3, rad4]).all()):
            continue
        line = (
            f"{row.basin}, {row.number:>2}, {row.datetime:%Y%m%d%H}, "
            f"{row.tnum:02.0f}, {row.tech:>4}, {row.tau:3.0f}, "
            f"{fillnan(abs(row.lat)*10):3.0f}{'N' if row.lat>0 else 'S'}, "
            f"{fillnan(abs(row.lon)*10):4.0f}{'E' if row.lon>0 else 'W'},"
            f"{fillnan(row.vmax):4.0f}, {fillnan(row.mslp):4.0f}, {row.type:>2}, "
            f"{kt:3}, NEQ, {fillnan(rad1):4.0f}, {fillnan(rad2):4.0f}, {fillnan(rad3):4.0f}, {fillnan(rad4):4.0f}, "
            f"{fillnan(row.pouter):4.0f}, {fillnan(row.router):4.0f}, {fillnan(row.rmw):3.0f}, {fillnan(row.gusts):3.0f}, "
            f"{fillnan(row.eye):3.0f},{row.subregion:>4},{fillnan(row.maxseas):4.0f}, {row.initials:>3}, "
            f"{fillnan(row.direction):3.0f},{fillnan(row.speed):4.0f}, {row.stormname:>10}, {row.depth:>1}, "
            f"{fillnan(row.seas):4.0f}, {row.seascode:>3}, {fillnan(row.seas1):4.0f}, {fillnan(row.seas2):4.0f}, "
            f"{fillnan(row.seas3):4.0f}, {fillnan(row.seas4):4.0f}, {row.userdefined1:>4}, {row.userdata1:>4}, "
            f"{row.userdefined2:>4}, {row.userdata2:>4}, {row.userdefined3:>4}, {row.userdata3:>4}, "
            f"{row.userdefined4:>4}, {row.userdata4:>4}, {row.userdefined5:>4}, {row.userdata5:>4}"
        )
        yield line


def main(input_filepath):
    """demo function of parsing single adeck file"""
    import time

    stime = time.time()
    deck = read_adeck(input_filepath)
    print(time.time() - stime)

    print(deck)

    # print(deck)
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
    datadir = Path(__file__).parent.parent.parent.parent / "data"
    print(datadir)
    main(datadir / "aal032023.dat")
