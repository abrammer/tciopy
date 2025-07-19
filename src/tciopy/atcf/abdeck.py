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
import polars as pl

from tciopy.atcf.decks import ADeck
# from tciopy.converters import DatetimeConverter, IntConverter, LatLonConverter, StrConverter
from tciopy.converters import datetimeconverter, int_converter, latlonconverter

        # self.basin = CategoricalColumn()
        # self.number = CategoricalColumn()
        # self.datetime = DatetimeColumn(datetime_format="%Y%m%d%H")
        # self.tnum = NumericColumn()
        # self.tech = CategoricalColumn()
        # self.tau = NumericColumn()
        # self.lat = LatLonColumn(scale=0.1)
        # self.lon = LatLonColumn(scale=0.1)
        # self.vmax = NumericColumn()
        # self.mslp = NumericColumn()
        # self.type = CategoricalColumn()
        # self.rad = CategoricalColumn()
        # self.windcode = StringColumn()
        # self.rad_NEQ = NumericColumn()
        # self.rad_SEQ = NumericColumn()
        # self.rad_SWQ = NumericColumn()
        # self.rad_NWQ = NumericColumn()
        # self.pouter = NumericColumn()
        # self.router = NumericColumn()
        # self.rmw = NumericColumn()
        # self.gusts = NumericColumn()
        # self.eye = NumericColumn()
        # self.subregion = StringColumn()
        # self.maxseas = NumericColumn()
        # self.initials = StringColumn()
        # self.direction = NumericColumn()
        # self.speed = NumericColumn()
        # self.stormname = CategoricalColumn()
        # self.depth = StringColumn()
        # self.seas = NumericColumn()
        # self.seascode = StringColumn()
        # self.seas1 = NumericColumn()
        # self.seas2 = NumericColumn()
        # self.seas3 = NumericColumn()
        # self.seas4 = NumericColumn()
        # self.userdefined1 = StringColumn()
        # self.userdata1 = StringColumn()
        # self.userdefined2 = StringColumn()
        # self.userdata2 = StringColumn()
        # self.userdefined3 = StringColumn()
        # self.userdata3 = StringColumn()
        # self.userdefined4 = StringColumn()
        # self.userdata4 = StringColumn()
        # self.userdefined5 = StringColumn()
        # self.userdata5 = StringColumn()

adeck_schema = pl.Schema({
    "basin": pl.Categorical(),
    "number": pl.Categorical(),
    "datetime": pl.String,
    "tnum": pl.Int16,
    "tech": pl.Categorical(),
    "tau": pl.Int16,
    "lat": pl.String,
    "lon": pl.String,
    "vmax": pl.Float32,
    "mslp": pl.Float32,
    "type": pl.Categorical(),
    "rad": pl.Categorical(),
    "windcode": pl.String,
    "rad_NEQ": pl.Float32,
    "rad_SEQ": pl.Float32,
    "rad_SWQ": pl.Float32,
    "rad_NWQ": pl.Float32,
    "pouter": pl.Float32,
    "router": pl.Float32,
    "rmw": pl.Float32,
    "gusts": pl.Float32,
    "eye": pl.Float32,
    "subregion": pl.String,
    "maxseas": pl.Float32,
    "initials": pl.String,
    "direction": pl.Float32,
    "speed": pl.Float32,
    "stormname": pl.Categorical(),
    "depth": pl.String,
    "seas": pl.Float32,
    "seascode": pl.String,
    "seas1": pl.Float32,
    "seas2": pl.Float32,
    "seas3": pl.Float32,
    "seas4": pl.Float32,
    "userdefined1": pl.String,
    "userdata1": pl.String,
    "userdefined2": pl.String,
    "userdata2": pl.String,
    "userdefined3": pl.String,
    "userdata3": pl.String,
    "userdefined4": pl.String,
    "userdata4": pl.String,
    "userdefined5": pl.String,
    "userdata5": pl.String
})


def read_adeck(fname: str):
    """Read adeck from filename into pandas dataframe"""
    # Tried versions of parsing colums in the read_csv func and they were much
    # slower
    if isinstance(fname, str):
        fname = Path(fname)

    # # n.b. ' *, *' takes care of stripping whitespace
    # #  python engine allows for providing too many column names
    # #  datetime as converter is super slow, str2ll is neglible time addition
    # if fname.suffix == ".gz":
    #     opener = gzip.open
    # else:
    #     opener = open
    # alldata = ADeck()
    # # alldata = AdeckEntry()
    # with opener(fname, mode="rt", newline="\n") as io_file:
    #     for line in io_file:
    #         splitline = re.split(r",\s*", line.rstrip("\n"), maxsplit=44)
    #         alldata.append(splitline)
    datum = pl.read_csv(fname, schema=adeck_schema)
    # datum = alldata.to_polarsframe()
    datum = datum.with_columns([
        (pl.col("datetime")+'00').str.strptime(pl.Datetime, "%Y%m%d%H%M", strict=False),
    ])
    datum = datum.with_columns([
            pl.when(pl.col("lat").str.ends_with('S'))
            .then(pl.col("lat").str.strip_chars('NS ').cast(pl.Float32) * -0.1)
            .otherwise(pl.col("lat").str.strip_chars('NS ').cast(pl.Float32)*0.1),
            pl.when(pl.col("lon").str.ends_with('W'))
            .then(pl.col("lon").str.strip_chars('EW ').cast(pl.Float32) * -0.1)
            .otherwise(pl.col("lon").str.strip_chars('EW ').cast(pl.Float32)*0.1)
    ])

    #  Quicker to process dates in series after than as a converter
    # datum["datetime"] = pd.to_datetime(datum["datetime"], format="%Y%m%d%H")
    best_lines = datum["tech"] == "BEST"
    datum = datum.with_columns([
        pl.when(best_lines)
        .then(pl.col("datetime") + pl.duration(minutes=pl.col("tnum").fill_null(0)))
        .otherwise(pl.col("datetime"))
        .alias("datetime")
    ])

    datum = datum.with_columns([
        pl.when(best_lines & pl.col("tnum").is_null())
        .then(0)
        .otherwise(pl.col("tnum"))
        .alias("tnum")
    ])
    datum = datum.with_columns([
        (pl.col("datetime") + pl.duration(hours=pl.col("tau")).alias("validtime"))
    ])

    # Set quadrant columns to null if all are zero
    quadrant_cols = ["rad_NWQ", "rad_NEQ", "rad_SEQ", "rad_SWQ"]
    all_zero = (pl.col("rad_NWQ") == 0) & (pl.col("rad_NEQ") == 0) & (pl.col("rad_SEQ") == 0) & (pl.col("rad_SWQ") == 0)
    datum = datum.with_columns([
        pl.when(all_zero).then(None).otherwise(pl.col(col)).alias(col)
        for col in quadrant_cols
    ])

    # Rename quadrant columns based on 'rad' value
    datum = datum.with_columns([
                        pl.when(pl.col('rad')==r).then(pl.col(f'rad_{y}')).alias(f'rad{r}_{y}')
                        for r in ['34', '50', '64']
                        for y in ['NEQ', 'NWQ', 'SEQ', 'SWQ']
                        ])

    # Aggregate columns
    aggmethod = {
        pl.datatypes.Utf8: pl.first,
        pl.datatypes.Int64: pl.mean,
        pl.datatypes.Float64: pl.mean,
        pl.datatypes.Datetime: pl.first,
    }
    grouper = ["basin", "number", "datetime", "tech", "tau"]
    exclude_cols = ["rad", "windcode",]
    agg_dict = [aggmethod.get(dtype, pl.first)(name)
                for name, dtype in zip(datum.columns, datum.dtypes)
                if name not in grouper+exclude_cols]

    decker = (datum
              .group_by(["basin", "number", "datetime", "tech", "tau"], maintain_order=True)
              .agg(agg_dict)
    )


    # Stretch out the stormname across neighboring rows
    decker = datum.with_columns([
        pl.col("stormname").forward_fill().backward_fill().alias("stormname")
    ])

    return decker


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
    print(deck.columns)

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
