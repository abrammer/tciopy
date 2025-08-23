"""
Read and write ATCF a or b deck files
"""
import re
from pathlib import Path
import numpy as np
import polars as pl
import polars.selectors as cs


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

def tolatlon(dataframe: pl.DataFrame,
            lat: str = 'lat',
            lon: str = 'lon',
            scale:float = 0.1) -> pl.DataFrame:
    """Convert lat/lon columns to numerical values in degrees"""
    latnum = pl.col(lat).str.strip_chars('NS ').cast(pl.Float64) * scale  # 64 is overkill but 32 creating floating point weirdness
    lonnum = pl.col(lon).str.strip_chars('EW ').cast(pl.Float64) * scale
    return dataframe.with_columns([
            pl.when(pl.col(lat).str.ends_with('S'))
            .then(latnum * -1.0)
            .otherwise(latnum),
            pl.when(pl.col(lon).str.ends_with('W'))
            .then(lonnum * -1.0)
            .otherwise(lonnum)
    ])


def read_adeck(fname: str | Path) -> pl.DataFrame:
    """Read adeck from filename into pandas dataframe"""
    # Tried versions of parsing colums in the read_csv func and they were much
    # slower

    datum = pl.scan_csv(fname, schema=adeck_schema, truncate_ragged_lines=True, has_header=False)
    datum = datum.with_columns(cs.string().str.strip_chars())

    datum = datum.with_columns([
        (pl.col("datetime")+'00').str.strptime(pl.Datetime, "%Y%m%d%H%M", strict=True),
    ])
    datum = tolatlon(datum)

    #  Quicker to process dates in series after than as a converter
    # datum["datetime"] = pd.to_datetime(datum["datetime"], format="%Y%m%d%H")
    # best_lines = datum["tech"] == "BEST"
    datum = datum.with_columns([
        pl.when(pl.col("tech") == "BEST")
        .then(pl.col("datetime") + pl.duration(minutes=pl.col("tnum").fill_null(0)))
        .otherwise(pl.col("datetime"))
        .alias("datetime"),
        pl.when((pl.col("tech") == "BEST") & pl.col("tnum").is_null())
        .then(0)
        .otherwise(pl.col("tnum"))
        .alias("tnum"),
        pl.duration(hours=pl.col("tau")).alias("tau"),
    ])

    datum = datum.with_columns((pl.col("datetime") + pl.col('tau')).alias("validtime"))

    quadrant_cols = ["NWQ", "NEQ", "SEQ", "SWQ"]
    all_zero = (pl.col("rad_NWQ") == 0) & (pl.col("rad_NEQ") == 0) & (pl.col("rad_SEQ") == 0) & (pl.col("rad_SWQ") == 0)

    # Set quadrant columns to null if all are zero
    datum = datum.with_columns([
        pl.when(~all_zero).then(pl.col(f'rad_{col}')).alias(f'rad_{col}')
        for col in quadrant_cols
    ])

    # Rename quadrant columns based on 'rad' value
    datum = datum.with_columns([
                        pl.when(pl.col('rad')==r).then(pl.col(f'rad_{y}')).alias(f'rad{r}_{y}')
                        for r in ['34', '50', '64']
                        for y in quadrant_cols
                        ])

    # Aggregate columns
    aggmethod = {
        pl.datatypes.Categorical: pl.first,
        pl.datatypes.String: pl.first,
        pl.datatypes.Int64: pl.mean,
        pl.datatypes.Float64: pl.mean,
        pl.datatypes.Datetime: pl.first,
    }
    grouper = ["basin", "number", "datetime", "tech", "tau"]
    exclude_cols = ["rad", "windcode",'rad_NEQ', 'rad_SEQ', 'rad_SWQ', 'rad_NWQ']
    agg_dict = [aggmethod.get(dtype, pl.first)(name)
                for name, dtype in zip( datum.collect_schema().names(), datum.collect_schema().dtypes())
                if name not in grouper+exclude_cols]

    decker = (datum
              .group_by(["basin", "number", "datetime", "tech", "tau"], maintain_order=True)
              .agg(agg_dict)
    )

    # Stretch out the stormname across neighboring rows
    # decker = decker.with_columns([
    #     pl.col("stormname").forward_fill().backward_fill().alias("stormname")
    # ])

    return decker


def read_bdeck(fname: str | Path) -> pl.DataFrame:
    """Read bdeck from filename into pandas dataframe"""
    return read_adeck(fname)


def read_adecks(fnames: list[str | Path]) -> pl.DataFrame:
    """Read multiple adeck files into a single pandas dataframe"""
    pl.enable_string_cache()
    return pl.concat([read_adeck(fname) for fname in fnames])


def read_bdecks(fnames: list[str | Path]) -> pl.DataFrame:
    """Read multiple bdeck files into a single pandas dataframe"""
    pl.enable_string_cache()
    return pl.concat([read_bdeck(fname) for fname in fnames])


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
    deck = deck.select(['lat', 'lon', 'datetime', 'basin', 'number', 'tech', 'tau', 'stormname']).collect()
    # deck = deck.collect()
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
    main(datadir / "aal032023.dat.gz")
