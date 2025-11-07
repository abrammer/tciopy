"""
Read and write ATCF a or b deck files
"""
import re
from pathlib import Path
import numpy as np
import polars as pl
import polars.selectors as cs

adeck_schema = pl.Schema({
    "basin": pl.String,
    "number": pl.String,
    "datetime": pl.String,
    "tnum": pl.Int16,
    "tech": pl.String,
    "tau": pl.Int16,
    "lat": pl.String,
    "lon": pl.String,
    "vmax": pl.Float32,
    "mslp": pl.Float32,
    "type": pl.String,
    "rad": pl.String,
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
    "stormname": pl.String,
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


def read_adeck(fname: str | Path, return_type:str='polars' ) -> pl.DataFrame:
    """Read adeck from filename into polars dataframe"""

    datum = pl.scan_csv(fname, schema=adeck_schema, truncate_ragged_lines=True, has_header=False)
    datum = datum.with_columns(cs.string().str.strip_chars())

    datum = datum.with_columns([
        (pl.col("datetime")+'00').str.strptime(pl.Datetime, "%Y%m%d%H%M", strict=True),
    ])
    datum = tolatlon(datum)

    # Best Lines reuse the TNUM column for minutes.
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

    # if mask_radii:
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
    grouper = ["basin", "number", "datetime", "tech", "tau"]
    exclude_cols = ['rad','rad_NEQ', 'rad_SEQ', 'rad_SWQ', 'rad_NWQ']
    agg_dict = [pl.col(name).drop_nulls().first().alias(name)
                for name in  datum.collect_schema().names()
                if name not in grouper+exclude_cols]

    decker = (datum
              .group_by(grouper, maintain_order=True)
              .agg(agg_dict)
    )


    if return_type == 'pandas':
        import pandas as pd
        return decker.collect().to_pandas()
    else:
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


def write_adeck(outf, deck: pl.DataFrame):
    # Accept either a LazyFrame or a collected DataFrame
    if isinstance(deck, pl.LazyFrame):
        deck = deck.collect()
    for row in deck.iter_rows(named=True):
        for line in format_adeck_line(row):
            line = line.rstrip("\n")
            outf.write(f"{line}\n")
        outf.write("")


def fillnan(val, nafill=0):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return nafill
    return val


def format_adeck_line(row):
    """Format a single row of a dataframe into an adeck line"""
    reg = re.compile(r"[a-zA-Z1-9]")
    carq0 = row['tech'] == 'CARQ' and row['tau'].total_seconds() == 0
    line = (
        f"{row["basin"]}, {row["number"]:>2}, {row["datetime"]:%Y%m%d%H}, "
        f"{row["tnum"]:02.0f}, {row["tech"]:>4}, {row["tau"].total_seconds()/3600:3.0f}, "
        f"{fillnan(abs(row["lat"])*10):3.0f}{'N' if row["lat"]>=0 else 'S'}, "
        f"{fillnan(abs(row["lon"])*10):4.0f}{'E' if row["lon"]>0 else 'W'},"
        f"{fillnan(row["vmax"]):4.0f}, ")

    extra2 = (
        f"{fillnan(row["pouter"]):4.0f}, {fillnan(row["router"]):4.0f}, {fillnan(row["rmw"]):3.0f}, {fillnan(row["gusts"]):3.0f}, "
        f"{fillnan(row["eye"]):3.0f},{fillnan(row["subregion"],""):>4},{fillnan(row["maxseas"]):4.0f}, {fillnan(row["initials"],""):>3}, "
        f"{fillnan(row["direction"]):3.0f},{fillnan(row["speed"]):4.0f}, ")
    extra2a = f"{fillnan(row["stormname"],""):>10}, {fillnan(row["depth"],""):>1},"
    extra3 = (
        f"{fillnan(row['seas'], 0):3.0f}, {fillnan(row["seascode"],nafill=""):>3}, {fillnan(row["seas1"]):4.0f}, {fillnan(row["seas2"]):4.0f}, "
        f"{fillnan(row["seas3"]):4.0f}, {fillnan(row["seas4"]):4.0f}, ")
    for i in range(1,6):
        userextra = f"{fillnan(row[f"userdefined{i}"], ""):>3}, {fillnan(row[f"userdata{i}"], ""):>1}, "
        if reg.search(userextra):
            extra3 += userextra
        else:
            break


    for kt in 34, 50, 64:
        rad1 = row[f"rad{kt}_NEQ"] or 0
        rad2 = row[f"rad{kt}_SEQ"] or 0
        rad3 = row[f"rad{kt}_SWQ"] or 0
        rad4 = row[f"rad{kt}_NWQ"] or 0
        if(not carq0) and (kt > 34) and \
            ((set([rad1, rad2, rad3, rad4]) == {0}) or np.isnan([rad1, rad2, rad3, rad4]).all()):
            continue
        if row['windcode'] == "":
            kt = 0
        extra1 = (f"{fillnan(row["mslp"]):4.0f}, {row["type"]:>2}, "
            f"{kt:3}, {row['windcode']:>3}, {fillnan(rad1):4.0f}, {fillnan(rad2):4.0f}, {fillnan(rad3):4.0f}, {fillnan(rad4):4.0f}, ")

        # if extra3 contains valid data include all three extras
        if reg.search(extra3):
            retline = line + extra1 + extra2 + extra2a + extra3
        elif reg.search(extra2a):
            retline = line + extra1 + extra2 + extra2a
        elif reg.search(extra2):
            retline = line + extra1 + extra2
        else:
            retline = line + extra1
        yield retline


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
