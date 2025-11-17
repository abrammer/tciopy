import gzip
import re
from pathlib import Path
import logging

import polars as pl

from tciopy.converters import tolatlon

LOGGER = logging.getLogger(__name__)


def read_edeck(fname: str, format_filter: list[str] = None) -> dict[str, pl.DataFrame]:
    """Read an f-deck file into a polars DataFrame
    Parameters
    ----------
    fname : str or Path
        Path to the edeck file. Can be gzipped.
    
    format_filter : list of str, optional
        List of edeck formats to read. If None, all formats are read.
        Valid formats are: "TR", "IN", "RI", "RW", "WR", "PR", "GN", "GS", "ER"
    
    Returns
    -------
    dict[str, pl.DataFrame]
        Dictionary of Polars DataFrames containing the edeck data, keyed by format.
    """
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open
    allformats = ["TR", "IN", "RI", "RW", "WR", "PR", "GN", "GS", "ER"]
    alldata = {key: [] for key in allformats if not format_filter or key in format_filter}
    with opener(fname, "rt", newline="\n") as io_file:
        for line in io_file:
            try:
                splitline = re.split(r",\s+", line)
            except ValueError as e:
                LOGGER.warning("Failed to parse edeck line from %s \n %s",fname, line)
            ftype = splitline[3]
            if ftype == "03":
                ftype = "TR"
            if ftype not in alldata:
                LOGGER.warning("Unrecognised type in edeck line from %s \n %s", fname, line)
            alldata[ftype].append(splitline)

    dfs = {}
    for key, data in alldata.items():
        datum =  pl.DataFrame(data, orient="row", schema=edeck_schemas[key])
        datum = datum.with_columns([
            (pl.col("datetime")+'00').str.strptime(pl.Datetime, "%Y%m%d%H%M", strict=True),
            ])
        datum = tolatlon(datum)
        dfs[key] = datum

    # df = pd.concat(dfs, ignore_index=True, sort=False)
    return dfs

#                  ATCF Probability Format         11/2020
# COMMON FIELDS
# -------------
# Basin, CY, YYYYMMDDHH, ProbFormat, Tech, TAU, LatN/S, LonE/W, Prob, ...

#   Common section, fields 1 - 9,
#   followed by a specific format, examples follow:
# 	Track related data
# 	Intensity probability
# 	Rapid Intensification probabilities
# 	Rapid Decay probabilities
# 	Wind Radii estimated errors
# 	Pressure estimated error
# 	TC Genesis probability
# 	TC Genesis shape
#   Eyeway Replacement forecast probabilities

edeck_base_schema = {
    "basin": pl.String,
    "number": pl.String, #: Storm number,  2 char. [can be alphanumeric for pregenesis]
    "datetime": pl.String,  #: Fix date-time-group,                 12 char.
    #: ProbFormat - 2 char.
        #:*   TR - track, "03" also accepted for existing old edeck files
        #:*   IN - intensity
        #:*   RI - rapid intensification
        #:*   RW - rapid weakening
        #:*   WR - wind radii
        #:*   PR - pressure
        #:*   GN - TC genesis probability
        #:*   GS - TC genesis shape
        #:*   ER - eyewall replacement
    "format": pl.String,
    "tech": pl.String,        #: Tech - acronym for each objective technique,  4 char.
    "tau": pl.Int16,    # TAU - forecast period: 0 through 168 hours,  3 char.
    "lat": pl.String,    # LatN/S - Latitude (tenths of degrees) for the DTG: 0 through 900, N/S is the hemispheric index,  4 char.
    "lon": pl.String,     # LonE/W - Longitude (tenths of degrees) for the DTG: 0 through 1800, E/W is the hemispheric index,  5 char.
    "prob": pl.Float32    # Prob - probability of ProbItem (see parameter specific definition of ProbItem), 0 - 100%,  3 char.
}

track_schema = edeck_base_schema.copy()
track_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - Probability radius, 0 through 2000 nm,  4 char.
    "ty": pl.String(),          #: TY - Level of tc development: currently unused,  2 char.
    "dir": pl.Int16(),          #: Dir - Cross track direction, 0 through 359 degrees,  3 char.
    "windcode": pl.String(),    #: WindCode - Radius code: currently unused,  3 char.
    "rad_cross": pl.Float32(),  #: rad_cross - Cross track radius, 0 through 2000 nm,  4 char.
    "rad_along": pl.Float32(),  #: rad_along - Along track radius, 0 through 2000 nm,  4 char.
    "bias_cross": pl.Float32(), #: bias_cross - Cross track bias, -999 through 999 nm,  4 char.
    "bias_along": pl.Float32(), #: bias_along - Along track bias, -999 through 999 nm,  4 char.
    "extra": pl.String()        # Placeholder for any null values
})

wind_radii_schema = edeck_base_schema.copy()
wind_radii_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - Wind speed (bias adjusted), 0 - 300 kts,  3 char.
    "th": pl.Int16(),           #: TH - Wind Threshold (e.g., 34),  2 char.
    "half_range": pl.Float32(), #: Half_Range - Half the probability range (radius in n mi), 15 - 200 n mi,  4 char.
    "extra": pl.String()        # Placeholder for any null values
})

intensity_schema = edeck_base_schema.copy()
intensity_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - Wind speed (bias adjusted), 0 - 300 kts,  3 char.
    "ty": pl.String(),          #: TY - Level of tc development: currently unused,  2 char.
    "half_range": pl.Float32(), #: Half_Range - Half the probability range (radius), 0 - 50 kts,  4 char.
    "extra": pl.String()        # Placeholder for any null values
})

pressure_schema = edeck_base_schema.copy()
pressure_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - Pressure (bias adjusted), 0 - 1000 mb,  3 char.
    "ty": pl.String(),          #: TY - Level of tc development: currently unused,  2 char.
    "half_range": pl.Float32(), #: Half_Range - Half the probability range (radius), 0 - 50 kts,  4 char.
    "extra": pl.String()        # Placeholder for any null values
})

rapid_intensification_schema = edeck_base_schema.copy()
rapid_intensification_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - Wind speed (bias adjusted), 0 - 300 kts,  3 char. 
    "v": pl.Float32(),          #: V - final intensity, 0 - 300 kts,  3 char.
    "initials": pl.String(),    #: Initials - forecaster initials,  3 char.
    "ri_start_tau": pl.Int16(), #: RIstartTAU - RI start time: 0 through 168 hours,  3 char.
    "ri_stop_tau": pl.Int16(),  #: RIstopTAU - RI stop time: 0 through 168 hours,  3 char.
    "extra": pl.String()        # Placeholder for any null values
})

rapid_weakening_schema = edeck_base_schema.copy()
rapid_weakening_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - intensity change, 0 - 300 kts,  3 char.
    "v": pl.Float32(),          #: V - final intensity, 0 - 300 kts,  3 char.
    "initials": pl.String(),    #: Initials - forecaster initials,  3 char.
    "rw_start_tau": pl.Int16(), #: RWstartTAU - RW start time: 0 through 168 hours,  3 char.
    "rw_stop_tau": pl.Int16(),  #: RWstopTAU - RW stop time: 0 through 168 hours,  3 char.
    "extra": pl.String()        #: Placeholder for any null values
})

genesis_schema = edeck_base_schema.copy()
genesis_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - time period, ie genesis during next xxx hours, 0 for genesis or dissipate event, 0 - 240 hrs,  4 char.
    "initials": pl.String(),    #: Initials - forecaster initials,  3 char.
    "gen_or_dis": pl.String(),  #: GenOrDis - "invest", "genFcst", "genesis", "disFcst" or "dissipate"
    "dtg": pl.String(),         #: DTG - Genesis or dissipated event Date-Time-Group, yyyymmddhhmm: 0000010100 through 9999123123,  12 char.
    "storm_id": pl.String(),    #: stormID - cyclone ID if the genesis developed into an invest area or cyclone ID of dissipated TC, e.g. al032014
    "min": pl.Int16(),          #: min - minutes, associated with DTG in common fields (3rd field in record), 0 - 59 min
    "genesis_num": pl.Int16(),  #: GenesisNum - Genesis number, if spawned from a genesis area (1-999),  3 char.
    "undefined": pl.String(),   #: Undefined - Placeholder for any null values,  3 char.
})

genesis_shape_schema = edeck_base_schema.copy()
genesis_shape_schema.update({
    "probitem": pl.Float32(),       #: ProbItem - time period, ie genesis during next xxx hours, 0 - 240 hrs,  4 char.
    "initials": pl.String(),        #: Initials - forecaster initials,  3 char.
    "tcfamanopdtg": pl.String(),    #: TCFAMANOPDTG - TCFA MANOP dtg, ddhhmm,  6 char.
    "tcfamsgdtg": pl.String(),      #: TCFAMSGDTG - TCFA message dtg, yymmddhhmm
    "tcfawtnum": pl.Int16(),        #: TCFAWTNUM - TCFA WT number,  2 char.
    "shapetype": pl.String(),       #: ShapeType - shape type, ELP - ellipse, BOX - box, CIR - circle, PLY - polygon,  3 char.
    "ellipseangle": pl.Int16(),     #: EllipseAngle - cross track angle for ellipse (math coords), 3 char.
    "ellipsercross": pl.Float32(),  #: EllipseRCross - Ellipse radius cross,  4 char.
    "ellipseralong": pl.Float32(),  #: EllipseRAlong - Ellipse radius along,  4 char.
    "box1latns": pl.Float32(),      #: Box1LatN/S - Latitude for start point for TCFA box center line or center point for TCFA circle,  4 char.
                                        #: 	     0 - 900 tenths of degrees
                                        #:              N/S is the hemispheric index
    "box1lonew": pl.Float32(),      #: Box1LonE/W - Longitude for start point for TCFA box center line or center point for TCFA circle,  5 char.
    "box2latns": pl.Float32(),      #: Box2LatN/S - Latitude for end point for TCFA box center line, not used for TCFA circle,  4 char.
    "box2lonew": pl.Float32(),      #: Box2LonE/W - Longitude for start point for TCFA box center line, not used for TCFA circle,  5 char.
    "tcfaradius": pl.Float32(),     #: TCFARADIUS - distance from center line to box edge, or radius of circle (nm),  3 char.
    "polygonpts": pl.String(),      #: PolygonPts - array of 20 lat, lon points defining a polygon
    "extra": pl.String()            #: Placeholder for any null values
})

eyewall_replacement_schema = edeck_base_schema.copy()
eyewall_replacement_schema.update({
    "probitem": pl.Float32(),   #: ProbItem - intensity change, 0 - 300 kts,  3 char.
    "v": pl.Float32(),          #: V - final intensity, 0 - 300 kts,  3 char.
    "initials": pl.String(),    #: Initials - forecaster initials,  3 char.
    "er_start_tau": pl.Int16(), #: ERstartTAU - ER start time: 0 through 168 hours,  3 char.
    "er_stop_tau": pl.Int16(),  #: ERstopTAU - ER stop time: 0 through 168 hours,  3 char.
    "extra": pl.String()        #: Placeholder for any null values
})

edeck_schemas={'TR': track_schema,
               'IN': intensity_schema,
               'RI': rapid_intensification_schema,
               'RW': rapid_weakening_schema,
               'WR': wind_radii_schema,
               'PR': pressure_schema,
               'GN': genesis_schema,
               'GS': genesis_shape_schema,
               'ER': eyewall_replacement_schema}


if __name__ == "__main__":
    # Example usage
    import time
    input_filepath = "/Users/abrammer/repos/tciopy/data/eal202023.dat"  # Replace with your file path
    stime = time.time()
    decks = read_edeck(input_filepath, format_filter=["TR","IN"])
    print(time.time() - stime)
    for dtype, deck in decks.items():
        print(f"Deck type: {dtype}")
        print(deck.head())
        print(deck.schema)
        print(deck.columns)
        print(deck.shape)
        # You can also save to CSV or Parquet if needed
        # deck.write_csv(f"{key}_edeck.csv")
        # deck.write_parquet(f"{key}_edeck.parquet")