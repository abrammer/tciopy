import gzip
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from tciopy.atcf.decks import BaseDeck
from tciopy.converters import StringColumn, NumericColumn, CategoricalColumn, LatLonColumn, DatetimeColumn



def read_edeck(fname: str) -> pd.DataFrame:
    """Read an f-deck file into a pandas DataFrame"""
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open

    alldata = {
        "TR": TrackEDeck(), #- track, "03" also accepted for existing old edeck files
        "03": TrackEDeck(), #- track, "03" also accepted for existing old edeck files
        "IN": IntensityEDeck(), # - intensity
        "RI": RapidIntensificationEDeck(), # - rapid intensification
        "RW": RapidWeakeningEDeck(), # - rapid weakening
        "WR": WindRadiiEDeck(), # - wind radii
        "PR": IntensityEDeck(), # - pressure
        "GN": GenesisEDeck(), # - TC genesis probability
        "GS": GenesisShapeEDeck(), # - TC genesis shape
        "ER": EyewallReplacementEDeck(), # - eyewall replacement
    }
    with opener(fname, "rt", newline="\n") as io_file:
        for line in io_file:
            splitline = re.split(r",\s+", line)
            ftype = splitline[3]
            alldata[ftype].append(splitline[:alldata[ftype]._num_columns])

    dfs = [value.to_dataframe() for value in alldata.values()]
    df = pd.concat(dfs, ignore_index=True, sort=False)
    return df

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
#         Eyeway Replacement forecast probabilities

class EDeckCommon(BaseDeck):
    """Common fields for e-deck entries
        Check https://www.nrlmry.navy.mil/atcf_web/docs/database/new/edeck.txt 
        for more information
    """
    def __init__(self):
        self.basin : str = StringColumn()
        self.number: int = NumericColumn()
        #: Fix date-time-group,                 12 char.
        self.datetime: datetime = DatetimeColumn(datetime_format="%Y%m%d%H")
        #: ProbFormat - 2 char.
        #:*   TR - track, "03" also accepted for existing old edeck files
        #:*         IN - intensity
        #:*         RI - rapid intensification
        #:*         RW - rapid weakening
        #:*   WR - wind radii
        #:*   PR - pressure
        #:*   GN - TC genesis probability
        #:*   GS - TC genesis shape
        #:*         ER - eyewall replacement
        self.format: str = StringColumn()
        #: Tech - acronym for each objective technique,  4 char.
        self.tech : str = StringColumn()
        # TAU - forecast period: 0 through 168 hours,  3 char.
        self.tau : int = NumericColumn()
        # LatN/S - Latitude (tenths of degrees) for the DTG: 0 through 900, N/S is the hemispheric index,  4 char.
        self.lat : float = LatLonColumn(scale=0.1)
        # LonE/W - Longitude (tenths of degrees) for the DTG: 0 through 1800, E/W is the hemispheric index,  5 char.
        self.lon : float = LatLonColumn(scale=0.1)
        # Prob - probability of ProbItem (see parameter specific definition of ProbItem), 0 - 100%,  3 char.
        self.prob : float = NumericColumn()


# TRACK RELATED VALUES 
class TrackEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - Probability radius, 0 through 2000 nm,  4 char.
        self.probitem : float = NumericColumn()
        #: TY - Level of tc development: currently unused,  2 char.
        self.ty : str = StringColumn()
        #: Dir - Cross track direction, 0 through 359 degrees,  3 char.
        self.dir : int = NumericColumn()
        #: WindCode - Radius code: currently unused,  3 char.
        self.windcode : str = StringColumn()
        #: rad_cross - Cross track radius, 0 through 2000 nm,  4 char.
        self.rad_cross : float = NumericColumn()
        #: rad_along - Along track radius, 0 through 2000 nm,  4 char.
        self.rad_along : float = NumericColumn()
        #: bias_cross - Cross track bias, -999 through 999 nm,  4 char.
        self.bias_cross : float = NumericColumn()
        #: bias_along - Along track bias, -999 through 999 nm,  4 char.
        self.bias_along : float = NumericColumn()


# WIND RADII PROBABILITY
class WindRadiiEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - Wind speed (bias adjusted), 0 - 300 kts,  3 char.
        self.probitem : float = NumericColumn()
        #: TH - Wind Threshold (e.g., 34),  2 char.
        self.th : int = NumericColumn()
        #: Half_Range - Half the probability range (radius in n mi), 15 - 200 n mi,  4 char.
        self.half_range : float = NumericColumn()


# INTENSITY PROBABILITY
class IntensityEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - Wind speed (bias adjusted), 0 - 300 kts,  3 char.
        self.probitem : float = NumericColumn()
        #: TY - Level of tc development: currently unused,  2 char.
        self.ty : str = StringColumn()
        #: Half_Range - Half the probability range (radius), 0 - 50 kts,  4 char.
        self.half_range : float = NumericColumn()


# RAPID INTENSIFICATION PROBABILITY
class RapidIntensificationEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - intensity change, 0 - 300 kts,  3 char.
        self.probitem : float = NumericColumn()
        #: V - final intensity, 0 - 300 kts,  3 char.
        self.v : float = NumericColumn()
        #: Initials - forecaster initials,  3 char.
        self.initials : str = StringColumn()
        #: RIstartTAU - RI start time: 0 through 168 hours,  3 char.
        self.ri_start_tau : int = NumericColumn()
        #: RIstopTAU - RI stop time: 0 through 168 hours,  3 char.
        self.ri_stop_tau : int = NumericColumn()


# RAPID WEAKENING PROBABILITY
class RapidWeakeningEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - intensity change, 0 - 300 kts,  3 char.
        self.probitem : float = NumericColumn()
        #: V - final intensity, 0 - 300 kts,  3 char.
        self.v : float = NumericColumn()
        #: Initials - forecaster initials,  3 char.
        self.initials : str = StringColumn()
        #: RWstartTAU - RW start time: 0 through 168 hours,  3 char.
        self.rw_start_tau : int = NumericColumn()
        #: RWstopTAU - RW stop time: 0 through 168 hours,  3 char.
        self.rw_stop_tau : int = NumericColumn()


# TC GENESIS PROBABILITY
class GenesisEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - time period, ie genesis during next xxx hours, 0 for genesis or dissipate event, 0 - 240 hrs,  4 char.
        self.probitem : float = NumericColumn()
        #: Initials - forecaster initials,  3 char.
        self.initials : str = StringColumn()
        #: GenOrDis - "invest", "genFcst", "genesis", "disFcst" or "dissipate"
        self.gen_or_dis : str = StringColumn()
        #: DTG - Genesis or dissipated event Date-Time-Group, yyyymmddhhmm: 0000010100 through 9999123123,  12 char.
        self.dtg : datetime = DatetimeColumn(datetime_format="%Y%m%d%H%M")
        #: stormID - cyclone ID if the genesis developed into an invest area or cyclone ID of dissipated TC, e.g. al032014
        self.storm_id : str = StringColumn()
        #: min - minutes, associated with DTG in common fields (3rd field in record), 0 - 59 min
        self.min : int = NumericColumn()
        #: genesisNum - genesis number, if spawned from a genesis area (1-999)
        self.genesis_num : int = NumericColumn()
        #: undefined - TBD
        self.undefined : str = StringColumn()


# TC GENESIS SHAPE
class GenesisShapeEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - time period, ie genesis during next xxx hours, 0 - 240 hrs,  4 char.
        self.probitem : float = NumericColumn()
        #: Initials - forecaster initials,  3 char.
        self.initials : str = StringColumn()
        #: TCFAMANOPDTG - TCFA MANOP dtg, ddhhmm
        self.tcfamanopdtg : datetime = DatetimeColumn(datetime_format="%d%H%M")
        #: TCFAMSGDTG - TCFA message dtg, yymmddhhmm
        self.tcfamsgdtg : datetime = DatetimeColumn(datetime_format="%y%m%d%H%M")
        #: TCFAWTNUM - TCFA WT number,  2 char.
        self.tcfawtnum : int = NumericColumn()
        #: ShapeType - shape type, ELP - ellipse, BOX - box, CIR - circle, PLY - polygon,  3 char.
        self.shapetype : str = StringColumn()
        #: EllipseAngle - cross track angle for ellipse (math coords), 3 char.
        self.ellipseangle : int = NumericColumn()
        #: EllipseRCross - cross track radius, 0 - 2000 nm,  4 char.
        self.ellipsercross : float = NumericColumn()
        #: EllipseRAlong - along track radius, 0 - 2000 nm,  4 char.
        self.ellipseralong : float = NumericColumn()
        #: Box1LatN/S - Latitude for start point for TCFA box center line or center point for TCFA circle,  4 char.
        #: 	     0 - 900 tenths of degrees
        #:              N/S is the hemispheric index
        self.box1latns : float = LatLonColumn(scale=0.1)
        #: Box1LonE/W - Longitude for start point for TCFA box center line or center point for TCFA circle,  5 char.
        #: 	     0 - 1800 tenths of degrees
        #:              E/W is the hemispheric index
        self.box1lonew : float = LatLonColumn(scale=0.1)
        #: Box2LatN/S - Latitude for end point for TCFA box center line, not used for TCFA circle,  4 char.
        #: 	     0 - 900 tenths of degrees
        #:              N/S is the hemispheric index
        self.box2latns : float = LatLonColumn(scale=0.1)
        #: Box2LonE/W - Longitude for start point for TCFA box center line, not used for TCFA circle,  5 char.
        #: 	     0 - 1800 tenths of degrees
        #:              E/W is the hemispheric index
        self.box2lonew : float = LatLonColumn(scale=0.1)
        #: TCFARADIUS - distance from center line to box edge, or radius of circle (nm),  3 char.
        self.tcfaradius : float = NumericColumn()
        #: PolygonPts - array of 20 lat, lon points defining a polygon
        self.polygonpts : str = StringColumn()


# EYEWALL REPLACEMENT PROBABILITY
class EyewallReplacementEDeck(EDeckCommon):
    def __init__(self):
        super().__init__()
        #: ProbItem - intensity change, 0 - 300 kts,  3 char.
        self.probitem : float = NumericColumn()
        #: V - final intensity, 0 - 300 kts,  3 char.
        self.v : float = NumericColumn()
        #: Initials - forecaster initials,  3 char.
        self.initials : str = StringColumn()
        #: ERstartTAU - RW start time: 0 through 168 hours,  3 char.
        self.er_start_tau : int = NumericColumn()
        #: ERstopTAU - RW stop time: 0 through 168 hours,  3 char.
        self.er_stop_tau : int = NumericColumn()


