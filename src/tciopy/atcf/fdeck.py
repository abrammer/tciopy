import gzip
import re
import time
from datetime import datetime
from pathlib import Path
import logging
from itertools import zip_longest

import polars as pl

from tciopy.converters import tolatlon

LOGGER = logging.getLogger(__name__)

def read_fdeck(fname: str, format_filter: list[int] = None) -> dict[str, pl.DataFrame]:
    """Read an f-deck file into a polars DataFrame
    
    Parameters
    ----------
    fname : str or Path
        Path to the fdeck file. Can be gzipped.
    format_filter : list of int, optional
        List of fdeck format codes to read. If None, all formats are read.
        Valid formats are: 10, 20, 30, 31, 40, 50, 60, 70
    
    Returns
    -------
    dict[str, pl.DataFrame]
        Dictionary of Polars DataFrames containing the fdeck data, keyed by format code.
    """
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open

    allformats = fdeck_schemas.keys()
    alldata = {key: [] for key in allformats if not format_filter or key in format_filter}
    with opener(fname, "rt", newline="\n") as io_file:
        for line in io_file:
            try:
                splitline = re.split(r",\s+", line)
            except ValueError as e:
                LOGGER.warning("Failed to parse fdeck line from %s \n %s",fname, line)
            ftype = int(splitline[3])
            if ftype == 31:
                ftype = 30
            if ftype not in alldata.keys():
                continue
            schema = fdeck_schemas.get(ftype)
            if not schema:
                LOGGER.warning("Unrecognised type in fdeck line from %s \n %s", fname, line) 
            # this handles empty strings as Null for polars parsing
            # and also handles lines that are too short for their schema (frequent)
            alldata[ftype].append([None if val == "" else val for _, val in zip_longest(schema.keys(), splitline, fillvalue=None)])

    dfs = {}
    for key, data in alldata.items():
        datum =  pl.DataFrame(data, orient="row", schema=fdeck_schemas[key], strict=False)
        datum = datum.with_columns([
            (pl.col("datetime")).str.strptime(pl.Datetime, "%Y%m%d%H%M", strict=True),
            ])
        datum = tolatlon(datum, scale=0.01)
        dfs[key] = datum
    return dfs



"""Common fields for f-deck entries
    Check https://www.nrlmry.navy.mil/atcf_web/docs/database/new/newfdeck.txt 
    for more information
"""

fdeck_base_schema = {
    "basin": pl.String,
    #: WP, IO, SH, CP, EP, AL, LS                  2 char.
    "number": pl.String,
    #: annual cyclone number, 1-99,                    2 char.
    "datetime": pl.String,
    #: Fix date-time-group,                 12 char.
    "format": pl.Int8,
    #:* 10 - subjective dvorak |
    #:* 20 - objective dvorak|
    #:* 30 - microwave|
    #:* 31 - scatterometer|
    #:* 40 - radar|
    #:* 50 - aircraft|
    #:* 60 - dropsonde|
    #:* 70 - analysis|
    "type": pl.String,
    #: 4 char
    #:
        #: * DVTS - subjective Dvorak 
        #: * DVTO - objective Dvorak 
        #: * SSMI - Special Sensor Microwave Imager 
        #: * SSMS - SSM/IS (Shared Processing Program) 
        #: * QSCT - QuikSCAT 
        #: * ERS2 - European Remote Sensing System scatterometer 
        #: * SEAW - SeaWinds  scatterometer 
        #: * TRMM - Tropical Rainfall Measuring Mission microwave and vis/ir imager 
        #: * WIND - WindSat, polarimetric microwave radiometer 
        #: * MMHS - Micorwave Humidity Sounder 
        #: * ALTG - geosat altimeter 
        #: * AMSU - Advanced Microwave Sounding Unit 
        #: * RDRC - conventional radar 
        #: * RDRD - doppler radar 
        #: * RDRT - TRMM radar 
        #: * SYNP - synoptic 
        #: * AIRC - aircraft 
        #: * ANAL - analysis 
        #: * DRPS - dropsonde 
        #: * UNKN - unknown 
    "ftype": pl.String,
    #: 10 char
    #:
        #: *  C-center fix, lat/lon specifies a center fix position,|
        #: *  I-intensity fix, V specifies the max wind speed assoc. with the storm,|
        #: *  R-wind radii fix,|
        #: *  P-pressure, Pressure specifies central mean sea level pressure|
        #: *  N-none,|
        #: *  C, I, P, R, CI, CP, IP, CIP, N ...|
    "flag": pl.String,
    #: 1 char
        #: *  C-center location is flagged|
        #: *  I-intensity is flagged|
        #: *  R-wind radii is flagged|
        #: *  P-pressure is flagged|
        #: *  F-all of the above are flagged|
    "lat": pl.String,
    #:  N/S - Latitude (hundreths of degrees), 0-9000    5 char
    #:  N/S is the hemispheric index
    "lon": pl.String,
    #:E/W - Longitude (hundreths of degrees), 0-18000  6 char
    #:  E/W is the hemispheric index
    "height": pl.Int16,
    #:of ob - 0-99999 meters                        5 char
    "height_confidence": pl.Int8,
    #:  1-good, 2-fair, 3-poor            1 char
    "vmax": pl.Int16,
    #: Wind speed (kts), 0 - 300 kts                    3 char
    "vmax_confidence": pl.Int8,
    #: 1-good, 2-fair, 3-poor                1 char
    "pressure": pl.Int16,
    #: Pressure, 0 - 1050 mb.                    4 char
    "pressure_confidence": pl.Int8,
    #: 1-good, 2-fair, 3-poor         1 char
    "pressure_derivation": pl.String,
    #: 4 char
    #: | DVRK- dvorak |
    #: | AKHL- atkinson-holiday table |
    #: | XTRP- extrapolated |
    #: | MEAS- measured         |
    "rad": pl.Int8,
    #: Wind intensity (kts) - 34, 50, 64              3 char
    #:  for the radii defined in this record
    "windcode": pl.String,
    #: Radius code                              4 char
    #:   AAA - full circle
    #:   NEQ - northeast quadrant
    #:   SEQ - southeast quadrant
    #:   SWQ - southwest quadrant
    #:   NWQ - northwest quadrant
    "rad1": pl.Int16,
    #: If full circle, radius of specified wind intensity
    #:Otherwise radius of specified wind intensity for northeast
    #:quadrant of circle.  0 - 1200 nm.             4 char
    "rad2": pl.Int16,
    "rad3": pl.Int16,
    "rad4": pl.Int16,
    "rad_mod1": pl.String,
    #: Modifier for rad1
    #: E-Edge of Pass, C-Cut off by Land, B-Both  1 char
    "rad_mod2": pl.String,
    "rad_mod3": pl.String,
    "rad_mod4": pl.String,
    "radii_confidence": pl.Int8,
    #: 1-good, 2-fair, 3-poor            1 char
    "rmw": pl.Int16,
    #: radius of max winds 0-999 nm                   3 char
    "eye": pl.Int16,
    #:  eye diameter, 0-120 nm.                        3 char
    "subregion": pl.String,
    #:  subregion code W,A,B,S,P,C,E,L,Q        1 char
        #: | A - Arabian Sea |
        #: | B - Bay of Bengal |
        #: | C - Central Pacific |
        #: | E - Eastern Pacific |
        #: | L - Atlantic |
        #: | P - South Pacific (135E - 120W) |
        #: | Q - South Atlantic |
        #: | S - South IO (20E - 135E) |
        #: | W - Western Pacific |
    "fix_identifier": pl.String,
    #:                            5 char
    "initials": pl.String,
    #: (fix enterer) -                             3 char
}

SatelliteDVTS_schema = fdeck_base_schema.copy()
SatelliteDVTS_schema.update({
    "dvts_sensor_type": pl.String,
    "dvts_pcn_code": pl.String,
    "dvts_dvorak_code_long": pl.String,
    "dvts_dvorak_code_short": pl.String,
    "dvts_ci_24hr_forecast": pl.Float64,
    "satellite_type": pl.String,
    "dvts_center_type": pl.String,
    "dvts_tropical_indicator": pl.String,
    "comments": pl.String,
})

SatelliteDVTO_schema = fdeck_base_schema.copy()
SatelliteDVTO_schema.update({
    "dvto_sensor_type": pl.String,
    "dvto_ci_num": pl.Int8,
    "dvto_ci_confidence": pl.Int8,
    "dvto_t_num_mean": pl.Int8,
    "dvto_t_num_time_period": pl.Int8,
    "dvto_t_num_derivation": pl.String,
    "dvto_t_num_raw": pl.Int8,
    "dvto_temperature_eye": pl.Int8,
    "dvto_temperature_cloud": pl.Int8,
    "dvto_scene_type": pl.String,
    "dvto_algorithm": pl.String,
    "dvto_satellite_type": pl.String,
    "dvto_tropical_indicator": pl.String,
    "comments": pl.String,
})

MicrowaveData_schema = fdeck_base_schema.copy()
MicrowaveData_schema.update({
    "microwave_rain_flagged": pl.String,
    "microwave_rainrate": pl.Int16,
    "microwave_process": pl.String,
    "microwave_wave_height": pl.Int16,
    "micrwowave_temperature": pl.Int16,
    "microwave_slp_raw": pl.Int16,
    "microwave_slp_retrieved": pl.Int16,
    "microwave_max_meas": pl.Int16,
    "microwave_satellite": pl.String,
    "microwave_rad_kt": pl.Int16,
    "micrwowave_windcode": pl.String,
    "microwave_rad1": pl.Int16,
    "microwave_rad2": pl.Int16,
    "microwave_rad3": pl.Int16,
    "microwave_rad4": pl.Int16,
    "microwave_rad5": pl.Int16,
    "microwave_rad6": pl.Int16,
    "microwave_rad7": pl.Int16,
    "microwave_rad8": pl.Int16,
    "microwave_rad1_mod": pl.String,
    "microwave_rad2_mod": pl.String,
    "microwave_rad3_mod": pl.String,
    "microwave_rad4_mod": pl.String,
    "microwave_rad5_mod": pl.String,
    "microwave_rad6_mod": pl.String,
    "microwave_rad7_mod": pl.String,
    "microwave_rad8_mod": pl.String,
    "microwave_radii_confidence": pl.Int8,
    "comments": pl.String,
    })

# RADAR

#   RDRC, RDRD, RDRT -- Radar, Conventional, Doppler and TRMM
#       Radar Type:                                    1 char.
#           L - Land; S - Ship; A - Aircraft; T - Satellite
#       Radar Format:                                  1 char.
#           R - RADOB; P - plain language; D - Doppler
#       RADOB CODE - A S W a r t d d f f              10 char.
#                    c c c c t e s s s s
#           See description below.
#           Also enter slashes if reported and in place of blanks.
#       Eye Shape -                                    2 char.
#           CI - Circular; EL - Elliptic; CO - concentric
#       Percent of Eye Wall Observed (99 = 100%) -     2 char.
#       Spiral Overlay (degrees) -                     2 char.
#       Radar Site Position Lat N/S -                  5 char.
#                           Lon E/W -                  6 char.
#       Inbound Max Wind - 0-300 kts                   3 char.
#          Azimuth - degrees, 1-360                    3 char.
#          Range - less than 400 nm,                   3 char.
#          Elevation - feet                            5 char.
#       Outbound Max Wind - 0-300 kts                  3 char.
#          Azimuth - degrees, 1-360                    3 char.
#          Range - less than 400 nm,                   3 char.
#          Elevation - feet                            5 char.
#       Max Cloud Height (trmm radar) - 70,000ft       5 char.
#       Rain accumulation:
#         Max. rain accumulation, hundreths of inches 0-10000 5 char.
#         Time interval, 1 - 120 hours                 3 char.
#         Lat N/S - Latitude (hundreths of degrees), 0-9000   5 char.
#             N/S is the hemispheric index.
#         Lon E/W - Longitude (hundreths of degrees), 0-18000 6 char.
#             E/W is the hemispheric index.
#       Comments -                                    52 char.

#  Note:  The greater of the two inbound and outbound winds should
#         be assigned to the V field.  It is not mandatory to have both 
#         inbound and outbound winds.

Radar_schema = fdeck_base_schema.copy()
Radar_schema.update({
    "radar_type": pl.String,
    "radar_format": pl.String,
    "radob_code": pl.String,
    "eye_shape": pl.String,
    "percent_eye_wall_observed": pl.Int8,
    "spiral_overlay": pl.Int16,
    "radar_site_lat": pl.String,
    "radar_site_lon": pl.String,
    "inbound_max_wind": pl.Int16,
    "inbound_azimuth": pl.Int16,
    "inbound_range": pl.Int16,
    "inbound_elevation": pl.Int32,
    "outbound_max_wind": pl.Int16,
    "outbound_azimuth": pl.Int16,
    "outbound_range": pl.Int16,
    "outbound_elevation": pl.Int32,
    "max_cloud_height": pl.Int32,
    "max_rain_accumulation": pl.Int32,
    "max_rain_time_interval": pl.Int16,
    "max_rain_lat": pl.String,
    "max_rain_lon": pl.String,
    "comments": pl.String,
})


# AIRCRAFT

#     AIRC -- Aircraft
#       Flight Level
#          Flight Level - 100's of feet                2 char.
#          Flight Level - millibars                    3 char.
#          Minimum height - meters                     4 char.
#       Maximum Surface Wind (inbound leg)
#          Intensity - kts                             3 char.
#          Bearing - degrees                           3 char.
#          Range - nm                                  3 char.
#       Maximum Flight Level Wind (inbound leg)
#          Direction - degrees                         3 char.
#          Intensity - kts                             3 char.
#          Bearing - degrees                           3 char.
#          Range - nm                                  3 char.
#       Minimum Sea Level Pressure - millibars         4 char.
#       Temperature Outside Eye  -99 to 99 Celsius     3 char.
#       Temperature Inside Eye   -99 to 99 Celsius     3 char.
#       Dew Point Temperature    -99 to 99 Celsius     3 char.
#       Sea Surface Temperature    0 to 40 Celsius     2 char.
#       Eye Character or Wall Cld Thickness (pre-2015) 2 char.
#          NA - < 50% eyewall
#          CL - Closed Wall
#          PD - Poorly Defined
#          N  - open North
#          NE - open Northeast
#          E  - open East     
#          SE - open Southeast
#          S  - open South
#          SW - open Southwest
#          W  - open West
#          NW - open Northwest
#          SB - spiral band 
#       Eye Shape/Orientation/Diameter
#          Shape: CI-circ.; EL-elliptic; CO-concentric 2 char.
#          Orientation - degrees                       3 char.
#          Diameter (long axis if elliptical) - nm     2 char.
#          Short Axis (blank if not elliptical) - nm   2 char.
#       Accuracy
#          Navigational - tenths of nm                 3 char.
#          Meteorological - tenths of nm               3 char.
#       Mission Number                                 2 char.
#       Comments -                                    52 char.

Aircraft_schema = fdeck_base_schema.copy()
Aircraft_schema.update({
    "flight_level_feet": pl.Int16,
    "flight_level_hPa": pl.Int16,
    "minimum_height_meters": pl.Int32,
    "max_surface_wind_intensity": pl.Int16,
    "max_surface_wind_bearing": pl.Int16,
    "max_surface_wind_range": pl.Int16,
    "max_flight_level_wind_direction": pl.Int16,
    "max_flight_level_wind_intensity": pl.Int16,
    "max_flight_level_wind_bearing": pl.Int16,
    "max_flight_level_wind_range": pl.Int16,
    "min_sea_level_pressure": pl.Int16,
    "temperature_outside_eye_celsius": pl.Int8,
    "temperature_inside_eye_celsius": pl.Int8,
    "dew_point_temperature_celsius": pl.Int8,
    "sea_surface_temperature_celsius": pl.Int8,
    "eye_character": pl.String,
    "eye_shape": pl.String,
    "eye_orientation_degrees": pl.Int16,
    "eye_diameter_nm": pl.Int16,
    "eye_short_axis_nm": pl.Int16,
    "accuracy_navigational_tenths_nm": pl.Int16,
    "accuracy_meteorological_tenths_nm": pl.Int16,
    "mission_number": pl.String,
    "comments": pl.String,
})

# DROPSONDE

#   DRPS -- Dropsondes
#       Sonde environment -                           10 char.
#         EYEWALL, EYE, RAINBAND, MXWNDBND, SYNOPTIC
#       Height of midpoint over lowest 150 m of drop,
#         meters (75 - 999 m)                          3 char.
#       Speed of mean wind, lowest 150 m of drop - kts 3 char.
#       Speed of mean wind, 0-500 m layer - kts        3 char.
#       Comments -                                    52 char.

Dropsonde_schema = fdeck_base_schema.copy()
Dropsonde_schema.update({
    "sonde_environment": pl.String,
    "height_midpoint_lowest_150m_meters": pl.Int16,
    "speed_mean_wind_lowest_150m_kts": pl.Int16,
    "speed_mean_wind_0_500m_kts": pl.Int16,
    "comments": pl.String,
})

# ANALYSIS

#   ANAL -- Analysis( HRD, personal, aircraft, model(GFD) )
#       Analyst initials -                             3 char.
#       Start time - YYYYMMDDHHMM                     12 char.
#       End time -  YYYYMMDDHHMM                      12 char.
#       Distance to Nearest Data, 0 - 300 nm           3 char.
#       SST - celsius                                  4 char
#       Observation sources -                         24 char.       
#          b - buoy, 
#          l - land station, 
#          m - SSMI, 
#          c - scat, 
#          t - trmm, 
#          i - ir, 
#          v - vis, 
#          p - ship, 
#          d - dropsonde, 
#          a - aircraft, 
#          r - radar, 
#          x - other
#          1 char. identifier for each ob source, concatenate
#          the selected 1 char. identifiers to list all of the
#          sources.
#       Comments -                                    52 char.

# -----------------------------------------------------------------
Analysis_schema = fdeck_base_schema.copy()
Analysis_schema.update({
    "analyst_initials": pl.String,
    "start_time": pl.String,
    "end_time": pl.String,
    "distance_nearest_data_nm": pl.Int16,
    "sst_celsius": pl.Int16,
    "observation_sources": pl.String,
    "comments": pl.String,
})

fdeck_schemas = {
    10: SatelliteDVTS_schema,
    20: SatelliteDVTO_schema,
    30: MicrowaveData_schema,
    31: MicrowaveData_schema,
    40: Radar_schema,
    50: Aircraft_schema,
    60: Dropsonde_schema,
    70: Analysis_schema,
}

def main():
    "demo function"
    stime = time.time()
    datadir = Path(__file__).parent.parent.parent.parent / "data"
    df = read_fdeck(datadir / "fal132023.dat", )
    print(time.time() - stime)
    print(df)  # [df["format"] == 30].dropna(axis="columns", how="all"))


if __name__ == "__main__":
    main()
