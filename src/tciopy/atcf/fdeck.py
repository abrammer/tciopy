import gzip
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from tciopy.converters import (
    DatetimeConverter,
    FloatConverter,
    IntConverter,
    LatLonConverter,
    StrConverter,
)


def read_fdeck(fname: str | Path) -> pd.DataFrame:
    """Read an f-deck file into a pandas DataFrame"""
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open

    alldata = defaultdict(list)
    with opener(fname, "rt", newline="\n") as io_file:
        for line in io_file:
            splitline = re.split(r",\s+", line)
            ftype = int(splitline[3])
            if ftype == 70:
                _fields = Analysis(*splitline)
            elif ftype == 60:
                _fields = Dropsonde(*splitline)
            elif ftype == 50:
                _fields = Aircraft(*splitline)
            elif ftype == 40:
                _fields = Radar(*splitline)
            elif ftype == 30 or ftype == 31:
                _fields = MicrowaveData(*splitline)
            elif ftype == 20:
                _fields = SatelliteDVTO(*splitline)
            elif ftype == 10:
                _fields = SatelliteDVTS(*splitline)
            else:
                continue
            alldata[ftype].append(_fields)

    dfs = [pd.DataFrame(value) for value in alldata.values()]
    df = pd.concat(dfs, ignore_index=True)
    return df


@dataclass
class CommonFields:
    """Common fields for f-deck entries"""

    basin: str = StrConverter()
    # WP, IO, SH, CP, EP, AL, LS                  2 char.
    number: int = IntConverter()
    # - annual cyclone number, 1-99,                    2 char.
    datetime: datetime = DatetimeConverter(datetime_format="%Y%m%d%H%M")
    # = Fix date-time-group,                 12 char.
    format: int = IntConverter()
    # IntConvertor #= IntConvertor(default=0)
    # - 10 - subjective dvorak                  3 char.
    #  20 - objective dvorak
    #  30 - microwave
    #  31 - scatterometer
    #  40 - radar
    #  50 - aircraft
    #  60 - dropsonde
    #  70 - analysis
    type: str = StrConverter()
    #    DVTS - subjective Dvorak                  4 char.
    #    DVTO - objective Dvorak
    #    SSMI - Special Sensor Microwave Imager
    #    SSMS - SSM/IS (Shared Processing Program)
    #    QSCT - QuikSCAT
    #    ERS2 - European Remote Sensing System scatterometer
    #    SEAW - SeaWinds  scatterometer
    #    TRMM - Tropical Rainfall Measuring Mission microwave and vis/ir imager
    #    WIND - WindSat, polarimetric microwave radiometer
    #    MMHS - Micorwave Humidity Sounder
    #    ALTG - geosat altimeter
    #    AMSU - Advanced Microwave Sounding Unit
    #    RDRC - conventional radar
    #    RDRD - doppler radar
    #    RDRT - TRMM radar
    #    SYNP - synoptic
    #    AIRC - aircraft
    #    ANAL - analysis
    #    DRPS - dropsonde
    #    UNKN - unknown
    ftype: str = StrConverter()
    # -                                  10 char.
    #   C-center fix, lat/lon specifies a center fix position,
    #   I-intensity fix, V specifies the max wind speed assoc. with the storm,
    #   R-wind radii fix,
    #   P-pressure, Pressure specifies central mean sea level pressure
    #   N-none,
    #   C, I, P, R, CI, CP, IP, CIP, N ...
    flag: str = StrConverter()
    # -                                  1 char.
    #   C-center location is flagged,
    #   I-intensity is flagged,
    #   R-wind radii is flagged,
    #   P-pressure is flagged,
    #   F-all of the above are flagged,
    #   " "-fix is not flagged.
    lat: float = LatLonConverter(scale=0.01)
    # N/S - Latitude (hundreths of degrees), 0-9000    5 char.
    #   N/S is the hemispheric index.
    lon: float = LatLonConverter(scale=0.01)
    # E/W - Longitude (hundreths of degrees), 0-18000  6 char.
    #   E/W is the hemispheric index.
    height: int = IntConverter()
    # of ob - 0-99999 meters                        5 char.
    height_confidence: int = IntConverter()
    #  - 1-good, 2-fair, 3-poor            1 char.
    vmax: int = IntConverter()
    # - Wind speed (kts), 0 - 300 kts                    3 char.
    vmax_confidence: int = IntConverter()
    # - 1-good, 2-fair, 3-poor                1 char.
    pressure: int = IntConverter()
    # - Pressure, 0 - 1050 mb.                    4 char.
    pressure_confidence: int = IntConverter()
    # - 1-good, 2-fair, 3-poor         1 char.
    pressure_derivation: str = StrConverter()
    # -                                4 char.
    #   DVRK - dvorak,
    #   AKHL - atkinson-holiday table,
    #   XTRP - extrapolated,
    #   MEAS - measured,
    rad: int = IntConverter()
    #  - Wind intensity (kts) - 34, 50, 64              3 char.
    #   for the radii defined in this record.
    windcode: str = StrConverter()
    # - Radius code:                              4 char.
    #    AAA - full circle
    #    NEQ - northeast quadrant
    rad1: int = IntConverter()
    # - If full circle, radius of specified wind intensity.
    # Otherwise radius of specified wind intensity for northeast
    # quadrant of circle.  0 - 1200 nm.             4 char.
    rad2: int = IntConverter()
    # - Radius of specified wind intensity for 2nd quadrant
    # (southeast quadrant).  0 - 1200 nm.           4 char.
    rad3: int = IntConverter()
    # - Radius of specified wind intensity for 3rd quadrant
    # (southwest quadrant).  0 - 1200 nm.           4 char.
    rad4: int = IntConverter()
    # - Radius of specified wind intensity for 4th quadrant
    # (northwest quadrant).  0 - 1200 nm.           4 char.
    rad_mod1: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    rad_mod2: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    rad_mod3: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    rad_mod4: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    radii_confidence: int = IntConverter()
    # - 1-good, 2-fair, 3-poor            1 char.
    rmw: int = IntConverter()
    # - radius of max winds 0-999 nm                   3 char.
    eye: int = IntConverter()
    # - eye diameter, 0-120 nm.                        3 char.
    subregion: str = StrConverter()
    # - subregion code: W,A,B,S,P,C,E,L,Q        1 char.
    # A - Arabian Sea
    # B - Bay of Bengal
    # C - Central Pacific
    # E - Eastern Pacific
    # L - Atlantic
    # P - South Pacific (135E - 120W)
    # Q - South Atlantic
    # S - South IO (20E - 135E)
    # W - Western Pacific
    fix_identifier: str = StrConverter()
    # -                            5 char.
    initials: str = StrConverter()
    # (fix enterer) -                             3 char.


@dataclass
class SatelliteDVTS(CommonFields):
    "Extra Satellite DVTS data columns"
    dvts_sensor_type: str = StrConverter()
    # - vis, ir, microwave...            4 char.
    dvts_pcn_code: str = StrConverter()
    # (deprecated) -                        1 char.
    # V, I, M, VI, IM, VM, VIM
    #  1 = Eye/Geography
    #  2 = Eye/Ephemeris
    #  3 = Well Defined Circ. Center/Geography
    #  4 = Well Definged Circ. Center/Ephemeris
    #  5 = Poorly Defined Circ. Center/Geography
    dvts_dvorak_code_long: str = StrConverter()
    # - long term trend,                10 char.
    #  6 = Poorly Defined Circ. Center/Ephemeris
    #  (18-30 hour change in t-number)
    #  T num, none or 0.0 - 8.0
    #  CI num, none or 0.0 - 8.0
    #  Forecast intensity change, + - or blank
    #  Past change - developed, steady, weakened (long term trend)
    #  Amount of t num change, none or 0.0 - 8.0
    #  Hours since previous eval,  18 - 30 HRS
    #  Example: T4.0/4.0+/D1.0/24HRS
    dvts_dvorak_code_short: str = StrConverter()
    # - short term trend,                5 char.
    #  Entry: 4040+D1024
    #  ( < 18 hour change in t-number.  Only used when
    #    significant difference from long term trend, i.e.,
    #    LTT is +1.0 strengthening, but STT over 6 hours
    #    shows - 0.5 weakening.)
    #  Past change - developed, steady, weakened (short term trend)
    #  Amount of t num change, none or 0.0 - 8.0
    #  Hours since previous eval,  0 - 17 HRS
    #  Example: W0.5/06HRS
    dvts_ci_24hr_forecast: float = FloatConverter()
    # - none or 0.0 - 8.0          2 char.
    #  Entry: W0506
    satellite_type: str = StrConverter()
    # -                               6 char.
    dvts_center_type: str = StrConverter()
    # - CSC, LLCC, ULCC                  4 char.
    #    GMS, DMSP, DMSP45, TRMM, NOAA...
    #    LLCC - lower level cloud center
    #    ULCC - upper level cloud center
    dvts_tropical_indicator: str = StrConverter()
    # -                           1 char.
    #    CSC - cloud system center
    comments: str = StrConverter()
    # -                                    52 char.
    #    S-subtropical, E-extratropical, T-tropical


@dataclass
class SatelliteDVTO(CommonFields):
    "Extra Satellite DVTO data columns"
    dvto_sensor_type: str = StrConverter()
    # - vis, ir, microwave...            4 char.
    # V_ I, M, VI, IM, VM, VIM
    dvto_ci_num: int = IntConverter()
    # -                                       2 char.
    dvto_ci_confidence: int = IntConverter()
    # - 1-good, 2-fair, 3-poor,        1 char.
    dvto_t_num_mean: int = IntConverter()
    #  (average)                                2 char.
    dvto_t_num_time_period: int = IntConverter()
    # - hours            3 char.
    dvto_t_num_derivation: str = StrConverter()
    # -                   1 char.
    # L_straight linear, T-time weighted
    dvto_t_num_raw: int = IntConverter()
    # (raw)                                    2 char.
    dvto_temperature_eye: int = IntConverter()
    # , -99 - 50 celsius            4 char.
    dvto_temperature_cloud: int = IntConverter()
    # surrounding eye) - celsius  4 char.
    dvto_scene_type: str = StrConverter()
    # -  CDO, EYE, EEYE, SHER...          4 char.
    # CDO_- central dense overcast
    # EYE_- definable eye
    # EMBC_- embedded center
    # SHER_- partially exposed eye due to strong wind shear
    # with_asymmetric convective structure
    dvto_algorithm: str = StrConverter()
    # _(Rule 9 flag, Rapid flag) - R9, RP   2 char.
    dvto_satellite_type: str = StrConverter()
    # -                               6 char.
    # GMS_ DMSP, DMSP45, TRMM, NOAA...
    dvto_tropical_indicator: str = StrConverter()
    # -                           1 char.
    # S_subtropical, E-extratropical, T-tropical
    dvto_comments: str = StrConverter()
    # -                                    52 char.


@dataclass
class MicrowaveData(CommonFields):
    "Extra Microwave data columns"
    # SSMI, TRMM, AMSU, ADOS, ALTI, ERS2, QSCT, SEAW -- MICROWAVE
    microwave_rain_flagged: str = StrConverter()
    # - "R" or blank                    1 char.
    microwave_rainrate: int = IntConverter()
    # -   0-500 mm/h                        3 char.
    microwave_process: str = StrConverter()
    # -                                      6 char.
    #  FNMOC algorithm, NESDIS algorithm, RSS...
    microwave_wave_height: int = IntConverter()
    ## (active micr) - 0-99 ft            2 char.
    microwave_temp: int = IntConverter()
    # (passive micr) - celsius                  4 char.
    microwave_slp_raw: int = IntConverter()
    # (raw, AMSU only) - mb                      4 char.
    microwave_slp_retrieved: int = IntConverter()
    # (retrieved, AMSU only) - mb                4 char.
    microwave_max_meas: int = IntConverter()
    # - (alti) 0-999 ft.                    3 char.
    microwave_satellite: str = StrConverter()
    # type -                               6 char.
    #    GMS, DMSP, DMSP45, TRMM, NOAA...
    microwave_rad_kt: int = IntConverter()
    # - Wind intensity (kts) - 34, 50, 64        3 char.
    # for the radii defined in this record.
    microwave_windcode: str = StrConverter()
    # - Radius code:                        4 char.
    #    AAA - full circle
    #   quadrant designations:
    #    NEQ - northeast quadrant
    #   octant designations:
    #    XXXO - octants (NNEO, ENEO, ESEO, SSEO, SSWO, WSWO, WNWO NNWO)
    microwave_rad1: int = IntConverter()
    # - If full circle, radius of specified wind intensity.  Otherwise
    #  radius of specified wind intensity of wind intensity of circle
    #  portion specified in windcode.  0 - 1200 nm.  4 char.
    microwave_rad2: int = IntConverter()
    # - Radius of specified wind intensity for 2nd quadrant/octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad3: int = IntConverter()
    # - Radius of specified wind intensity for 3rd quadrant/octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad4: int = IntConverter()
    # - Radius of specified wind intensity for 4th quadrant/octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad5: int = IntConverter()
    # - Radius of specified wind intensity for 5th octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad6: int = IntConverter()
    # - Radius of specified wind intensity for 6th octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad7: int = IntConverter()
    # - Radius of specified wind intensity for 7th octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad8: int = IntConverter()
    # - Radius of specified wind intensity for 8th octant
    #  (counting clockwise from quadrant specified in windcode).
    #  0 - 1200 nm.                                  4 char.
    microwave_rad_mod1: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod2: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod3: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod4: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod5: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod6: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod7: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_rad_mod8: str = StrConverter()
    # - E-Edge of Pass, C-Cut off by Land, B-Both  1 char.
    microwave_radii_confidence: int = IntConverter()
    # confidence - 1-good, 2-fair, 3-poor        1 char.
    comments: str = StrConverter()  # -                                    52 char.


@dataclass
class Radar(CommonFields):
    "Extra Radar data columns"
    # RDRC, RDRD, RDRT -- Radar, Conventional, Doppler and TRMM
    radar_type: str = StrConverter()
    #:                                    1 char.
    # L - Land; S - Ship; A - Aircraft; T - Satellite
    radar_format: str = StrConverter()
    #:                                  1 char.
    # R - RADOB; P - plain language; D - Doppler
    radob_code: str = StrConverter()
    # - A S W a r t d d f f              10 char.
    # c c c c t e s s s s
    # See description below.
    # Also enter slashes if reported and in place of blanks.
    eye_shape: str = StrConverter()
    # -                                    2 char.
    # CI - Circular; EL - Elliptic; CO - concentric
    radar_percent_of_eye_wall_observed: int = IntConverter()
    # (99 = 100%) -     2 char.
    radar_spiral_overlay: int = IntConverter()
    # (degrees) -                     2 char.
    radar_site_position_lat: float = LatLonConverter(scale=0.01)
    # N/S -                  5 char.
    radar_site_position_lon: float = LatLonConverter(scale=0.01)
    # E/W -                  6 char.
    radar_inbound_max_wind: int = IntConverter()
    # - 0-300 kts                   3 char.
    radar_inbound_max_wind_azimuth: int = IntConverter()
    # - degrees, 1-360                    3 char.
    radar_inbound_max_wind_range: int = IntConverter()
    # - less than 400 nm,                   3 char.
    radar_inbound_max_wind_elevation: int = IntConverter()
    # - feet                            5 char.
    radar_outbound_max_wind: int = IntConverter()
    # - 0-300 kts                  3 char.
    radar_outbound_max_wind_azimuth: int = IntConverter()
    # - degrees, 1-360                    3 char.
    radar_outbound_max_wind_range: int = IntConverter()
    # - less than 400 nm,                   3 char.
    radar_outbound_max_wind_elevation: int = IntConverter()
    # - feet                            5 char.
    radar_max_cloud_height: int = IntConverter()
    # (trmm radar) - 70,000ft       5 char.
    # Rain accumulation:
    radar_max_rain_accumulation: int = IntConverter()
    # , hundreths of inches 0-10000 5 char.
    radar_max_rain_accumulation_time_interval: int = IntConverter()
    # , 1 - 120 hours                 3 char.
    radar_max_rain_accumulation_lat: float = LatLonConverter(scale=0.01)
    # N/S - Latitude (hundreths of degrees), 0-9000   5 char.
    # N/S is the hemispheric index.
    radar_max_rain_accumulation_lon: float = LatLonConverter(scale=0.01)
    # E/W - Longitude (hundreths of degrees), 0-18000 6 char.
    # E/W is the hemispheric index.
    comments: str = StrConverter()
    #  -                                    52 char.


@dataclass
class Aircraft(CommonFields):
    "Extra Aircraft data columns"
    # flight_level: str #
    aircraft_flight_level_ft: int = IntConverter()
    # - 100's of feet                2 char.
    aircraft_flight_level_hpa: int = IntConverter()
    # - millibars                    3 char.
    aircraft_minimum_height: int = IntConverter()
    # - meters                     4 char.
    # msw: str # # (inbound leg)
    aircraft_max_surface_wind_intensity: int = IntConverter()
    # - kts                             3 char.
    aircraft_max_surface_wind_bearing: int = IntConverter()
    # - degrees                           3 char.
    aircraft_max_surface_wind_range: int = IntConverter()
    # - nm                                  3 char.
    # mflw: str # # (inbound leg)
    aircraft_max_fl_wind_direction: int = IntConverter()
    # - degrees                         3 char.
    aircraft_max_fl_wind_intensity: int = IntConverter()
    # - kts                             3 char.
    aircraft_max_fl_wind_bearing: int = IntConverter()
    # - degrees                           3 char.
    aircraft_max_fl_wind_range: int = IntConverter()
    # - nm                                  3 char.
    aircraft_mean_slp: int = IntConverter()
    # # - millibars         4 char.
    aircraft_temperature_outside_eye: int = IntConverter()
    #  -99 to 99 Celsius     3 char.
    aircraft_temperature_inside_eye: int = IntConverter()
    #   -99 to 99 Celsius     3 char.
    aircraft_dew_point_temperature: int = IntConverter()
    #    -99 to 99 Celsius     3 char.
    aircraft_sea_surface_temperature: int = IntConverter()
    #    0 to 40 Celsius     2 char.
    aircraft_eye_character: str = StrConverter()
    # #or Wall Cld Thickness (pre-2015) 2 char.
    #  NA - < 50% eyewall
    #  CL - Closed Wall
    #  PD - Poorly Defined
    #  N  - open North
    #  NE - open Northeast
    #  E  - open East
    #  SE - open Southeast
    #  S  - open South
    #  SW - open Southwest
    #  W  - open West
    #  NW - open Northwest
    #  SB - spiral band
    #   Eye Shape/Orientation/Diameter
    aircraft_shape: str = StrConverter()
    #: #CI-circ.; EL-elliptic; CO-concentric 2 char.
    aircraft_orientation: int = IntConverter()
    # #- degrees                       3 char.
    aircraft_diameter: int = IntConverter()
    # #(long axis if elliptical) - nm     2 char.
    aircraft_short_axis: int = IntConverter()
    # #(blank if not elliptical) - nm   2 char.
    aircraft_navigational_accuracy: int = IntConverter()
    # #- tenths of nm                 3 char.
    aircraft_meteorological_accuracy: int = IntConverter()
    ## - tenths of nm               3 char.
    aircraft_mission_number: str = StrConverter()
    # #                                 2 char.
    comments: str = StrConverter()
    # #-                                    52 char.
    # extra: str


@dataclass
class Dropsonde(CommonFields):
    "Extra Dropsonde data columns"
    drop_environment: str = StrConverter()
    # -                           10 char.
    # EYEWALL, EYE, RAINBAND, MXWNDBND, SYNOPTIC
    drop_height_of_midpoint: int = IntConverter()
    # meters (75 - 999 m)                          3 char.
    drop_windspeed_150m: int = IntConverter()
    # of drop - kts 3 char.
    drop_windspeed_500m: int = IntConverter()
    # - kts        3 char.
    comments: str = StrConverter()
    # -                                    52 char.


@dataclass
class Analysis(CommonFields):
    "Extra Analysis data columns"
    analysis_analyst: str = StrConverter()
    # initials -                             3 char.
    analysis_start_time: datetime = DatetimeConverter(datetime_format="%Y%m%d%H%M")
    # - YYYYMMDDHHMM                     12 char.
    analysis_end_time: datetime = DatetimeConverter(datetime_format="%Y%m%d%H%M")
    # -  YYYYMMDDHHMM                      12 char.
    analysis_distance_data: int = IntConverter()
    # , 0 - 300 nm           3 char.
    analysis_sst: int = IntConverter()
    # - celsius                                  4 char
    analysis_sources: str = StrConverter()
    # -                         24 char.
    # b - buoy,
    # l - land station,
    # m - SSMI,
    # c - scat,
    # t - trmm,
    # i - ir,
    # v - vis,
    # p - ship,
    # d - dropsonde,
    # a - aircraft,
    # r - radar,
    # x - other
    # 1 char. identifier for each ob source, concatenate
    # the selected 1 char. identifiers to list all of the
    # sources.
    comments: str = StrConverter()
    # -                                    52 char.


def main():
    "demo function"
    stime = time.time()
    df = read_fdeck("/Users/abrammer/repos/tciopy/data/fal132023.dat")
    print(time.time() - stime)
    print(df)  # [df["format"] == 30].dropna(axis="columns", how="all"))


if __name__ == "__main__":
    main()
