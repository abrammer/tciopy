import gzip
import re
import time
from itertools import zip_longest
# from collections import defaultdict
# from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from tciopy.atcf.decks import BaseDeck
from tciopy.converters import StringColumn, NumericColumn, CategoricalColumn, LatLonColumn, DatetimeColumn

from tciopy.converters import datetimeconverter, int_converter, latlonconverter, categoricalconverter


def read_fdeck(fname: str) -> pd.DataFrame:
    """Read an f-deck file into a pandas DataFrame"""
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open

    alldata = {
        7: Analysis(),
        6: Dropsonde(),
        5: Aircraft(),
        4: Radar(),
        3: MicrowaveData(),
        2: SatelliteDVTO(),
        1: SatelliteDVTS(),
    }
    with opener(fname, "rt", newline="\n") as io_file:
        for line in io_file:
            splitline = re.split(r",\s+", line)
            ftype = int(splitline[3]) // 10
            alldata[ftype].append(splitline)

    dfs = [value.to_dataframe() for value in alldata.values()]
    df = pd.concat(dfs, ignore_index=True, sort=False)
    return df


class FDeckCommon_(BaseDeck):
    """Common fields for f-deck entries
        Check https://www.nrlmry.navy.mil/atcf_web/docs/database/new/newfdeck.txt 
        for more information
    """
    def __init__(self):
        #: WP, IO, SH, CP, EP, AL, LS                  2 char.
        self.basin: str = StringColumn()
        #: annual cyclone number, 1-99,                    2 char.
        self.number: int = NumericColumn()
        #: Fix date-time-group,                 12 char.
        self.datetime: datetime = DatetimeColumn(datetime_format="%Y%m%d%H%M")
        #: 
        #:* 10 - subjective dvorak |
        #:* 20 - objective dvorak|
        #:* 30 - microwave|
        #:* 31 - scatterometer|
        #:* 40 - radar|
        #:* 50 - aircraft|
        #:* 60 - dropsonde|
        #:* 70 - analysis|
        self.format: int = NumericColumn()
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
        self.type: str = StringColumn()
        #: 10 char
        #:
        #: *  C-center fix, lat/lon specifies a center fix position,|
        #: *  I-intensity fix, V specifies the max wind speed assoc. with the storm,|
        #: *  R-wind radii fix,|
        #: *  P-pressure, Pressure specifies central mean sea level pressure|
        #: *  N-none,|
        #: *  C, I, P, R, CI, CP, IP, CIP, N ...|
        self.ftype: str = StringColumn()
        #: 1 char
        #: |  C-center location is flagged|
        #: |  I-intensity is flagged|
        #: |  R-wind radii is flagged|
        #: |  P-pressure is flagged|
        #: |  F-all of the above are flagged|
        #: |  " "-fix is not flagged|
        self.flag: str = StringColumn()
        #:  N/S - Latitude (hundreths of degrees), 0-9000    5 char
        #:  N/S is the hemispheric index
        self.lat: float = LatLonColumn(scale=0.01)
        #:E/W - Longitude (hundreths of degrees), 0-18000  6 char
        #:  E/W is the hemispheric index
        self.lon: float = LatLonColumn(scale=0.01)
        #:of ob - 0-99999 meters                        5 char
        self.height: int = NumericColumn()
        #:  1-good, 2-fair, 3-poor            1 char
        self.height_confidence: int = NumericColumn()
        #: Wind speed (kts), 0 - 300 kts                    3 char
        self.vmax: int = NumericColumn()
        #: 1-good, 2-fair, 3-poor                1 char
        self.vmax_confidence: int = NumericColumn()
        #: Pressure, 0 - 1050 mb.                    4 char
        self.pressure: int = NumericColumn()
        #: 1-good, 2-fair, 3-poor         1 char
        self.pressure_confidence: int = NumericColumn()
        #: 4 char
        #: | DVRK- dvorak |
        #: | AKHL- atkinson-holiday table |
        #: | XTRP- extrapolated |
        #: | MEAS- measured         |
        self.pressure_derivation: str = StringColumn()
        #: Wind intensity (kts) - 34, 50, 64              3 char
        #:  for the radii defined in this record
        self.rad: int = NumericColumn()
        #: Radius code                              4 char
        #:   AAA - full circle
        #:   NEQ - northeast quadrant
        self.windcode: str = StringColumn()
        #: If full circle, radius of specified wind intensity
        #:Otherwise radius of specified wind intensity for northeast
        #:quadrant of circle.  0 - 1200 nm.             4 char
        self.rad1: int = NumericColumn()
        #: Radius of specified wind intensity for 2nd quadrant
        #:(southeast quadrant).  0 - 1200 nm.           4 char
        self.rad2: int = NumericColumn()
        #: Radius of specified wind intensity for 3rd quadrant
        #:(southwest quadrant).  0 - 1200 nm.           4 char
        self.rad3: int = NumericColumn()
        #: Radius of specified wind intensity for 4th quadrant
        #:(northwest quadrant).  0 - 1200 nm.           4 char
        self.rad4: int = NumericColumn()
        #: E-Edge of Pass, C-Cut off by Land, B-Both  1 char
        self.rad_mod1: str = StringColumn()
        #: E-Edge of Pass, C-Cut off by Land, B-Both  1 char
        self.rad_mod2: str = StringColumn()
        #: E-Edge of Pass, C-Cut off by Land, B-Both  1 char
        self.rad_mod3: str = StringColumn()
        #: E-Edge of Pass, C-Cut off by Land, B-Both  1 char
        self.rad_mod4: str = StringColumn()
        #: 1-good, 2-fair, 3-poor            1 char
        self.radii_confidence: int = NumericColumn()
        #: radius of max winds 0-999 nm                   3 char
        self.rmw: int = NumericColumn()
        #:  eye diameter, 0-120 nm.                        3 char
        self.eye: int = NumericColumn()
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
        self.subregion: str = StringColumn()
        #:                            5 char
        self.fix_identifier: str = StringColumn()
        #: (fix enterer) -                             3 char
        self.initials: str = StringColumn()



class SatelliteDVTS(FDeckCommon_):
    """Extra Satellite DVTS data columns
    
        These columns will added to the 32 common columns of :py:class:`FDeckCommon_`
    """
    def __init__(self):
        super().__init__()
        #: Sensor type - vis, ir, microwave... 4 char.
        #:                 V, I, M, VI, IM, VM, VIM
        self.dvts_sensor_type: str = StringColumn()
        #: PCN code (deprecated) 1 char.
        #:
        #: * V, I, M, VI, IM, VM, VIM
        #: * 1 = Eye/Geography
        #: * 2 = Eye/Ephemeris
        #: * 3 = Well Defined Circ. Center/Geography
        #: * 4 = Well Definged Circ. Center/Ephemeris
        #: * 5 = Poorly Defined Circ. Center/Geography
        #: * 6 = Poorly Defined Circ. Center/Ephemeris
        self.dvts_pcn_code: str = StringColumn()
        #: Dvorak code - long term trend,                10 char.
        #:
        #: | (18-30 hour change in t-number)
        #: | T num, none or 0.0 - 8.0
        #: | CI num, none or 0.0 - 8.0
        #: | Forecast intensity change, + - or blank
        #: | Past change - developed, steady, weakened (long term trend)
        #: | Amount of t num change, none or 0.0 - 8.0
        #: | Hours since previous eval,  18 - 30 HRS
        #: | Example: T4.0/4.0+/D1.0/24HRS
        self.dvts_dvorak_code_long: str = StringColumn()
        #: Dvorak code - short term trend,                5 char.
        #:
        #: | ( < 18 hour change in t-number.  Only used when  
        #:   significant difference from long term trend, i.e.,
        #:   LTT is +1.0 strengthening, but STT over 6 hours
        #:   shows - 0.5 weakening.)
        #: | Past change - developed, steady, weakened (short term trend)
        #: | Amount of t num change, none or 0.0 - 8.0
        #: | Hours since previous eval,  0 - 17 HRS
        #: | Example: W0.5/06HRS
        self.dvts_dvorak_code_short: str = StringColumn()
        #: CI 24 hr forecast - none or 0.0 - 8.0          2 char.
        self.dvts_ci_24hr_forecast: float = NumericColumn()
        #: Satellite type -                               6 char.
        #:    GMS, DMSP, DMSP45, TRMM, NOAA...
        self.satellite_type: str = StringColumn()
        #: Center type - CSC, LLCC, ULCC                  4 char.
        #:
        #: * LLCC - lower level cloud center
        #: * ULCC - upper level cloud center
        #: * CSC - cloud system center
        self.dvts_center_type: str = StringColumn()
        #: Tropical indicator -   S,E,T       1 char.
        #:
        #: * S-subtropical
        #: * E-extratropical
        #: * T-tropical
        self.dvts_tropical_indicator: str = StringColumn()
        #:                                     52 char
        self.comments: str = StringColumn()


class SatelliteDVTO(FDeckCommon_):
    # """Extra Satellite DVTO data columns
    # DVTO -- Objective dvorak technique Data (IR)

    # Attributes
    # ----------
    # dvto_sensor_type: string
    #             - vis, ir, microwave...            4 char.
    #               V, I, M, VI, IM, VM, VIM
    # dvto_ci_num:
    #     -                                       2 char.
    # dvto_ci_confidence:
    #     CI confidence - 1-good, 2-fair, 3-poor,        1 char.
    # dvto_t_num_mean:
    #     T num (average)                                2 char.
    # dvto_t_num_time_period:
    #     T num averaging time period - hours            3 char.
    # dvto_t_num_derivation:
    #     T num averaging derivation -                   1 char.
    #            L-straight linear, T-time weighted
    # dvto_t_num_raw:
    #     T num (raw)                                    2 char.
    # dvto_temperature_eye:
    #     Temperature (eye), -99 - 50 celsius            4 char.
    # dvto_temperature_cloud:
    #     Temperature (cloud surrounding eye) - celsius  4 char.
    # dvto_scene_type:
    #     Scene type -  CDO, EYE, EEYE, SHER...          4 char.
    #          CDO - central dense overcast
    #          EYE - definable eye
    #          EMBC - embedded center
    #          SHER - partially exposed eye due to strong wind shear 
    #                 with asymmetric convective structure
    # dvto_algorithm:
    #     Algorithm (Rule 9 flag, Rapid flag) - R9, RP   2 char.
    # dvto_satellite_type:
    #     Satellite type -                               6 char.
    #              GMS, DMSP, DMSP45, TRMM, NOAA...
    # dvto_tropical_indicator:
    #     Tropical indicator -                           1 char.
    #          S-subtropical, E-extratropical, T-tropical
    # comments:
    #     Comments -                                    52 char.
    # """
    def __init__(self):
        super().__init__()
        self.dvto_sensor_type: str = StringColumn()
        # - vis, ir, microwave...            4 char.
        # V_ I, M, VI, IM, VM, VIM
        self.dvto_ci_num: int = NumericColumn()
        # -                                       2 char.
        self.dvto_ci_confidence: int = NumericColumn()
        # - 1-good, 2-fair, 3-poor,        1 char.
        self.dvto_t_num_mean: int = NumericColumn()
        #  (average)                                2 char.
        self.dvto_t_num_time_period: int = NumericColumn()
        # - hours            3 char.
        self.dvto_t_num_derivation: str = StringColumn()
        # -                   1 char.
        # L_straight linear, T-time weighted
        self.dvto_t_num_raw: int = NumericColumn()
        # (raw)                                    2 char.
        self.dvto_temperature_eye: int = NumericColumn()
        # , -99 - 50 celsius            4 char.
        self.dvto_temperature_cloud: int = NumericColumn()
        # surrounding eye) - celsius  4 char.
        self.dvto_scene_type: str = StringColumn()
        # -  CDO, EYE, EEYE, SHER...          4 char.
        # CDO_- central dense overcast
        # EYE_- definable eye
        # EMBC_- embedded center
        # SHER_- partially exposed eye due to strong wind shear
        # with_asymmetric convective structure
        self.dvto_algorithm: str = StringColumn()
        # _(Rule 9 flag, Rapid flag) - R9, RP   2 char.
        self.dvto_satellite_type: str = StringColumn()
        # -                               6 char.
        # GMS_ DMSP, DMSP45, TRMM, NOAA...
        self.dvto_tropical_indicator: str = StringColumn()
        # -                           1 char.
        # S_subtropical, E-extratropical, T-tropical
        self.comments: str = StringColumn()
        # -                                    52 char.


class MicrowaveData(FDeckCommon_):
    """Extra Satellite DVTS data columns
    
        These columns will added to the 32 common columns of :py:class:`FDeckCommon_`
    """

    def __init__(self):
        super().__init__()
        # SSMI, TRMM, AMSU, ADOS, ALTI, ERS2, QSCT, SEAW -- MICROWAVE
        self.microwave_rain_flagged: str = StringColumn()
        """ "R" or blank                    1 char."""
        self.microwave_rainrate: int = NumericColumn()
        """   0-500 mm/h                        3 char."""
        self.microwave_process: str = StringColumn()
        """                                      6 char.
          FNMOC algorithm, NESDIS algorithm, RSS..."""
        self.microwave_wave_height: int = NumericColumn()
        """(active micr) - 0-99 ft            2 char."""
        self.microwave_temp: int = NumericColumn()
        """(passive micr) - celsius                  4 char."""
        self.microwave_slp_raw: int = NumericColumn()
        """(raw, AMSU only) - mb                      4 char."""
        self.microwave_slp_retrieved: int = NumericColumn()
        """(retrieved, AMSU only) - mb                4 char."""
        self.microwave_max_meas: int = NumericColumn()
        """ (alti) 0-999 ft.                    3 char."""
        self.microwave_satellite: str = StringColumn()
        """ type -                               6 char.
           GMS, DMSP, DMSP45, TRMM, NOAA..."""
        self.microwave_rad_kt: int = NumericColumn()
        """ Wind intensity (kts) - 34, 50, 64        3 char.
        for the radii defined in this record."""
        self.microwave_windcode: str = StringColumn()
        """ Radius code:                        4 char.
        
          * AAA - full circle
          * quadrant designations -  NEQ - northeast quadrant
          * octant designations: (*NNEO, ENEO, ESEO, SSEO, SSWO, WSWO, WNWO NNWO)"""
        self.microwave_rad1: int = NumericColumn()
        """ If full circle, radius of specified wind intensity.  Otherwise
         radius of specified wind intensity of wind intensity of circle
         portion specified in windcode.  0 - 1200 nm.  4 char."""
        self.microwave_rad2: int = NumericColumn()
        """ Radius of specified wind intensity for 2nd quadrant/octant
          (counting clockwise from quadrant specified in windcode).
          0 - 1200 nm.                                  4 char."""
        self.microwave_rad3: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 3rd quadrant/octant"""
        self.microwave_rad4: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 4th quadrant/octant"""
        self.microwave_rad5: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 5th octant"""
        self.microwave_rad6: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 6th octant"""
        self.microwave_rad7: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 7th octant"""
        self.microwave_rad8: int = NumericColumn()
        """ As :py:attr:`microwave_rad2` for 8th octant"""
        self.microwave_rad_mod1: str = StringColumn()
        """ Flag for the respective radii   - 1 char

         * E-Edge of Pass, 
         * C-Cut off by Land,
         * B-Both"""
        self.microwave_rad_mod2: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod3: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod4: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod5: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod6: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod7: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_rad_mod8: str = StringColumn()
        """As :py:attr:`microwave_rad_mod1`"""
        self.microwave_radii_confidence: int = NumericColumn()
        """# confidence - 1-good, 2-fair, 3-poor        1 char."""
        self.comments: str = StringColumn()  
        """# -                                    52 char."""


class Radar(FDeckCommon_):
    "Extra Radar data columns"
    def __init__(self):
        super().__init__()
        # RDRC, RDRD, RDRT -- Radar, Conventional, Doppler and TRMM
        self.radar_type: str = StringColumn()
        #:                                    1 char.
        # L - Land; S - Ship; A - Aircraft; T - Satellite
        self.radar_format: str = StringColumn()
        #:                                  1 char.
        # R - RADOB; P - plain language; D - Doppler
        self.radob_code: str = StringColumn()
        # - A S W a r t d d f f              10 char.
        # c c c c t e s s s s
        # See description below.
        # Also enter slashes if reported and in place of blanks.
        self.eye_shape: str = StringColumn()
        # -                                    2 char.
        # CI - Circular; EL - Elliptic; CO - concentric
        self.radar_percent_of_eye_wall_observed: int = NumericColumn()
        # (99 = 100%) -     2 char.
        self.radar_spiral_overlay: int = NumericColumn()
        # (degrees) -                     2 char.
        self.radar_site_position_lat: float = LatLonColumn(scale=0.01)
        # N/S -                  5 char.
        self.radar_site_position_lon: float = LatLonColumn(scale=0.01)
        # E/W -                  6 char.
        self.radar_inbound_max_wind: int = NumericColumn()
        # - 0-300 kts                   3 char.
        self.radar_inbound_max_wind_azimuth: int = NumericColumn()
        # - degrees, 1-360                    3 char.
        self.radar_inbound_max_wind_range: int = NumericColumn()
        # - less than 400 nm,                   3 char.
        self.radar_inbound_max_wind_elevation: int = NumericColumn()
        # - feet                            5 char.
        self.radar_outbound_max_wind: int = NumericColumn()
        # - 0-300 kts                  3 char.
        self.radar_outbound_max_wind_azimuth: int = NumericColumn()
        # - degrees, 1-360                    3 char.
        self.radar_outbound_max_wind_range: int = NumericColumn()
        # - less than 400 nm,                   3 char.
        self.radar_outbound_max_wind_elevation: int = NumericColumn()
        # - feet                            5 char.
        self.radar_max_cloud_height: int = NumericColumn()
        # (trmm radar) - 70,000ft       5 char.
        # Rain accumulation:
        self.radar_max_rain_accumulation: int = NumericColumn()
        # , hundreths of inches 0-10000 5 char.
        self.radar_max_rain_accumulation_time_interval: int = NumericColumn()
        # , 1 - 120 hours                 3 char.
        self.radar_max_rain_accumulation_lat: float = LatLonColumn(scale=0.01)
        # N/S - Latitude (hundreths of degrees), 0-9000   5 char.
        # N/S is the hemispheric index.
        self.radar_max_rain_accumulation_lon: float = LatLonColumn(scale=0.01)
        # E/W - Longitude (hundreths of degrees), 0-18000 6 char.
        # E/W is the hemispheric index.
        self.comments: str = StringColumn()
        #  -                                    52 char.


class Aircraft(FDeckCommon_):
    def __init__(self):
        super().__init__()
        "Extra Aircraft data columns"
        # flight_level: str #
        self.aircraft_flight_level_ft: int = NumericColumn()
        # - 100's of feet                2 char.
        self.aircraft_flight_level_hpa: int = NumericColumn()
        # - millibars                    3 char.
        self.aircraft_minimum_height: int = NumericColumn()
        # - meters                     4 char.
        # msw: str # # (inbound leg)
        self.aircraft_max_surface_wind_intensity: int = NumericColumn()
        # - kts                             3 char.
        self.aircraft_max_surface_wind_bearing: int = NumericColumn()
        # - degrees                           3 char.
        self.aircraft_max_surface_wind_range: int = NumericColumn()
        # - nm                                  3 char.
        # mflw: str # # (inbound leg)
        self.aircraft_max_fl_wind_direction: int = NumericColumn()
        # - degrees                         3 char.
        self.aircraft_max_fl_wind_intensity: int = NumericColumn()
        # - kts                             3 char.
        self.aircraft_max_fl_wind_bearing: int = NumericColumn()
        # - degrees                           3 char.
        self.aircraft_max_fl_wind_range: int = NumericColumn()
        # - nm                                  3 char.
        self.aircraft_mean_slp: int = NumericColumn()
        # # - millibars         4 char.
        self.aircraft_temperature_outside_eye: int = NumericColumn()
        #  -99 to 99 Celsius     3 char.
        self.aircraft_temperature_inside_eye: int = NumericColumn()
        #   -99 to 99 Celsius     3 char.
        self.aircraft_dew_point_temperature: int = NumericColumn()
        #    -99 to 99 Celsius     3 char.
        self.aircraft_sea_surface_temperature: int = NumericColumn()
        #    0 to 40 Celsius     2 char.
        self.aircraft_eye_character: str = StringColumn()
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
        self.aircraft_shape: str = StringColumn()
        #: #CI-circ.; EL-elliptic; CO-concentric 2 char.
        self.aircraft_orientation: int = NumericColumn()
        # #- degrees                       3 char.
        self.aircraft_diameter: int = NumericColumn()
        # #(long axis if elliptical) - nm     2 char.
        self.aircraft_short_axis: int = NumericColumn()
        # #(blank if not elliptical) - nm   2 char.
        self.aircraft_navigational_accuracy: int = NumericColumn()
        # #- tenths of nm                 3 char.
        self.aircraft_meteorological_accuracy: int = NumericColumn()
        ## - tenths of nm               3 char.
        self.aircraft_mission_number: str = StringColumn()
        # #                                 2 char.
        self.comments: str = StringColumn()
        # #-                                    52 char.
        # extra: str


class Dropsonde(FDeckCommon_):
    def __init__(self):
        super().__init__()
        "Extra Dropsonde data columns"
        self.drop_environment: str = StringColumn()
        # -                           10 char.
        # EYEWALL, EYE, RAINBAND, MXWNDBND, SYNOPTIC
        self.drop_height_of_midpoint: int = NumericColumn()
        # meters (75 - 999 m)                          3 char.
        self.drop_windspeed_150m: int = NumericColumn()
        # of drop - kts 3 char.
        self.drop_windspeed_500m: int = NumericColumn()
        # - kts        3 char.
        self.comments: str = StringColumn()
        # -                                    52 char.


class Analysis(FDeckCommon_):
    def __init__(self):
        super().__init__()
        "Extra Analysis data columns"
        self.analysis_analyst: str = StringColumn()
        # initials -                             3 char.
        self.analysis_start_time: datetime = DatetimeColumn(datetime_format="%Y%m%d%H%M")
        # - YYYYMMDDHHMM                     12 char.
        self.analysis_end_time: datetime = DatetimeColumn(datetime_format="%Y%m%d%H%M")
        # -  YYYYMMDDHHMM                      12 char.
        self.analysis_distance_data: int = NumericColumn()
        # , 0 - 300 nm           3 char.
        self.analysis_sst: int = NumericColumn()
        # - celsius                                  4 char
        self.analysis_sources: str = StringColumn()
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
        self.comments: str = StringColumn()
        # -                                    52 char.


def main():
    "demo function"
    stime = time.time()
    datadir = Path(__file__).parent.parent.parent.parent / "data"
    df = read_fdeck(datadir / "fal132023.dat")
    print(time.time() - stime)
    print(df)  # [df["format"] == 30].dropna(axis="columns", how="all"))


if __name__ == "__main__":
    main()
