import sys
from datetime import datetime
import pathlib
import logging
import itertools

import polars as pl
import numpy as np

from eccodes import (codes_bufr_new_from_file, codes_get, codes_set,
                     codes_get_array, CodesInternalError, CODES_MISSING_DOUBLE,
                     CODES_MISSING_LONG, codes_release)
import gribapi

## to do refactor this to less assumptive and more polars friendly. 
## bufr -> to_json -> polars dataframe. ??

LOGGER = logging.getLogger(__name__)

def add_constants(kwargs, df):
    for key, value in kwargs.items():
        df = df.with_columns(pl.lit(value).alias(key))
    return df


def get_analysis_datetime(bufr):
    """ return datetime object from bufr header """
    year = codes_get(bufr, "year")
    month = codes_get(bufr, "month")
    day = codes_get(bufr, "day")
    hour = codes_get(bufr, "hour")
    minute = codes_get(bufr, "minute")
    return datetime(year, month, day, hour, minute)


def get_wind_radii(bufr, i):
    rad_codes = {(0,90): 'NEQ',
                (90,180): 'SEQ',
                (180,270): 'SWQ',
                (270,0): 'NWQ'}
    rank1 = i*3
    radii = {}
    for j in range(1,4):
        radms = codes_get(bufr, "#%d#windSpeedThreshold" % (rank1 + j))
        if radms == CODES_MISSING_DOUBLE or radms == CODES_MISSING_LONG:
            continue
        radkt = np.floor(radms * 1.943) # convert from ms to kts ! should we do this or keep in native units. 
        for k in range(1,5):
            # there is a logic to this, but it relies heavily on assuming the number of entries per time dont change
            # the rank increments with each use.  so hard code the ranks based on the time index and how much data has been consumed.
            rank2 = i*12 + ((j-1)*4) + k
            rank3 = i*24 + ((j-1)*8) + (k*2) 
            _rad = codes_get_array(bufr, "#%d#effectiveRadiusWithRespectToWindSpeedsAboveThreshold" % rank2)
            units = codes_get(bufr, "#%d#effectiveRadiusWithRespectToWindSpeedsAboveThreshold->units" % rank2)
            _bear1 = codes_get(bufr, "#%d#bearingOrAzimuth" % (rank3-1))
            _bear2 = codes_get(bufr, "#%d#bearingOrAzimuth" % rank3)
            rad_code = rad_codes.get((_bear1, _bear2), f"{_bear1:.0f}:{_bear2:.0f}")  # convert (0,90) to NEQ etc.
            radii[f'rad_{radkt:2.0f}_{rad_code}_[{units}]'] = _rad
    return radii


def get_location(bufr, rank):
    """ Extract location data from bufr """
    lat = codes_get_array(bufr, "#%d#latitude" % rank)
    lon = codes_get_array(bufr, "#%d#longitude" % rank)
    locationtype = codes_get(bufr, "#%d#MeteorologicalAttributeSignificance" % rank)
    loctype = {1:"", 2:"outer_limit_", 3:"max_wind_", 4:"", 5:""}
    if locationtype not in  loctype.keys():
        LOGGER.debug("No valid significance data found lat:%s lon:%s significance:%s rank:%s", lat, lon, locationtype, rank)
        raise ValueError
    return lat, lon, loctype.get(locationtype)


def get_analysis_data(bufr, member_number):
    """ Extract data for initial observed or perturbed locations"""
    # Observed Storm Centre
    # significance = codes_get(bufr, '#1#meteorologicalAttributeSignificance')
    # Location of storm in perturbed analysis
    temp_df = pl.DataFrame({'member': member_number,})

    try:
        lat, lon, loc_prefix = get_location(bufr, 2)
        LOGGER.debug("Extracted latitude %s, longitude %s", lat, lon)
    except gribapi.errors.ArrayTooSmallError:
        LOGGER.info("No pertubed location found")
        lat, lon, loc_prefix = get_location(bufr, 1)
        LOGGER.debug("Observed latitude %s, longitude %s", lat, lon)
    temp_df = expand_value(f'{loc_prefix}latitude', lat, temp_df)
    temp_df = expand_value(f'{loc_prefix}longitude', lon, temp_df)

    pressure_analysis = codes_get_array(bufr, '#1#pressureReducedToMeanSeaLevel')
    pressure_units = codes_get(bufr, '#1#pressureReducedToMeanSeaLevel->units')
    temp_df=expand_value(f'mslp_[{pressure_units}]', pressure_analysis, temp_df)

    # Location of Maximum Wind
    wind_max_wind0 = codes_get_array(bufr, '#1#windSpeedAt10M')
    wind_units = codes_get(bufr, '#1#windSpeedAt10M->units')
    temp_df=expand_value(f'vmax_[{wind_units}]', wind_max_wind0, temp_df)

    lat, lon, loc_prefix = get_location(bufr, 3)
    temp_df= expand_value(f'{loc_prefix}latitude', lat, temp_df)
    temp_df= expand_value(f'{loc_prefix}longitude', lon, temp_df)

    try:
        radii = get_wind_radii(bufr, 0)
    except gribapi.errors.KeyValueNotFoundError:
        LOGGER.info("No wind radii found")
        pass
        # radii = {'rad34_1':[0], 'rad34_2':[0], 'rad34_3':[0], 'rad34_4':[0]}
    temp_df=expand_value('tau', [0], temp_df)
    for key,val in radii.items():
        temp_df=expand_value(key, val, temp_df)
    return temp_df


def expand_value(key, values, df):
    if len(values) != len(df):
        df = df.with_columns(pl.lit(values[0]).alias(key))
    else:
        df = df.with_columns(pl.Series(values).alias(key))
    return df



def extract_timeperiod(i, bufr, members):
    """ Extract scattered data by some insane logic of multiplying the time index """
    centerloc_rank = i * 2 + 2
    maxwndloc_rank = i * 2 + 3
    centerdata_rank = i + 1

    LOGGER.debug("Extracting time period %s with ranks %s,%s", i,centerdata_rank , centerloc_rank)

    temp_df = pl.DataFrame({'member': members,})

    time_period = codes_get_array(bufr, "#%d#timePeriod" % i)
    temp_df = expand_value('tau', time_period, temp_df)

    press = codes_get_array(bufr, "#%d#pressureReducedToMeanSeaLevel" % centerdata_rank)
    press_units = codes_get(bufr, "#%d#pressureReducedToMeanSeaLevel->units" % centerdata_rank)
    wind10m = codes_get_array(bufr, "#%d#windSpeedAt10M" % centerdata_rank)
    wind_units = codes_get(bufr, "#%d#windSpeedAt10M->units" % centerdata_rank)
    temp_df = expand_value(f'vmax_[{wind_units}]', wind10m, temp_df)
    temp_df = expand_value(f'mslp_[{press_units}]', press, temp_df)

    # Location of the storm
    try:
        lat, lon, loc_prefix = get_location(bufr, centerloc_rank)
        temp_df = expand_value(f'{loc_prefix}latitude', lat, temp_df)
        temp_df = expand_value(f'{loc_prefix}longitude', lon, temp_df)
        LOGGER.debug("Extracted center location @ %s, %s", lat, lon)
    except ValueError as exc:
        LOGGER.debug("No center data time:%s, rank:%s", i, centerloc_rank)
        pass

    try:
        lat, lon, loc_prefix = get_location(bufr, maxwndloc_rank)
        temp_df = expand_value(f'{loc_prefix}latitude', lat, temp_df)
        temp_df = expand_value(f'{loc_prefix}longitude', lon, temp_df)
        LOGGER.debug("Extracted max wind %s @ %s, %s", wind10m, lat, lon)
    except ValueError as exc:
        LOGGER.debug("No max wind data time:%s, rank:%s", i, maxwndloc_rank)
        pass

    try:
        radii = get_wind_radii(bufr, i)
        for key, val in radii.items():
            temp_df = expand_value(key, val, temp_df)
    except gribapi.errors.KeyValueNotFoundError:
        LOGGER.info("No wind radii found time: %s", i)
        pass

    return temp_df


def read_bufr(filepath:pathlib.Path) -> pl.DataFrame:
    """
    Read a BUFR file and return a DataFrame.

    Parameters
    ----------
    filepath : pathlib.Path
        Path to the BUFR file.
    """
    data = []
    with open(input_filepath, 'rb') as bufr_file:
        # loop for the messages in the file
        for message_count in itertools.count():
            LOGGER.debug("Starting message %s", message_count)
            # get handle for message
            bufr = codes_bufr_new_from_file(bufr_file)
            if bufr is None:
                break

            # we need to instruct ecCodes to expand all the descriptors
            # i.e. unpack the data values
            codes_set(bufr, 'unpack', 1)

            # numObs = codes_get(bufr, "numberOfSubsets")
            try:
                model_name = codes_get(bufr, "numericalModelIdentifier")
            except gribapi.errors.KeyValueNotFoundError:
                model_name = "UNKNOWN"
            #
            analysis_datetime = get_analysis_datetime(bufr)

            storm_identifier = codes_get(bufr, "stormIdentifier")
            # Get ensembleMemberNumber
            member_number = codes_get_array(bufr, "ensembleMemberNumber")
            storm_name = codes_get(bufr, 'longStormName')
            constant_data = {'storm_name': storm_name,
                             'storm_identifier': storm_identifier,
                             'model': model_name,
                             'analysis_datetime': analysis_datetime}
            # Observed Storm Centre
            temp_df = get_analysis_data(bufr, member_number)
            temp_df = add_constants(constant_data, temp_df)
            data.append(temp_df)

            # How many different timePeriod in the data structure?
            for time_ct in itertools.count(1):
                try:
                    codes_get_array(bufr, "#%d#timePeriod" % time_ct)
                except CodesInternalError:
                    break
                LOGGER.debug("fetching timePeriod %s,",time_ct)
                    # the numberOfPeriods includes the analysis (period=0)
                temp_df = extract_timeperiod(time_ct, bufr, member_number)
                temp_df = add_constants(constant_data, temp_df)
                data.append(temp_df)
            # release the BUFR message
            codes_release(bufr)

    # close the BUFR file
    all_df = pl.concat(data, how='diagonal')
    all_df = all_df.with_columns(
        pl.col(pl.Float64).replace(CODES_MISSING_DOUBLE, None),
        pl.col(pl.Int64).replace(CODES_MISSING_LONG, None),
        )
    # remove rows with all missing data except member
    all_df = all_df.filter(~(
        pl.all_horizontal(
            pl.exclude('member').is_null()
        )
    ))
    return all_df


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    # Read the BUFR file
    LOGGER.info("Reading BUFR file: %s", sys.argv[1])
    input_filepath = pathlib.Path(sys.argv[1])
    df = read_bufr(input_filepath)
    LOGGER.info("Read %s rows", len(df))
    # Save the DataFrame to a CSV file
    LOGGER.info("Saving DataFrame to CSV file: %s", input_filepath.with_suffix('.csv'))
    df.write_csv(input_filepath.with_suffix('.csv'), float_scientific=False, float_precision=1)
    
    # print(df)