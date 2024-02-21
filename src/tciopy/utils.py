import numpy as np
import pandas as pd


def lon_lat_to_cartesian(lon, lat, radius=6378):
    """
    Calculates lon, lat coordinates of a point on a sphere with
    radius R
    """
    lon_r = np.radians(lon)
    lat_r = np.radians(lat)

    x = radius * np.cos(lat_r) * np.cos(lon_r)
    y = radius * np.cos(lat_r) * np.sin(lon_r)
    z = radius * np.sin(lat_r)
    return x, y, z


def cartesian_to_lon_lat(x, y, z, radius=None):
    """"
    Convert x, y, z coordinates back to longitude latitude

    The earths radius is probably constant(?), passing a radius arg
    would reduce one line of computation...
    """
    if radius is None:
        radius = np.sqrt(x**2 + y**2 + z**2)

    theta = np.arcsin(z / radius)
    phi = np.arctan2(y, x)
    lon = np.degrees(phi)
    lat = np.degrees(theta)
    return lon, lat


def swap_to_cartesian(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace lat/lon columns with cartesian coords
    """
    x, y, z = lon_lat_to_cartesian(df['lon'], df['lat'])
    df = df.assign(cart_x=x, cart_y=y, cart_z=z)
    df.drop(['lat', 'lon'], axis=1, inplace=True)
    return df


def swap_from_cartesian(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace cartesian coord columns with lat/lon
    """
    lon, lat = cartesian_to_lon_lat(df['cart_x'], df['cart_y'], df['cart_z'])
    df = df.assign(lon=lon, lat=lat)
    df.drop(['cart_x', 'cart_y', 'cart_z'], axis=1, inplace=True)
    return df


def _upsample_group(group: pd.DataFrame, output_freq: str,
                    method: str) -> pd.DataFrame:
    """ interpolates each track with respect to time """
    _allgroup = group[~group.index.duplicated(keep='first')]
    _allgroup = swap_to_cartesian(_allgroup)
    _group = _allgroup.select_dtypes(exclude=['category', 'datetime', 'object']).fillna(0)
    upsampled = _group.resample(output_freq).interpolate(method='time', limit_area='inside')
    if method != 'time':
        if _group.shape[0] < 4:
            method = 'linear'
        _group_resmapler = _group[['cart_x', 'cart_y',
                                   'cart_z']].resample(output_freq)
        upsampled.loc[:, ['cart_x', 'cart_y', 'cart_z'
                          ]] = _group_resmapler.interpolate(method=method)
    upsampled = swap_from_cartesian(upsampled)
    upsampled['datetime'] = upsampled.index
    upsampled = pd.merge(upsampled, _allgroup.select_dtypes(include=['category', 'datetime', 'object']), on='datetime', how='left')
    return upsampled.interpolate(method='pad', limit_area='inside', axis='columns')  # fill in any missing categorical gaps


def upsample_forecast(dataframe: pd.DataFrame,
                   output_freq: str = '1H',
                   method: str = 'time') -> pd.DataFrame:
    """ Upsamples tracks by tech / datetime to desired frequency """
    _dataframe = dataframe.set_index('validtime')
    groups = _dataframe.groupby(['number', 'basin', 'tech', 'datetime'])
    upsampled = groups.apply(_upsample_group,
                             output_freq=output_freq,
                             method=method)

    return upsampled.reset_index(level=['number', 'basin', 'tech', 'datetime'],
                                 drop=True).reset_index()

def upsample_besttrack(dataframe: pd.DataFrame,
                   output_freq: str = '1H',
                   method: str = 'time') -> pd.DataFrame:
    """ Upsamples tracks by tech / datetime to desired frequency """
    _dataframe = dataframe.set_index('validtime')
    groups = _dataframe.groupby(['number', 'basin', 'tech'], observed=False)
    upsampled = groups.apply(_upsample_group,
                             output_freq=output_freq,
                             method=method)

    return upsampled.reset_index(level=['number', 'basin', 'tech'],
                                 drop=True).reset_index()

