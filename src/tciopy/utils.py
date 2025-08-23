import numpy as np
import polars as pl

EARTH_RADIUS_KM = 6371.0  # Radius of Earth in kilometers


def haversine_distance(lat1: pl.Series | pl.Expr, lon1: pl.Series | pl.Expr, lat2: pl.Series | pl.Expr, lon2: pl.Series | pl.Expr) -> pl.Expr:
    """
    Calculates the Haversine distance between two points on the Earth
    (specified in decimal degrees) in kilometers using Polars.

    Args:
        lat1 (polars.Series): Latitude of the first point.
        lon1 (polars.Series): Longitude of the first point.
        lat2 (polars.Series): Latitude of the second point.
        lon2 (polars.Series): Longitude of the second point.

    Returns:
        polars.Series: Haversine distance between the two points in kilometers.
    """
    lat1_rad = lat1.radians()
    lon1_rad = lon1.radians()
    lat2_rad = lat2.radians()
    lon2_rad = lon2.radians()

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = (dlat / 2).sin().pow(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2).sin().pow(2)
    c = 2 * pl.arctan2(a.sqrt(), (1 - a).sqrt())

    distance = EARTH_RADIUS_KM * c
    return distance


def mean_location(lat: pl.Series | pl.Expr, lon: pl.Series | pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """
    Calculate the mean latitude and longitude using the Haversine formula
    to account for the spherical nature of the Earth.

    Args:
        latL (polars.Series): Series of latitudes in decimal degrees.
        lon (polars.Series): Series of longitudes in decimal degrees.

    Returns:
        tuple: Mean latitude and longitude as polars expressions.
    """
    lat_rad = lat.radians()
    lon_rad = lon.radians()

    x = lon_rad.cos() * lat_rad.cos()
    y = lon_rad.sin() * lat_rad.cos()
    z = lat_rad.sin()

    x_mean = x.mean()
    y_mean = y.mean()
    z_mean = z.mean()

    lon_mean = pl.arctan2(y_mean, x_mean).degrees()
    hyp = (x_mean.pow(2) + y_mean.pow(2)).sqrt()
    lat_mean = pl.arctan2(z_mean, hyp).degrees()

    return lat_mean, lon_mean


def lon_lat_to_cartesian(lon, lat, radius=EARTH_RADIUS_KM):
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


def cartesian_to_lon_lat(x, y, z, radius=EARTH_RADIUS_KM):
    """"
    Convert x, y, z coordinates back to longitude latitude

    The earths radius is probably constant(?), passing a radius arg
    would reduce one line of computation...
    """

    theta = np.arcsin(z / radius)
    phi = np.arctan2(y, x)
    lon = np.degrees(phi)
    lat = np.degrees(theta)
    return lon, lat


# def swap_to_cartesian(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Replace lat/lon columns with cartesian coords
#     """
#     x, y, z = lon_lat_to_cartesian(df['lon'], df['lat'])
#     df = df.assign(cart_x=x, cart_y=y, cart_z=z)
#     df.drop(['lat', 'lon'], axis=1, inplace=True)
#     return df


# def swap_from_cartesian(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Replace cartesian coord columns with lat/lon
#     """
#     lon, lat = cartesian_to_lon_lat(df['cart_x'], df['cart_y'], df['cart_z'])
#     df = df.assign(lon=lon, lat=lat)
#     df.drop(['cart_x', 'cart_y', 'cart_z'], axis=1, inplace=True)
#     return df


# def _upsample_group(group: pd.DataFrame, output_freq: str,
#                     method: str) -> pd.DataFrame:
#     """ interpolates each track with respect to time """
#     _allgroup = group[~group.index.duplicated(keep='first')]
#     _allgroup = swap_to_cartesian(_allgroup)
#     _group = _allgroup.select_dtypes(exclude=['category', 'datetime', 'object']).fillna(0)
#     upsampled = _group.resample(output_freq).interpolate(method='time', limit_area='inside')
#     if method != 'time':
#         if _group.shape[0] < 4:
#             method = 'linear'
#         _group_resmapler = _group[['cart_x', 'cart_y',
#                                    'cart_z']].resample(output_freq)
#         upsampled.loc[:, ['cart_x', 'cart_y', 'cart_z'
#                           ]] = _group_resmapler.interpolate(method=method)
#     upsampled = swap_from_cartesian(upsampled)
#     upsampled['datetime'] = upsampled.index
#     upsampled = pd.merge(upsampled, _allgroup.select_dtypes(include=['category', 'datetime', 'object']), on='datetime', how='left')
#     return upsampled.interpolate(method='pad', limit_area='inside', axis='columns')  # fill in any missing categorical gaps


# def upsample_forecast(dataframe: pd.DataFrame,
#                    output_freq: str = '1H',
#                    method: str = 'time') -> pd.DataFrame:
#     """ Upsamples tracks by tech / datetime to desired frequency """
#     _dataframe = dataframe.set_index('validtime')
#     groups = _dataframe.groupby(['number', 'basin', 'tech', 'datetime'])
#     upsampled = groups.apply(_upsample_group,
#                              output_freq=output_freq,
#                              method=method)

#     return upsampled.reset_index(level=['number', 'basin', 'tech', 'datetime'],
#                                  drop=True).reset_index()


# def upsample_besttrack(dataframe: pd.DataFrame,
#                    output_freq: str = '1H',
#                    method: str = 'time') -> pd.DataFrame:
#     """ Upsamples tracks by tech / datetime to desired frequency """
#     _dataframe = dataframe.set_index('validtime')
#     groups = _dataframe.groupby(['number', 'basin', 'tech'], observed=False)
#     upsampled = groups.apply(_upsample_group,
#                              output_freq=output_freq,
#                              method=method)

#     return upsampled.reset_index(level=['number', 'basin', 'tech'],
#                                  drop=True).reset_index()

