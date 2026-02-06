import numpy as np
import polars as pl

EARTH_RADIUS_KM = 6371.0  # Radius of Earth in kilometers


def fillnan(val, nafill=0):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return nafill
    return val


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


def storm_direction(lat: pl.Series | pl.Expr, lon: pl.Series | pl.Expr) -> pl.Expr:
    """
    Calculate the storm direction in degrees clockwise from north
    given series of latitudes and longitudes.

    Args:
        lat (polars.Series): Series of latitudes in decimal degrees.
        lon (polars.Series): Series of longitudes in decimal degrees.
    Returns:
        polars.Expr: Storm direction in degrees clockwise from north.
    """
    dlat = lat.diff()
    dlon = lon.diff()

    angle_rad = pl.arctan2(dlon, dlat)
    angle_deg = (angle_rad.degrees()) % 360
    angle_deg = angle_deg.fill_null(angle_deg.shift(-1))
    return angle_deg.alias('storm_dir')


def storm_speed(lat: pl.Series | pl.Expr, lon: pl.Series | pl.Expr, tau: pl.Series | pl.Expr) -> pl.Expr:
    """
    Calculate the storm speed in km/h given series of latitudes,
    longitudes, and forecast lead times (tau) in hours.

    Args:
        lat (polars.Series): Series of latitudes in decimal degrees.
        lon (polars.Series): Series of longitudes in decimal degrees.
        tau (polars.Series): Series of forecast lead times in duration[μs].
    Returns:
        polars.Expr: Storm speed in km/h.
    """
    distance = haversine_distance(
        lat.shift(1).fill_null(lat),
        lon.shift(1).fill_null(lon),
        lat,
        lon
    )
    delta_tau = tau.diff().dt.total_hours().fill_null(1)  # Avoid division by zero

    speed = (distance / delta_tau).alias('storm_speed')
    return speed


def direction_spread(lat: pl.Expr, lon:pl.Expr, direction: pl.Expr) -> pl.Expr:
    """
    Calculate the distance of points in a given direction.

    Args:
        lat (polars.Series): Series of latitudes in decimal degrees.
        lon (polars.Series): Series of longitudes in decimal degrees.
        direction (polars.Series): Series of directions in degrees clockwise from north.
    Returns:
        polars.Expr: distance from mean in the given direction in km. +/- values indicate direction from mean.
            +ve values are in the direction, -ve values are opposite.
    """
    mean_lat, mean_lon = mean_location(lat, lon)
    direction_rad = direction.radians()
    delta_lat = (lat - mean_lat).radians()
    delta_lon = (lon - mean_lon).radians()
    projected_distance = (EARTH_RADIUS_KM * (
        (delta_lat * direction_rad.cos()) +
        (delta_lon * direction_rad.sin() * mean_lat.radians().cos())
    ))
    
    return projected_distance


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


def calculate_ellipse(lat, lon):
    ''' Given Pandas group with lat,lon calculate EOF for Major/Minor
        ellipse axis or eigen values and vectors'''

    sla = lat.var()
    slo = lon.var()
    rho = pl.cov(lat, lon)

    trace = sla + slo
    determinant = (sla * slo) - (rho * rho)
    term = ((trace ** 2 - 4 * determinant).sqrt()) / 2
    
    # Eigenvalues
    eig_val_1 = (trace / 2) + term
    eig_val_2 = (trace / 2) - term
    
    # Take square root and sort by magnitude
    eig_val_1_sqrt = eig_val_1.sqrt()
    eig_val_2_sqrt = eig_val_2.sqrt()
    
    ellipse_major = pl.max_horizontal(eig_val_1_sqrt, eig_val_2_sqrt).alias('ellipse_major')
    ellipse_minor = pl.min_horizontal(eig_val_1_sqrt, eig_val_2_sqrt).alias('ellipse_minor')
    
    # Calculate angle based on which eigenvalue is larger
    # angle = 90 + arctan2(eigenvector_y, eigenvector_x)
    angle = (pl.lit(90) + pl.arctan2(eig_val_1 - slo, rho).degrees()) % 360
    angle = angle.alias('ellipse_angle')

    return ellipse_major, ellipse_minor, angle


def calculate_new_lon_lat(lon, lat, delta_along, delta_cross, direction):
    """
    Calculate new lon/lat given original lon/lat, displacements along
    and across a given direction (degrees clockwise from north)
    """
    # Convert degrees to radians
    direction_rad = direction.radians()
    lat_rad = lat.radians()
    lon_rad = lon.radians()

    # Calculate displacements in lat/lon
    delta_lat = (delta_along * direction_rad.cos() - delta_cross * direction_rad.sin()) / EARTH_RADIUS_KM
    delta_lon = (delta_along * direction_rad.sin() + delta_cross * direction_rad.cos()) / (EARTH_RADIUS_KM * lat_rad.cos())

    # New lat/lon in radians
    new_lat_rad = lat_rad + delta_lat
    new_lon_rad = lon_rad + delta_lon

    # Convert back to degrees
    new_lat = new_lat_rad.degrees()
    new_lon = new_lon_rad.degrees()

    return new_lon.alias('lon'), new_lat.alias('lat')


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

