"""
Reader for TC-CXML files, to unpack into a pandas dataframe matching column names for other readers

// TODO: Handle the mixed units contained in cxml files.  
"""
import warnings
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd


CENTER_TO_TECH = {
    "GEFS": "AP{:02d}",
    "GFS": "AVNO",
    "MOGREPS-G": "EG{:02d}",
}

XML_TO_COLNAME = {
    "lat": {"xpath": ".//latitude", "dtype": float},
    "lon": {"xpath": ".//longitude", "dtype": float},
    "vmax": {"xpath": ".//maximumWind/speed", "dtype": float},
    "type": {"xpath": ".//development", "dtype": str},
    "mslp": {"xpath": ".//pressure", "dtype": float},
    "pouter": {"xpath": ".//lastClosedIsobar/pressure", "dtype": float},
    "router": {"xpath": ".//lastClosedIsobar/radius", "dtype": float},
    "rmw": {"xpath": ".//maximumWind/radius", "dtype": float},
    "validtime": {"xpath": ".//validTime", "dtype": str},
    "subregion": {"xpath": ".//subRegion", "dtype": str},
    "direction": {"xpath": ".//stormMotion/directionToward", "dtype": float},
    "speed": {"xpath": ".//stormMotion/speed", "dtype": float},
}


def read_cxml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    center = tree.find(".//name").text
    alldata = []
    for member in root.findall(".//data[@type='ensembleForecast']"):
        tech = _get_tech(center, int(member.get("member")))
        for disturbance in member.findall(".//disturbance"):
            try:
                stormname = disturbance.find("cycloneName").text
                num = int(disturbance.find("cycloneNumber").text)
                basin = disturbance.find("basin").text
            except (ValueError, AttributeError):
                warnings.warn("Entry missing stormname, num, or basin, was not processed")
                continue
            for fix in disturbance.findall(".//fix"):
                data = {
                    "basin": basin,
                    "number": num,
                    "tech": tech,
                    "stormname": stormname,
                    "tau": int(fix.get("hour")),
                }
                for key, kwargs in XML_TO_COLNAME.items():
                    data[key] = _find_text(fix, **kwargs)
                for key, value in _extract_radii(fix, ".//windContours/windSpeed", "rad"):
                    data[key] = value
                for key, value in _extract_radii(fix, ".//seaContours/waveHeight", "seas"):
                    data[key] = value
                alldata.append(data)

    df = pd.DataFrame.from_records(alldata)
    df["validtime"] = pd.to_datetime(df.loc[:, "validtime"], format="%Y-%m-%dT%H:%M:%SZ")
    df.loc[:, "datetime"] = df.loc[:, "validtime"] - pd.to_timedelta(df.loc[:, "tau"], unit="h")

    return df


def _get_tech(center, member):
    return CENTER_TO_TECH.get(center, center).format(member)


def _find_text(tag, xpath, dtype=str):
    default = "" if dtype is str else np.nan
    try:
        return dtype(tag.find(xpath).text)
    except AttributeError:
        return default


def _extract_radii(tag, xpath, key):
    for radii in tag.findall(xpath):
        ins = float(radii.text)
        for quad in radii.findall(".//radius"):
            sector = quad.get("sector")
            yield f"{key}{ins:.0f}_{sector}", float(quad.text)


def main():
    df = read_cxml(
        # "/Users/abrammer/repos/tciopy/data/kwbc_20200918120000_GEFS_glob_prod_esttr_glo.xml"
        "/Users/abrammer/repos/tciopy/data/complete_cxml.xml"
        # "/Users/abrammer/Downloads/z_tigge_c_egrr_20230612000000_mogreps_glob_prod_etctr_glo.xml"
    )
    print(df)
    # print(df.dtypes)


main()
