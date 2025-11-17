"""
Reader for TC-CXML files, to unpack into a pandas dataframe matching column names for other readers

// TODO: Handle the mixed units contained in cxml files.  
"""
import warnings
import xml.etree.ElementTree as ET

import numpy as np
import polars as pl


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

    datum = pl.DataFrame(alldata)
    datum = datum.with_columns([
            pl.col("validtime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ").alias("validtime"),
            ],
            ).with_columns([
            (pl.col('validtime') - pl.duration(hours=pl.col('tau'))).alias('datetime')
            ],
            )
    return datum


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
    from pathlib import Path

    datadir = Path(__file__).parent.parent.parent.parent / "data"
    df = read_cxml(datadir / "complete_cxml.xml")
    print(df.select('validtime', 'tau', 'datetime'))
    # print(df.dtypes)


if __name__ == "__main__":
    main()
