from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime

def str2ll(x):
    """Convert atcf str to latlon -- internal single value only"""
    converters = {'N': 1, 'S': -1, 'W': -1, 'E': 1}
    ret = (int(x[:-1]) * converters[x[-1]]) / 10
    return ret

@dataclass
class BestTrackData:
    _BASIN: str
    _CY: str
    _YYYYMMDDHH: str
    _TECHNUM_MIN: str
    _TECH: str
    _TAU: str
    _LatNS: str
    _LonEW: str
    _VMAX: str
    _MSLP: str
    _TY: str
    _RAD: str
    _WINDCODE: str
    _RAD1: str
    _RAD2: str
    _RAD3: str
    _RAD4: str
    _RADP: str
    _RRP: str = ""
    _MRD: str = ""
    _GUSTS: str = ""
    _EYE: str = ""
    _SUBREGION: str = ""
    _MAXSEAS: str = ""
    _INITIALS: str = ""
    _DIR: str = ""
    _SPEED: str = ""
    _STORMNAME: str = ""
    _DEPTH: str = ""
    _SEAS: str = ""
    _SEASCODE: str = ""
    _SEAS1: str = ""
    _SEAS2: str = ""
    _SEAS3: str = ""
    _SEAS4: str = ""
    _USERDEFINED: str = ""

    @property
    def datetime(self):
        return datetime(
            int(self._YYYYMMDDHH[0:4]), 
            int(self._YYYYMMDDHH[4:6]), 
            int(self._YYYYMMDDHH[6:8]), 
            int(self._YYYYMMDDHH[8:10]))
        


def decode_best_track_file(filename):
    records = []
    with open(filename, "r") as file:
        for line in file:
            line = line.strip()
            if line:
                data = map(str.strip, line.split(',', 35))
                record = BestTrackData(
                    *data
                )
                records.append(record)
    return records


# Example usage:
if __name__ == '__main__':
    import timeit
    num = 10
    x = timeit.timeit("decode_best_track_file('data/aal032023.dat')", setup="from __main__ import decode_best_track_file", number=num)
    print(x/num)
# for record in decoded_data:
#     print(record)
