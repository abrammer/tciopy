from datetime import datetime
from abc import ABC, abstractmethod
import numpy as np


class ConversionDescriptor(ABC):
    _name = None
    _default = None

    def __set_name__(self, owner, name):
        self._name = "_" + name

    def __set__(self, obj, value):
        setattr(obj, self._name, value)

    @abstractmethod
    def decode(self, value):
        ...

    def __get__(self, obj, dtype):
        if obj is None:
            return self._default
        value = getattr(obj, self._name)
        if not value:
            return self._default
        return self.decode(value)

class StrConverter(ConversionDescriptor):
    def __init__(self, *, default=""):
        self._default = default

    def decode(self, value):
        return value


class IntConverter(ConversionDescriptor):
    def __init__(self, *, default=np.nan):
        self._default = default

    def decode(self, value):
        if value is self._default:
            return value
        return int(value)


class FloatConverter(ConversionDescriptor):
    def __init__(self, *, default=np.nan):
        self._default = default

    def decode(self, value):
        return float(value)


class DatetimeConverter(ConversionDescriptor):
    def __init__(self, *, datetime_format="%Y%m%d%H", default=np.datetime64("NaT")):
        self._default = default
        self.format =  datetime_format

    def decode(self, value):
        if self.format == "%Y%m%d%H":
            return datetime(int(value[:4]), int(value[4:6]), int(value[6:8]), int(value[8:10]))
        elif self.format == "%Y%m%d%H%M":
            return datetime(int(value[:4]), int(value[4:6]), int(value[6:8]), int(value[8:10]), int(value[10:12]))
        else:
            return datetime.strptime(value, self.format)


class LatLonConverter(ConversionDescriptor):
    def __init__(self, *,scale=0.1, default=None):
        super().__init__()
        self.scale = scale
        self._default = default or np.nan
        self.hemisphere_signs = {"W":-1, "S":-1}

    def decode(self, value):
        degsign = self.hemisphere_signs.get(value[-1], 1)
        return int(value[:-1])* degsign* self.scale

    

def main():
    from dataclasses import dataclass

    @dataclass
    class InventoryItem:
        date: DatetimeConverter# = DatetimeConvertor(default="1970010100")
        vmax: IntConverter# = IntConvertor(default=5)
        mslp: IntConverter# = IntConvertor(default=10)
        lat: LatLonConverter# = LatLonConvertor(default=0)
        lon: LatLonConverter# = LatLonConvertor(default=0)


    i = [
        InventoryItem("2023091018", "65", "990", "15N", "90W"),
        InventoryItem("2023091012", "65", "990", "12N", "89W"),
    ]

    import pandas as pd
    print(pd.DataFrame(i))


if __name__ == "__main__":
    main()