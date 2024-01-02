from functools import cached_property
import numpy as np
import pandas as pd
from itertools import zip_longest
from abc import ABC, abstractmethod

from tciopy.converters import StringColumn, NumericColumn, CategoricalColumn, LatLonColumn, DatetimeColumn


class BaseDeck(ABC):
    @abstractmethod
    def __init__(self):
        pass

    def __repr__(self,):
        colnames = ",\t".join(self.colnames)+"\n"
        return colnames+"\n".join([",\t".join(row) for row in self.rows()])
    
    @cached_property
    def colnames(self):
        return vars(self).keys()
    
    @cached_property
    def columns(self):
        return [(var, data) for var,data in vars(self).items()]

    def from_iterable(self, iterable):
        for row in iterable:
            self.append(row)

    def rows(self):
        for row in zip(*list(vars(self).values())):
            yield row

    def append(self, iterable):
        for (_, col), val in zip_longest(self.columns, iterable,fillvalue=""):
            col.append(val)

    def __len__(self,):
        return len(self.basin)

    def to_dataframe(self):
        columns = {name:column.pd_parse() for name, column in self.columns}
        return pd.DataFrame(columns)


class ADeck(BaseDeck):
    def __init__(self):
        self.basin = CategoricalColumn()
        self.number = NumericColumn()
        self.datetime = DatetimeColumn(datetime_format="%Y%m%d%H")
        self.tnum = NumericColumn()
        self.tech = CategoricalColumn()
        self.tau = NumericColumn()
        self.lat = LatLonColumn(scale=0.1)
        self.lon = LatLonColumn(scale=0.1)
        self.vmax = NumericColumn()
        self.mslp = NumericColumn()
        self.type = CategoricalColumn()
        self.rad = CategoricalColumn()
        self.windcode = StringColumn()
        self.rad_NEQ = NumericColumn()
        self.rad_SEQ = NumericColumn()
        self.rad_SWQ = NumericColumn()
        self.rad_NWQ = NumericColumn()
        self.pouter = NumericColumn()
        self.router = NumericColumn()
        self.rmw = NumericColumn()
        self.gusts = NumericColumn()
        self.eye = NumericColumn()
        self.subregion = StringColumn()
        self.maxseas = NumericColumn()
        self.initials = StringColumn()
        self.direction = NumericColumn()
        self.speed = NumericColumn()
        self.stormname = CategoricalColumn()
        self.depth = StringColumn()
        self.seas = NumericColumn()
        self.seascode = StringColumn()
        self.seas1 = NumericColumn()
        self.seas2 = NumericColumn()
        self.seas3 = NumericColumn()
        self.seas4 = NumericColumn()
        self.userdefined1 = StringColumn()
        self.userdata1 = StringColumn()
        self.userdefined2 = StringColumn()
        self.userdata2 = StringColumn()
        self.userdefined3 = StringColumn()
        self.userdata3 = StringColumn()
        self.userdefined4 = StringColumn()
        self.userdata4 = StringColumn()
        self.userdefined5 = StringColumn()
        self.userdata5 = StringColumn()
