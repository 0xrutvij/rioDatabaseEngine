from enum import Enum, IntEnum
from struct import pack, unpack
from typing import Any
import numpy as np
import datetime as dt
from collections import namedtuple

"""
time to milliseconds ==> (x.hour * 3600 * 1000 + x.minute * 60 * 1000 + x.second * 1000 + x.microsecond // 1000)
"""

# TODO: GENERIC_DATA_TYPE = namedtuple("GENERIC_DATA_TYPE", ["str_rep", "serial_code", "byte_size", "pytype"])

class DataType(Enum):
    # data_type enum => (str_rep, serial_code, byte_size, pytype)
    NULL = ("NULL", 0, 0, None)
    TINYINT = ("TINYINT", 1, 1, np.int8)
    SMALLINT = ("SMALLINT", 2, 2, np.int16)
    INT = ("INT", 3, 4, np.int32)
    BIGINT = ("BIGINT", 4, 8, np.int64)
    LONG = ("LONG", 4, 8, np.int64)
    FLOAT = ("FLOAT", 5, 4, np.float32)
    DOUBLE = ("DOUBLE", 6, 8, np.double)
    YEAR = ("YEAR", 8, 1, np.int8)
    TIME = ("TIME", 9, 4, dt.time)
    DATETIME = ("DATETIME", 10, 8, dt.datetime)
    DATE = ("DATE", 11, 8, dt.date)
    TEXT = ("TEXT", 12, 0, str)

    def get_id_bytes(self, value: Any):
        id_int = self.value[1]
        if self.value[0] == "TEXT":
            id_int += len(value)
        return int.to_bytes(id_int, 1, "big")


    @classmethod
    def type_id_to_type_mapping(cls):
        mapping = {}
        for _, k in cls._member_map_.items():
            v = k.value
            mapping[v[1]] = k

        return mapping

    @classmethod
    def from_id_byte(cls, big_endian_int: bytes):
        typing_map = cls.type_id_to_type_mapping()
        x = int.from_bytes(big_endian_int, "big")
        return typing_map.get(x, cls.TEXT)

    def bytes_to_typed_value(self, byte_data: bytes):
        if (type_ := self.value[0]) == "NULL":
            return None

        elif type_ in {"TINYINT", "SMALLINT", "INT", "BIGINT", "LONG", "YEAR"}:
            int_val = int.from_bytes(byte_data, "big")
            return self.value[3](int_val)

        elif type_ == "FLOAT":
            return self.val[3](unpack('f', byte_data)[0])

        elif type_ == "DOUBLE":
            return self.val[3](unpack('d', byte_data)[0])

        elif type_ == "TIME":
            ms_since_midnight = int.from_bytes(byte_data)
            hours = ms_since_midnight // 3600 * 1000
            rem_ms = ms_since_midnight % (3600 * 1000)
            minutes = rem_ms // (60*1000)
            rem_ms = rem_ms % (60 * 1000)
            seconds = rem_ms // 1000
            rem_ms = rem_ms % 1000
            mu_s = rem_ms * 1000
            return dt.time(hours, minutes, seconds, mu_s)

        elif type_ == "DATETIME":
            ms_epoch = int.from_bytes(byte_data)
            s_epoch = ms_epoch / 1000
            return dt.datetime.fromtimestamp(s_epoch)

        elif type_ == "DATE":
            ms_epoch = int.from_bytes(byte_data)
            s_epoch = ms_epoch / 1000
            return dt.date.fromtimestamp(s_epoch)

        elif type_ == "TEXT":
            return byte_data.decode()


    def typed_value_to_bytes(self, value: Any):
        if (type_ := self.value[0]) == "NULL":
            return b""

        elif type_ in {"TINYINT", "SMALLINT", "INT", "BIGINT", "LONG", "YEAR"}:
            return int(value).to_bytes(self.value[2], "big")

        elif type_ == "FLOAT":
            return pack("f", value)

        elif type_ == "DOUBLE":
            return pack("d", value)

        elif type_ == "TIME":
            if isinstance(value, dt.time):
                int_val = (value.hour * 3600 * 1000 
                          + value.minute * 60 * 1000
                          + value.second * 1000 
                          + value.microsecond // 1000)
                
                return int_val.to_bytes(self.value[2], "big")

        elif type_ == "DATETIME":
            if isinstance(value, dt.datetime):
                s_epoch = value.timestamp()
                ms_epoch = int(s_epoch * 1000)
                return ms_epoch.to_bytes(self.value[2], "big")

        elif type_ == "DATE":
            if isinstance(value, dt.date):
                s_epoch = dt.datetime.combine(value, dt.time.min).timestamp()
                ms_epoch = int(s_epoch * 1000)
                return ms_epoch.to_bytes(self.value[2], "big")

        elif type_ == "TEXT":
            if isinstance(value, str):
                return value.encode()

        return int.to_bytes(0, self.value[2], "big")




class PageType(IntEnum):
    
    index_interior_page = 2
    index_leaf_page = 10
    table_interior_page = 5
    table_leaf_page = 13

    @classmethod
    def from_int(cls, int_val: int):
        if int_val == 2:
            return cls.index_interior_page
        elif int_val == 5:
            return cls.table_interior_page
        elif int_val == 10:
            return cls.index_leaf_page
        elif int_val == 13:
            return cls.table_leaf_page
        else:
            raise ValueError


if __name__ == "__main__":
    pass