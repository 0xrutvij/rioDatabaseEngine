from enum import Enum, IntEnum

class DataType(Enum):
    # data_type enum => (str_rep, serial_code, byte_size)
    NULL = ("NULL", 0, 0)
    TINYINT = ("TINYINT", 1, 1)
    SMALLINT = ("SMALLINT", 2, 2)
    INT = ("INT", 3, 4)
    BIGINT = ("BIGINT", 4, 8)
    LONG = ("LONG", 4, 8)
    FLOAT = ("FLOAT", 5, 4)
    DOUBLE = ("DOUBLE", 6, 8)
    YEAR = ("YEAR", 8, 1)
    TIME = ("TIME", 9, 4)
    DATETIME = ("DATETIME", 10, 8)
    DATE = ("DATE", 11, 8)
    TEXT = ("TEXT", 12, 115)

class PageType(IntEnum):
    
    index_interior_page = 2
    index_leaf_page = 10
    table_interior_page = 5
    table_leaf_page = 13