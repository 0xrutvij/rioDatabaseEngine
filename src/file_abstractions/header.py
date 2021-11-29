import io
import numpy as np

from dataclasses import dataclass
from itertools import starmap
from typing import Any
from enums import PageType

PAGE_SIZE = 512
DEFAULT_AVG_LENGTH = 100

def int_to_byte_stream(int_like_val: Any, size: int):
    try:
        int_like_val.tobytes(">")
    except Exception:
        pass
    finally:
        if int_like_val >= 0:
            return int(int_like_val).to_bytes(size, "big")
        else:
            return int(int_like_val).to_bytes(size, "big", signed=True)
        

def big_endian_int(byte_st: bytes):
    # TODO: test for negative values
    return int.from_bytes(byte_st, "big")


@dataclass
class PageHeader:
    page_type: PageType
    num_cells: np.uint16
    # true value is the complete of this num ~data_start
    data_start: np.uint16
    right_relatve: np.uint32
    parent: np.uint32

    def to_byte_stream(self):
        return b''.join(
            starmap(
                int_to_byte_stream,
                [
                    (self.page_type, 1),
                    (0, 1),
                    (self.num_cells, 2),
                    (self.data_start, 2),
                    (self.right_relatve, 4),
                    (self.parent, 4),
                    (0, 2)
                ]
            )
        )

    @classmethod
    def default_header(cls, is_root=False):
        return cls(
            PageType.table_leaf_page,
            np.uint16(0),
            np.uint16(0),
            np.uint32(0),
            ~np.uint32(0) if is_root else np.uint32(0)
        )

    @classmethod
    def from_byte_stream(cls, byte_stream: bytes):
        base_obj = cls.default_header()
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)
        
        pg_type = big_endian_int(read_buff.read(1))
        base_obj.page_type = PageType.from_int(pg_type)

        read_buff.read(1)
        n_cells = big_endian_int(read_buff.read(2))
        base_obj.num_cells = np.uint16(n_cells)

        pg_data_start = big_endian_int(read_buff.read(2))
        base_obj.data_start = np.uint16(pg_data_start)

        right_relative = big_endian_int(read_buff.read(4))
        base_obj.right_relatve = np.uint32(right_relative)

        parent = big_endian_int(read_buff.read(4))
        base_obj.parent = np.uint32(parent)

        return base_obj


if __name__ == "__main__":
    page_header_obj = PageHeader.default_header()
