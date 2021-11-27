import io
import numpy as np
import sys
import pathlib

from collections import deque
from dataclasses import dataclass, field
from itertools import starmap
from typing import Any, List

from enums import PageType, DataType

PAGE_SIZE = 512
DEFAULT_AVG_LENGTH = 100

def int_to_byte_stream(int_like_val: Any, size: int):
    return int(int_like_val).to_bytes(size, "big")

def big_endian_int(byte_st: bytes):
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
                    (PAGE_SIZE - self.data_start, 2),
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
        base_obj.data_start = np.uint16(PAGE_SIZE - pg_data_start)

        right_relative = big_endian_int(read_buff.read(4))
        base_obj.right_relatve = np.uint32(right_relative)

        parent = big_endian_int(read_buff.read(4))
        base_obj.parent = np.uint32(parent)

        return base_obj


@dataclass
class Record:

    row_id: np.uint32
    num_columns: np.uint8
    data_types: List[DataType]
    data_values: List

    def _cell_payload_byte_stream(self):
        
        acc_list = []
        type_id_list = []
        for v, typ in zip(self.data_values, self.data_types):
            acc_list.append(typ.typed_value_to_bytes(v))
            type_id_list.append(typ.get_id_bytes(v))

        dval_bytes = b"".join(acc_list)
        data_type_bytes = b"".join(type_id_list)

        return int_to_byte_stream(self.num_columns, 1) + data_type_bytes + dval_bytes

    def to_byte_stream(self):
        payload = self._cell_payload_byte_stream()
        payload_size = len(payload)
        row_id_byte_stream = int_to_byte_stream(self.row_id, 4)
        return b''.join([
            int_to_byte_stream(payload_size, 2),
            row_id_byte_stream,
            payload
        ])

    @classmethod
    def from_byte_stream(cls, byte_stream: bytes):
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)
        
        d_types = []
        d_vals = []

        payload_size_b = read_buff.read(2)
        row_id_b = read_buff.read(4)
        row_id = big_endian_int(row_id_b)
        
        num_cols_b = read_buff.read(1)
        num_cols = big_endian_int(num_cols_b)

        for _ in range(num_cols):
            type_id_byte = read_buff.read(1)
            d_type = DataType.from_id_byte(type_id_byte)
            d_types.append((d_type, big_endian_int(type_id_byte)))


        d_types_clean = []
        for d_type, type_id_int in d_types:
            if d_type is DataType.NULL:
                continue
            elif d_type is DataType.TEXT:
                read_len = type_id_int - d_type.value[1]
            else:
                read_len = d_type.value[2]

            val_bytes = read_buff.read(read_len)
            typed_val = d_type.bytes_to_typed_value(val_bytes)
            d_vals.append(typed_val)
            d_types_clean.append(d_type)

        return cls(
            row_id,
            num_cols,
            d_types_clean,
            d_vals
        )



@dataclass
class PageNode:
    page_number: int
    # very important: prevent future bugs, always use def factory.
    header: PageHeader = field(default_factory=PageHeader.default_header)
    offsets: List[bytes] = None
    records: List[Record] = None

    def to_byte_stream(self):
        self.offsets = []
        cell_bytes_ll = deque()
        for record in reversed(self.records):
            self.header.num_cells += np.uint16(1)
            cell = record.to_byte_stream()
            cell_size = len(cell)

            self.header.data_start += np.uint16(cell_size)
            offset = int_to_byte_stream(PAGE_SIZE - self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        header_bytes = self.header.to_byte_stream()
        offset_bytes = b''.join(self.offsets)
        cell_bytes = b''.join(cell_bytes_ll)
        padding_len = PAGE_SIZE - len(header_bytes) - len(offset_bytes) - len(cell_bytes)

        padding = int_to_byte_stream(0, 1) * padding_len
        self.offsets = []

        return b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])

    @classmethod
    def from_byte_stream(cls, byte_stream: bytes, pg_num: int):
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        header_bytes = read_buff.read(16)
        header = PageHeader.from_byte_stream(header_bytes)
        num_cells = header.num_cells

        read_buff.seek(16)

        offsets_paired = list()

        for _ in range(num_cells):
            cell_offset_bytes = read_buff.read(2)
            cell_offset_int = big_endian_int(cell_offset_bytes)
            offsets_paired.append((cell_offset_bytes, cell_offset_int))


        records = list()
        for _, ci in offsets_paired:
            read_buff.seek(ci)
            rec_len = big_endian_int(read_buff.read(2))
            read_buff.seek(ci)
            record_bytes = read_buff.read(6 + rec_len)
            rec = Record.from_byte_stream(record_bytes)
            records.append(rec)

        return cls(
            pg_num,
            header,
            list(),
            records
        )

