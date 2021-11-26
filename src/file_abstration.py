from dataclasses import dataclass
from os import read
from typing import Any, List
import numpy as np
from itertools import starmap
from collections import deque
import io
import unittest
from hexdump import hex_dump_main

from enums import PageType, DataType

PAGE_SIZE = 512
DEFAULT_AVG_LENGTH = 100

def int_to_byte_stream(int_like_val: Any, size: int):
    return int(int_like_val).to_bytes(size, "big")

def val_to_byte_stream(str_or_int_like: Any, size: int):
    try:
        return int_to_byte_stream(str_or_int_like, size)
    except ValueError as e:
        return str_or_int_like.encode()

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


@dataclass
class Record:

    row_id: np.uint32
    num_columns: np.uint8
    data_types: List[DataType]
    data_values: List

    def _cell_payload_byte_stream(self):
        ex_bytes = lambda x: int_to_byte_stream(x.value[1], 1)
        data_type_bytes = b''.join(
            map(ex_bytes, self.data_types)
        )

        acc_list = []
        for s in zip(self.data_values, [x.value[2] for x in self.data_types]):
            acc_list.append(val_to_byte_stream(*s))

        dval_bytes = b"".join(acc_list)

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



@dataclass
class PageNode:
    page_number: int
    header: PageHeader = PageHeader.default_header()
    offsets: List[bytes] = None
    records: List[Record] = None

    def to_byte_stream(self):
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

        return b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])








class FileAbstractDevTests(unittest.TestCase):

    def setUp(self) -> None:
        self.output = io.BytesIO()
        return super().setUp()

    def header_to_bytes_test(self):
        ex = PageHeader.default_header(True)
        b = self.output.write(ex.to_byte_stream())
        print(self.output.getvalue())
        self.assertEqual(b, 16)

    def record_to_bytes_test(self):
        ex = Record(
            row_id=1,
            num_columns=np.uint8(10),
            data_types=[
                DataType.INT,
                DataType.TEXT
            ],
            data_values=[
                10,
                "scsc"
            ]
        )
        print(ex.to_byte_stream())
        x = self.output.write(ex.to_byte_stream())
        print(x)
        #self.assertEqual(x, 11)

    def page_to_bytes_test(self):
        ex = Record(
            row_id=1,
            num_columns=np.uint8(10),
            data_types=[
                DataType.TEXT,
                DataType.INT,
                DataType.TEXT
            ],
            data_values=[
                "asdasda",
                10,
                "scsc"
            ]
        )
        y = PageNode(
            1,
            records=[ex],
            offsets=[]
        )
        op_len = self.output.write(y.to_byte_stream())

        with open("filedump2.txt", "wb") as op:
            op.write(self.output.getvalue())

        self.assertEqual(op_len, 512)

if __name__ == "__main__":
    suite = unittest.TestSuite()
    # suite.addTest(FileAbstractDevTests("header_to_bytes_test"))
    # suite.addTest(FileAbstractDevTests("record_to_bytes_test"))
    suite.addTest(FileAbstractDevTests("page_to_bytes_test"))
    runner = unittest.TextTestRunner()
    runner.run(suite)