"""
This class will only be used to convert a BPlusNode to a disk writable byte stream
and vice versa.

TODO: The CRUD methods need to be removed.
"""
from collections import deque
from dataclasses import dataclass
import io
import numpy as np

from dataclasses import dataclass, field
from typing import List
from bplus_tree import BPlusNode
from btree import DataPointer

from header import PageHeader, int_to_byte_stream, big_endian_int
from record import Record, RouterCell
from enums import PageType

PAGE_SIZE_DEFAULT = 512

@dataclass
class LeafPageWriter:
    page_number: int
    # very important: prevent future bugs, always use def factory.
    header: PageHeader = field(default_factory=PageHeader.default_header)
    offsets: List[bytes] = field(default_factory=list)
    records: List[Record] = field(default_factory=list)
    page_size: int = PAGE_SIZE_DEFAULT

    def to_byte_stream(self):
        self.offsets = deque()
        cell_bytes_ll = deque()
        num_cells = np.uint16(0)

        data_size = np.uint16(0)

        for record in self.records:
            num_cells += np.uint16(1)
            cell = record.to_byte_stream()
            cell_size = len(cell)

            data_size = data_size + np.uint16(cell_size)
            self.header.data_start = self.page_size - data_size
            offset = int_to_byte_stream(self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        self.header.num_cells = num_cells
        header_bytes = self.header.to_byte_stream()
        offset_bytes = b''.join(self.offsets)
        cell_bytes = b''.join(cell_bytes_ll)
        padding_len = self.page_size - len(header_bytes) - len(offset_bytes) - len(cell_bytes)

        padding = int_to_byte_stream(0, 1) * padding_len
        self.offsets = []

        byte_stream = b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])

        return byte_stream

    def to_bpnode(self) -> BPlusNode:
        x = BPlusNode(True, None)
        x.keys = []
        
        for rec in self.records:
            dp = DataPointer(
                Record.get_id,
                rec
            )
            x.keys.append(dp)

        x.keys.sort(key=DataPointer.get_id)
        return x


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

@dataclass
class InternalPageWriter:
    page_number: int
    # very important: prevent future bugs, always use def factory.
    header: PageHeader = field(default_factory=PageHeader.default_header)
    offsets: List[bytes] = field(default_factory=list)
    keys: List[RouterCell] = field(default_factory=list)
    last_child_pg: int = field(default_factory=int)
    page_size: int = PAGE_SIZE_DEFAULT


    def to_byte_stream(self, parent_page_num: int):
        self.offsets = deque()
        cell_bytes_ll = deque()
        num_cells = np.uint16(0)

        data_size = np.uint16(0)

        for key in self.keys:
            num_cells += np.uint16(1)
            cell = key.to_byte_stream()
            cell_size = len(cell)

            data_size = data_size + np.uint16(cell_size)
            self.header.data_start = self.page_size - data_size
            offset = int_to_byte_stream(self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        self.header.num_cells = num_cells
        self.header.page_type = PageType.table_interior_page
        self.header.parent = np.uint32(parent_page_num)
        self.header.right_relatve = np.uint32(self.last_child_pg)

        header_bytes = self.header.to_byte_stream()
        offset_bytes = b''.join(self.offsets)
        cell_bytes = b''.join(cell_bytes_ll)
        padding_len = self.page_size - len(header_bytes) - len(offset_bytes) - len(cell_bytes)

        padding = int_to_byte_stream(0, 1) * padding_len
        self.offsets = []

        byte_stream = b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])

        return byte_stream

    def to_bpnode(self):
        #TODO: Implement for a truer, more efficient implementation.
        raise NotImplementedError
    
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


        router_cells = list()
        for _, ci in offsets_paired:
            read_buff.seek(ci)
            cell_bytes = read_buff.read(8)
            rec = RouterCell.from_byte_stream(cell_bytes)
            router_cells.append(rec)

        return InternalPageWriter(
            pg_num,
            header,
            list(),
            router_cells,
            header.right_relatve
        )


if __name__ == "__main__":

    r_cells = []
    for i in range(5):
        r_cells.append(RouterCell(i+10, i+11))

    te = InternalPageWriter(
        10,
        keys=r_cells,
        last_child_pg=16,
        page_size=512
    )

    with open("internal_page.hex", "wb") as of:
        of.write(te.to_byte_stream(9))