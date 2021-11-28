from collections import deque
from dataclasses import dataclass
import io
import numpy as np

from dataclasses import dataclass, field
from bisect import insort_right
from typing import Dict, List

from header import PageHeader, int_to_byte_stream, big_endian_int, PAGE_SIZE
from record import Record

@dataclass
class PageNode:
    page_number: int
    # very important: prevent future bugs, always use def factory.
    header: PageHeader = field(default_factory=PageHeader.default_header)
    offsets: List[bytes] = field(default_factory=list)
    records: List[Record] = field(default_factory=list)

    def add_record(self, new_record: Record):
        # check for overflow by checking if the potential byte stream size exceeds 512
        # provide for a call back/ return mechanism which allows the tree structure to
        # deal with the overflow
        # returns overflow list to caller
        overflow_list = []
        insort_right(self.records, new_record)
        while self.check_overflow():
            overflow_list.append(self.records.pop())
        
        return overflow_list

    def update_record(self, update_op: Dict, condition: Dict):
        """
        Where operation and condition are of the form...
        
        operation = { "operation_type": "SET", "column_name": "FirstString", 
        "column_ord": 0, "comparator": "=", "value": "Don Juan Quixote" }

        Where value is an object of the correct type as defined in the DataType Enum
        and matches the column type.

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """
        # check for overflow by checking if the potential byte stream size exceeds 512
        # provide for a call back/ return mechanism which allows the tree structure to
        # deal with the overflow
        overflow_list = []
        self.records = Record.filter_update(self.records, update_op, condition)
        while self.check_overflow():
            overflow_list.append(self.records.pop())
        
        return overflow_list


    def delete_record(self, condition: Dict = None):
        """
        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """
        if condition is None:
            self.records = []
        else:
            self.records = Record.filter_delete(self.records, condition)

    def select_record(self, col_ord_list: List[int], condition: Dict = None) -> List[List]:
        """
        Column Ordinal List: Supplies the ordinal position of the columns to be selected

        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }

        Returns: A list of list containing the values selected. 
        """
        return Record.filter_subset_select(self.records, col_ord_list, condition)

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
            self.header.data_start = PAGE_SIZE - data_size
            offset = int_to_byte_stream(self.header.data_start, 2)
            self.offsets.append(offset)
            cell_bytes_ll.appendleft(cell)

        self.header.num_cells = num_cells
        header_bytes = self.header.to_byte_stream()
        offset_bytes = b''.join(self.offsets)
        cell_bytes = b''.join(cell_bytes_ll)
        padding_len = PAGE_SIZE - len(header_bytes) - len(offset_bytes) - len(cell_bytes)

        padding = int_to_byte_stream(0, 1) * padding_len
        self.offsets = []

        byte_stream = b''.join([
            header_bytes,
            offset_bytes,
            padding,
            cell_bytes
        ])

        return byte_stream

    def check_overflow(self):
        return len(self.to_byte_stream()) > PAGE_SIZE

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


if __name__ == "__main__":
    example = PageNode(1)
    from enums import DataType
    num_columns=np.uint8(3),
    data_types=[
        DataType.TEXT,
        DataType.INT,
        DataType.TEXT
    ]
    
    data_values=[
        "asdasda",
        10,
        "scsc"
    ]
    
    rec_list = []
    for i in range(18):
        dv = data_values.copy()
        dv[1] *= i
        ex_c = Record(
            i,
            np.uint8(3),
            data_types.copy(),
            dv
        )
        rec_list.append(ex_c)

    for ex_c in rec_list:
        ret_val = example.add_record(ex_c)
        #print(ret_val or None)

    example.update_record(
        {
            "operation_type": "SET",
            "column_name": "FirstString",
            "column_ord": 0,
            "comparator": "=",
            "value": "Don Juan Quixote"
        },
        {
            "negated": "FALSE",
            "column_name": "MiddleInt",
            "column_ord": 1,
            "comparator": "=",
            "value": 0
        }
    )

    sel_list = example.select_record([], {
        "negated": "TRUE",
        "column_name": "FirstString",
        "column_ord": 0,
        "comparator": "=",
        "value": "Don Juan Quixote"
    })
    # all columns for record where FirstString isn't "Don Juan ...""
    print(sel_list)

    # first string is not equal to value "Don Juan ..."
    example.delete_record(condition={
            "negated": "TRUE",
            "column_name": "FirstString",
            "column_ord": 0,
            "comparator": "=",
            "value": "Don Juan Quixote"
    })

    with open("len_check.hex", "wb") as op:
        op.write(example.to_byte_stream())

    y = PageNode.from_byte_stream(example.to_byte_stream(), 1)
    assert example == y