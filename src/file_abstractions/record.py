from enums import DataType
import io
import numpy as np

from dataclasses import dataclass
from functools import total_ordering
from typing import List

from header import int_to_byte_stream, big_endian_int

@dataclass
@total_ordering
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

    def __lt__(self, other):
        return self.row_id < other.row_id

    def __eq__(self, __o: object) -> bool:
        return self.row_id == __o.row_id

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

