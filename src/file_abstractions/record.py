from __future__ import annotations
from enums import DataType
import io
import numpy as np

from dataclasses import dataclass
from functools import total_ordering
from itertools import filterfalse
from typing import Dict, List, Union
from operator import lt, gt, eq, ne, ge, le, itemgetter

from header import int_to_byte_stream, big_endian_int

_CONDITION_NEGATED = {
    "=": "<>",
    ">=": "<",
    "<=": ">"
}

CONDITION_NEGATED = {v: k for k, v in _CONDITION_NEGATED.items()} | _CONDITION_NEGATED

COMP_FUNCT = {
    "=": eq,
    ">=": ge,
    "<=": le,
    "<>": ne,
    ">": gt,
    "<": lt
}

@dataclass
@total_ordering
class Record:

    row_id: np.uint32
    num_columns: np.uint8
    data_types: List[DataType]
    data_values: List

    def get_id(self):
        return self.row_id

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

    def matches_condition(self, condition: Dict) -> bool:
        """
        where the condition if of the form
        Table class will convert ("column_name": "Country") to ("column_ord": 1 ... n)
        "condition": {
            "negated": "FALSE",
            "column_name": "Country",
            "column_ord": 1 ... n,
            "comparator": "=",
            "value": "Mexico"
        }
        """
        lval = self.data_values[condition["column_ord"]]
        rval = condition["value"]
        comp = condition["comparator"]
        if condition["negated"] == "TRUE":
            comp = CONDITION_NEGATED[comp]

        return COMP_FUNCT[comp](lval, rval)

    def update_val(self, operation: Dict):
        """
        where an operation is of the form
        Table class will convert ("column_name": "Country") to ("column_ord": 1 ... n)
        "operation": {
            "operation_type": "SET",
            "column_name": "Country",
            "column_ord": 1 ... n,
            "comparator": "=",
            "value": "Juan"
        },
        the "comparator" is always "=" as in the assignment operator.
        """
        rval = operation["value"]
        type_ = self.data_types[operation["column_ord"]]
        type_cast = type_.value[3]
        col_name = operation["column_name"]

        if isinstance(rval, type_cast):
            self.data_values[operation["column_ord"]] = type_cast(rval)
            return self
        else:
            raise TypeError(f"{rval} isn't a valid value for {col_name} of type {type_.value[0]}")


    def __lt__(self, other: Union[Record, int]):
        
        if isinstance(other, int):
            self.row_id < other
        
        return self.row_id < other.row_id
        

    def __eq__(self, __o: Union[Record, int]) -> bool:
        
        if isinstance(__o, int):
            return self.row_id == __o
        
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

    @classmethod
    def filter_update(cls, ls: List[Record], operation: Dict, condition: Dict) -> List[Record]:
        filter_ = cls.filter_func(condition)
        update_ = cls.record_updater(operation)
        return [update_(rec) if filter_(rec) else rec for rec in ls]

    @classmethod
    def filter_delete(cls, ls: List[Record], condition: Dict) -> List[Record]:
        filter_ = cls.filter_func(condition)
        return list(filterfalse(filter_, ls))

    @classmethod
    def filter_subset_select(cls, ls: List[Record], col_ord_list: List[int], condition: Dict = None) -> List[List]:
        filter_ = cls.filter_func(condition) if condition else lambda x: True
        columns_filter = itemgetter(*col_ord_list) if col_ord_list else lambda x: tuple(x)
        cf_wrapper = lambda x: list(r) if isinstance((r := columns_filter(x)), tuple) else [r]

        return [cf_wrapper(rec.data_values) for rec in filter(filter_, ls)]

    @classmethod
    def filter_func(cls, condition: Dict):
        return lambda x: cls.matches_condition(x, condition)

    @classmethod
    def record_updater(cls, operation: Dict):
        return lambda x: cls.update_val(x, operation)