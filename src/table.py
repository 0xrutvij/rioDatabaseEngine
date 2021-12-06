from __future__ import annotations
import datetime as dt
import io
import traceback
from typing import Any, Dict, List
import numpy as np
import math

from bplus_tree import BPlusNode, BPlusTree
from file_abstractions import Record, DataType
from btree import DataPointer
from header import PageHeader, big_endian_int
from page_writer import LeafPageWriter, InternalPageWriter
from enums import PageType
from record import RouterCell

COL_DATA_KEYS = {"column_names", "data_types", "nullability", "column_keys"}
class Table:

    def __init__(self, page_size = 512) -> None:
        """
        16 byte page header; 496 bytes left
        6 * (2 + max_rec_size) = 496
        max_rec_size = floor(496 / 6) - 2
        """
    
        page_space = page_size - 16
        self.max_rec_size = math.floor(page_space/6) - 2

        print(f"\n\nFor a page size of {page_size} bytes, the max"
              f" allowed record size is {self.max_rec_size} bytes.\n\n")

        self.bptree = BPlusTree()
        self.column_data = {
            "column_names": [],
            "data_types": [],
            "nullability": [],
            "column_keys": []
        }
        self.record_count = 0
        self.page_size = page_size
        self.name = ""

    @classmethod
    def create_table(cls, create_op: Dict, page_size=512) -> Table:
        """
        { "table_name": "new_table",
          "column_list": [
              {
                "column_key": "UNI", "is_nullable": "NO", 
                "column_name": "column_name1", "data_type": "INT", "ordinal_position": 1,
                "table_name": "new_table"
              }, 
              ...
            ]
        }
        """
        new_table = cls(page_size)

        col_list: List = create_op["column_list"]
        col_list.sort(key=lambda x: x.get("ordinal_position", 0))

        column_data_init = {
            "column_names": [],
            "data_types": [],
            "nullability": [],
            "column_keys": []
        }

        for column in col_list:
            column_data_init["column_keys"].append(column["column_key"])
            column_data_init["nullability"].append(column["is_nullable"])
            column_data_init["column_names"].append(column["column_name"])
            dtype_enum = DataType.from_type_name(column["data_type"])
            column_data_init["data_types"].append(dtype_enum)
        
        new_table.update_metadata(
            column_data=column_data_init,
            record_count=0,
            name=create_op["table_name"]
        )

        return new_table

    def update_metadata(self, column_data: Dict, record_count: int, name: str):
        # TODO: set up methods for retrieving info
        if COL_DATA_KEYS.difference(set(column_data.keys())) == set():
            self.column_data = column_data
            if column_data["data_types"] and isinstance(column_data["data_types"][0], str):
                column_data["data_types"] = list(map(DataType.from_type_name, column_data["data_types"])) 
            self.record_count = record_count
            self.name = name

        else:
            print("Missing column data, update failed.")

    @classmethod
    def from_table_file(cls, file_path: str) -> Table:
        byte_stream = cls.read_table(file_path)
        return cls.from_byte_stream(byte_stream)

    @classmethod
    def from_byte_stream(cls, byte_stream: bytes, 
                         page_size: int = 512, rec_count: int = 0, 
                         cdata: Dict = {}, name: str = "", update = True) -> Table:
        """
        currently -> simple implementation: insert one at a time
        TODO: future -> better implementation: bulk load OR use file page nos.
        """
        raw = io.BytesIO(byte_stream)
        read_buff = io.BufferedRandom(raw)

        #nodes = []
        page_n = read_buff.read(page_size)
        pg_no = 0
        dps: List[DataPointer] = []

        while page_n:
            pg_type = big_endian_int(page_n[:1])

            if pg_type == 13:
                node = LeafPageWriter.from_byte_stream(page_n, pg_no)
                leaf = node.to_bpnode()
                dps.extend(leaf.keys)
            else:
                #node = InternalPageWriter.from_byte_stream(page_n, pg_no)
                pass

            #nodes.append(node)
            pg_no+=1
            page_n = read_buff.read(512)

        new_table = cls(page_size=page_size)
            
        dps.sort(key=DataPointer.get_id)
        for dp in dps:
            new_table.bptree.insert(dp)

        if update:
            new_table.update_metadata(
                cdata,
                rec_count,
                name
            )
            
        return new_table

    def to_byte_stream(self) -> bytes:
        # converting a table to byte stream
        # all aspects of writing the table file header and page-file offsets
        # ordering the pages for de-serialization later
        # saving the file to disk
        # TODO: add logging and backup mechanism for failure protection

        page_list = []

        self._recurse_to_bytes(self.bptree.root, 0, 0xFFFFFFFF, page_list)

        return b"".join(reversed(page_list))

    def _recurse_to_bytes(self, node: BPlusNode, page_num: int, parent_page_num: int, page_bytes_list: list, right_relative: int = 0):
        # root node condition.
        if node.is_leaf:
        
            head = PageHeader(
                PageType.table_leaf_page,
                num_cells=0,
                data_start=0,
                right_relatve=right_relative,
                parent=parent_page_num
            )


            writer = LeafPageWriter(
                page_number=page_num,
                header=head,
                records=[rec.data for rec in node.keys],
                page_size=self.page_size
            )

            page_bytes_list.append(writer.to_byte_stream())
            # we added one page to the list...
            # TODO: verify
            return page_num + 1
        
            
        # post-order recursion, call children left to right 
        # and they append to the page bytes list
        else:

            n_children = len(node.pointers)
            my_page_no = page_num
            ccpno = my_page_no + 1
            my_cpnos = []

            for i, child in enumerate(node.pointers):
                my_cpnos.append(ccpno)
                if i == n_children-1:
                    ccpno = self._recurse_to_bytes(child, ccpno, my_page_no, page_bytes_list, 0)
                else:
                    ccpno = self._recurse_to_bytes(child, ccpno, my_page_no, page_bytes_list, ccpno+1)

            head = PageHeader(
                PageType.table_interior_page,
                0,
                0,
                my_cpnos[-1],
                parent_page_num
            )

            r_cells =[]
            for pno, rid in zip(my_cpnos[:-1], node.keys):
                r_cells.append(RouterCell(rid, pno))

            writer = InternalPageWriter(
                my_page_no,
                head,
                [],
                r_cells,
                my_cpnos[-1],
                self.page_size
            )

            page_bytes_list.append(writer.to_byte_stream(parent_page_num))
                
            return ccpno
          
    @staticmethod
    def read_table(tfile: str):
        fb = None
        with open(tfile, "rb") as inf:
            fb = inf.read()
        
        return fb

    def insert(self, insert_dict: Dict) -> int:
        """
        A record information dictionary of the form 
        { "column_name_list": [ ... ], , "value_list": [ ... ]}
        """
        insert_dict["col_ord_list"] = self._column_name_list_to_ord(insert_dict["column_name_list"])
        # input validation against column info in table 
        # create a record and insert.
        # self.bptree.insert(record)
        insertion_values = []
        all_columns = self._column_name_list_to_ord()

        for i in all_columns:
            if i in insert_dict["col_ord_list"]:
                insertion_values.append(insert_dict["value_list"][i])
            else:
                insertion_values.append(b"")

        try:
            self._validate_insert_types(insertion_values)
        except Exception as e:
            traceback.print_exc()
            # TODO: print(traceback.format_exception_only(e.__class__, e)[-1])
            return
        
        insertion_record = Record(
            self.record_count,
            np.uint8(len(all_columns)),
            self.column_data["data_types"],
            insertion_values
        )

        if not self._validate_record_size(insertion_record):
            raise OverflowError(f"Record with values {insertion_values} exceeds maximum permissible record byte size of {self.max_rec_size}")

        ptr_to_record = DataPointer[Record](Record.get_id, insertion_record)
        self.bptree.insert(ptr_to_record)
        self.record_count += 1
        return self.record_count

    def update(self, update_op: Dict, condition: Dict) -> None:
        """
        Where operation and condition are of the form...
        
        operation = {"column_name": "FirstString", "column_ord": 0, 
        "comparator": "=", "value": "Don Juan Quixote" }

        Where value is an object of the correct type as defined in the DataType Enum
        and matches the column type.

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """


        if "column_ord" not in condition and condition:
            condition["column_ord"] = self._column_name_to_ord(condition["column_name"])
        
        if "column_ord" not in update_op and update_op:
            update_op["column_ord"] = self._column_name_to_ord(update_op["column_name"])

        try:
            _ = self._validate_update_types(update_op) and self._validate_condition(condition)
        except Exception as e:
            print(traceback.format_exception_only(e.__class__, e)[-1])
            return

        # check if an index exists for the condition col (name/ord)
        if False:
            # TODO: Change condition.
            # find the row-id/s to be updated from index
            # call the search method on each id
            # if a returned node has more than one id remove the others from list
            # update the record.
            raise NotImplementedError

        else:
            # once leaf found, apply update/s
            current_leaf = self._find_first_record()

            keys_to_update = []
            record_refs_agg = []

            # let the leaf decide whether the update is necessary.
            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys]

                updated_refs = Record.filter_update(record_refs, update_op, condition)

                for rec in updated_refs:
                    if not self._validate_record_size(rec):
                        raise OverflowError(f"Record with values {rec.data_values} exceeds maximum"
                                            f" permissible record byte size of {self.max_rec_size}")

                keys_to_update.extend(current_leaf.keys)
                record_refs_agg.extend(updated_refs)

                # keep applying update to each leaf by calling leaf.next!
                current_leaf = current_leaf.next

            for key, rec in zip(keys_to_update, record_refs_agg):
                    key.data = rec
        
        return

    def delete(self, condition: Dict = None):
        """
        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """

        if "column_ord" not in condition and condition:
            condition["column_ord"] = self._column_name_to_ord(condition["column_name"])
        
        # if no condition, clear all records.
        if condition is None:
            self.bptree = BPlusTree()
        
        try:
            self._validate_condition(condition)
        except Exception as e:
            print(traceback.format_exception_only(e.__class__, e)[-1])
            return

        if False:
            pass
            # TODO: index methods ...
        else:
            current_leaf = self._find_first_record()

            ids_to_delete = set()

            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys]
                record_id_set = set(key.id for key in current_leaf.keys)

                updated_refs = Record.filter_delete(record_refs, condition)

                retained_id_set = {ref.get_id():ref for ref in updated_refs}

                ids_to_delete.update(record_id_set.difference(retained_id_set))

                for key in current_leaf.keys:
                    if key.id in retained_id_set:
                        key.data = retained_id_set[key.id]

                # keep applying update to each leaf by calling leaf.next!
                current_leaf = current_leaf.next

            for id in ids_to_delete:
                self.bptree.delete(id)
        
        return

    def select(self, selection_dict: Dict) -> List[List]:
        """
        selection_dict = { "command": "SELECT", "column_name_list": [],
        "table_name": "table_name", "condition": {}}

        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }

        Returns: A list of list containing the values selected. 
        """
        col_ord_list = self._column_name_list_to_ord(selection_dict["column_name_list"])
        condition = selection_dict["condition"]
        
        if "column_ord" not in condition and condition:
            condition["column_ord"] = self._column_name_to_ord(condition["column_name"])
            
            

        selections = []

        if False:
            #TODO: Implement keys...
            pass
        else:
            current_leaf = self._find_first_record()

            while current_leaf:
                record_refs = [key.data for key in current_leaf.keys]

                sels = Record.filter_subset_select(record_refs, col_ord_list, condition)

                selections.extend(sels)
                
                current_leaf = current_leaf.next

        return selections, selection_dict["column_name_list"] or self.column_data["column_names"]

    def _column_name_list_to_ord(self, name_list: List[str] = []) -> List[int]:

        existing_cols = self.column_data["column_names"]
        
        if len(name_list) == 0:
            return list(range(len(existing_cols)))

        elif set(name_list).issubset(existing_cols):
            return list(map(self._column_name_to_ord, name_list))

        else:
            raise NameError(f"One of the column names in {name_list} not found in table {self.name}")

    def _column_name_to_ord(self, col_name: str) -> int:
        existing_cols = self.column_data["column_names"]
        
        if col_name in existing_cols:
            return existing_cols.index(col_name)
        else:
            raise NameError(f"Column Name {col_name} not found in table {self.name}.")

    def _validate_insert_types(self, value_list: List[Any]) -> bool:
        """
        Function has side-effect, modifies the value_list.
        """

        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["nullability"]
        col_role = self.column_data["column_keys"]

        for i, (typ, is_null, role, val) in enumerate(zip(types, nullables, col_role, value_list)):
            nv = self._validate_property_value_tuple(i, typ, is_null, role, val, names)
            value_list[i] = nv
            
        return True

    def _validate_property_value_tuple(self, i, typ, is_null, role, val, names, skip_uni=False):
        if val is None or val == b"" or val == "":
            if is_null == "YES":
                return 
            else:
                raise TypeError(f"Column {names[i]} of type {typ.value[0]} can't be NULL.")


        typ_string, type_class = typ.value[0], typ.value[3]

        new_val = None

        try:
            if typ_string in {"TINYINT", "SMALLINT", "INT", "BIGINT", "LONG"}:
                type_bounds = np.iinfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= int(val) <= max_)
                new_val = type_class(val)
            
            elif typ_string in {"FLOAT", "DOUBLE"}:
                type_bounds = np.finfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= float(val) <= max_)
                new_val = type_class(val)

            elif typ_string == "YEAR":
                if isinstance(val, str):
                    val = int(val)
                
                type_bounds = np.iinfo(type_class)
                min_, max_ = type_bounds.min, type_bounds.max
                pred = (min_ <= (2000 - val) <= max_)
                new_val = type_class(2000 - val)

            elif typ_string == "TIME":
                if isinstance(val, str):
                    new_val = dt.time.fromisoformat(val)
                elif isinstance(val, int):
                    min_, max_ = 0, 86400000
                    pred = (min_ <= int(val) <= max_)
                    if pred:
                        ms_since_midnight = int(val)
                        hours = ms_since_midnight // 3600 * 1000
                        rem_ms = ms_since_midnight % (3600 * 1000)
                        minutes = rem_ms // (60*1000)
                        rem_ms = rem_ms % (60 * 1000)
                        seconds = rem_ms // 1000
                        rem_ms = rem_ms % 1000
                        mu_s = rem_ms * 1000
                        new_val = dt.time(hours, minutes, seconds, mu_s)

                
            elif typ_string == "DATETIME":
                if isinstance(val, str):
                    new_val = dt.datetime.fromisoformat(val)
                    pred = True
                elif isinstance(val, int):
                    new_val = dt.datetime.fromtimestamp(val)
                    pred = True

            elif typ_string == "DATE":
                if isinstance(val, str):
                    new_val = dt.datetime.fromisoformat(val).date()
                    pred = True
                elif isinstance(val, int):
                    new_val = dt.datetime.fromtimestamp(val).date()
                    pred = True

            elif typ_string == "TEXT":
                pred = (isinstance(val, str)
                        and len(val) <= 115)
                new_val = val

        except ValueError:
            raise TypeError(f"A value for column {names[i]} of type {typ_string} can't be attained from {val}.")

        if not pred:
            raise ValueError(f"Column {names[i]} of type {typ_string} can't have the value {val}.")

        if (role == "UNI" or role == "PRI") and not skip_uni:
            found, _ = self.select({
                "condition": {
                    "negated": "FALSE",
                    "column_name": names[i],
                    "column_ord": i,
                    "comparator": "=",
                    "value": new_val
                },
                "column_name_list": [names[i]],
            })

            if found:
                raise ValueError(f"Column {names[i]} has a uniqueness constraint, and value {val} already exists.")

        return new_val

    def _validate_condition(self, condition: Dict) -> bool:
        """
        Function has side-effect, modifies the dictionary.
        """

        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["nullability"]
        col_role = self.column_data["column_keys"]

        c_ord = condition.get("column_ord", self._column_name_to_ord(condition["column_name"]))

        typ = types[c_ord]
        is_null = nullables[c_ord]
        role = col_role[c_ord]
        val = condition["value"]

        new_val = self._validate_property_value_tuple(c_ord, typ, is_null, role, val, names, True)

        condition["value"] = new_val

        return True

    def _validate_update_types(self, update_op: Dict) -> bool:
        """
        Function has side-effect, modifies the dictionary.
        """

        names = self.column_data["column_names"]
        types = self.column_data["data_types"]
        nullables = self.column_data["nullability"]
        col_role = self.column_data["column_keys"]

        c_ord = update_op.get("column_ord", self._column_name_to_ord(update_op["column_name"]))

        typ = types[c_ord]
        is_null = nullables[c_ord]
        role = col_role[c_ord]
        val = update_op["value"]

        new_val = self._validate_property_value_tuple(c_ord, typ, is_null, role, val, names)

        update_op["value"] = new_val
        
        return True

    def _validate_record_size(self, rec: Record) -> bool:
        return len(rec.to_byte_stream()) <= self.max_rec_size

    def _find_first_record(self) -> BPlusNode:
        # search from uid min (0) up until the last assigned number
        # at least one of them should be in the table if the table is not empty
        for uid in range(self.record_count):
            node, idx = self.bptree._search(self.bptree.root, uid)
            if node and idx is not None:
                return node

        return None

    
if __name__ == "__main__":

    data_values=[
        "First Col Val",
        10,
        "Second Col Val",
        dt.datetime.now().isoformat()
    ]

    col_names = [
        "FirstString",
        "MiddleInt",
        "LastString",
        "TimeOfCreation"
    ]

    cop = {
        "table_name": "test_table",
        "column_list": [
            {
                "column_key": "", "is_nullable": "NO", 
                "column_name": "FirstString", "data_type": "TEXT", "ordinal_position": 1
            },
            {
                "column_key": "", "is_nullable": "NO", 
                "column_name": "MiddleInt", "data_type": "INT", "ordinal_position": 2
            },
            {
                "column_key": "", "is_nullable": "NO", 
                "column_name": "LastString", "data_type": "TEXT", "ordinal_position": 3
            },
            {
                "column_key": "UNI", "is_nullable": "NO", 
                "column_name": "TimeOfCreation", "data_type": "DATETIME", "ordinal_position": 4
            }
        ]
    }


    table_test = Table.create_table(create_op=cop)

    for i in range(30):
        dv = data_values.copy()
        dv[1] *= i
        dv[3] = dt.datetime.now().isoformat()
        table_test.insert(
            {
                "column_name_list": col_names,
                "table_name": "test_table",
                "value_list": dv
            }
        )


    table_test.update(
        {
            "operation_type": "SET",
            "column_name": "FirstString",
            "comparator": "=",
            "value": "Don Juan Quixote"
        },
        {
            "negated": "FALSE",
            "column_name": "MiddleInt",
            "comparator": "=",
            "value": 10
        }
    )

    print("\n\nExecuting Select Statement", end="\n\n")

    selections, _ = table_test.select({
        "condition": {
            "negated": "TRUE",
            "column_name": "FirstString",
            "column_ord": 0,
            "comparator": "=",
            "value": "Don Juan Quixote"
        },
        "column_name_list": [],
    })

    print(selections, end="\n\n\n")

    print("Pre-Delete", end="\n\n")

    table_test.bptree.show_tree()
    """
    table_test.delete(condition={
            "negated": "TRUE",
            "column_name": "FirstString",
            "column_ord": 0,
            "comparator": "=",
            "value": "Don Juan Quixote"
    })

    print("\n\nPost-Delete", end="\n\n")

    table_test.bptree.show_tree()
    """
    

    #with open("table.hex", "wb") as of:
    #    of.write(table_test.to_byte_stream())

    table_t = Table.from_byte_stream(table_test.to_byte_stream())

    table_t.bptree.show_tree()



