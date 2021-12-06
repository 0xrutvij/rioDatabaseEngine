import inspect
from re import L
from typing import Dict, List
from pyparsing import ParseSyntaxException
from table import Table
from utils.table_format import table_format_print
from utils.settings import Settings
from utils.internal_queries import (insert_parse_dict_cdata, 
                                    insert_parse_dict_tdata,
                                    delete_cdata_dict,
                                    delete_tdata_dict)
import query_parser as qp
CMD = "command"

def switch_and_delegate(parse_dict: Dict, in_memory_tables: Dict[str, Table], 
    in_memory_indices: Dict):

    command = parse_dict.get(CMD, None)

    parse_dict["mem_data"] = {
        "imt": in_memory_tables,
        "imi": in_memory_indices
    }

    if command.upper() not in DEFINED_CLAUSES:
        raise ParseSyntaxException("", 0, msg="Clause not detected.")

    function_ptr = DEFINED_CLAUSES[command]
    
    if command.upper() != "SELECT":
        sign = inspect.signature(function_ptr)

        for function_arg in sign.parameters.keys():
            if function_arg not in parse_dict:
                raise ParseSyntaxException("", 0, msg="Malformed SQL Statement.")

    del parse_dict[CMD]

    return function_ptr(**parse_dict)


def get_table(tname: str, in_mem_tables: Dict, in_mem_idx: Dict, creation_mode=False):
    """
    Finds the table name in list of memory tables and disk tables.
    """
    tname = tname.lower()

    if creation_mode:
        if tname in in_mem_tables:
            raise ValueError(f"Table named {tname} already exists!")
        else:
            return False
    else:
        if tname in in_mem_tables:
            return in_mem_tables[tname]
        else:
            raise FileNotFoundError(f"Table named {tname} not found!")


def create_table(table_name: str, column_list: List[Dict], mem_data: Dict):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])
    
    if not get_table(table_name, imt, imi, creation_mode=True):
        imt[table_name] = Table.create_table(
            {
                "table_name": table_name,
                "column_list": column_list
            },
            page_size=Settings.get_page_size()
        )

        print(f"Table {table_name} created! \n")
        
    if table_name not in {"riobase_tables", "riobase_columns"}:
        table_rec_count = imt["riobase_tables"].record_count
        pdict = insert_parse_dict_tdata(table_rec_count + 1, table_name, Settings.get_page_size(), 0)
        switch_and_delegate(pdict, imt, imi)
        
        
        for column_data in column_list:
            col_rec_count = imt["riobase_columns"].record_count
            column_data["row_id"] = col_rec_count + 1
            column_data["table_name"] = column_data["table_name"].lower()
            pdict2 = insert_parse_dict_cdata(**column_data)
            switch_and_delegate(pdict2, imt, imi)

    return


def create_index(table_name: str, column_name: str, mem_data: Dict):
    # should create a new b tree object in memory
    raise NotImplementedError


def drop_table(table_name: str, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])

    if table_obj := get_table(table_name, imt, imi):
        del imt[table_name]
        #del imi[table_name]
        del table_obj
        
    p1 = delete_tdata_dict(table_name)
    switch_and_delegate(p1, imt, imi)
    p2 = delete_cdata_dict(table_name)
    switch_and_delegate(p2, imt, imi)
        
    return

def insert_row(table_name: str, column_name_list: List[str], value_list: List[str], mem_data: Dict):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])

    table_obj: Table = get_table(table_name, imt, imi)

    table_obj.insert(
        {
            "column_name_list": column_name_list,
            "value_list": value_list
        }
    )

    return

def delete_row(table_name: str, condition: Dict, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])

    table_obj: Table = get_table(table_name, imt, imi)

    if condition:
        table_obj.delete(condition)

    else:
        table_obj.delete()

    return

def update_row(table_name: str, operation: Dict, condition: Dict, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])

    table_obj: Table = get_table(table_name, imt, imi)

    table_obj.update(
        update_op=operation,
        condition=condition
    )

    return

def select_rows(table_name: str, column_name_list: List[str], condition: Dict, mem_data: Dict, ret_mode: bool = False):
    table_name = table_name.lower()
    imt, imi = map(lambda x: mem_data[x], ["imt", "imi"])

    table_obj: Table = get_table(table_name, imt, imi)


    selections, cname_list = table_obj.select(
        {
            "column_name_list": column_name_list,
            "condition": condition
        }
    )
    
    if ret_mode:
        return selections

    table_format_print(selections, cname_list)
    

DEFINED_CLAUSES = {
    "CREATE TABLE": create_table,
    "CREATE INDEX": create_index,
    "DROP TABLE": drop_table,
    "INSERT INTO TABLE": insert_row,
    "DELETE": delete_row,
    "UPDATE": update_row,
    "SELECT": select_rows
}
    
if __name__ == "__main__":
    pass