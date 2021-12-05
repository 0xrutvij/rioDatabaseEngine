import inspect
from re import L
from typing import Dict, List
from pyparsing import ParseSyntaxException
from table import Table
from utils.table_format import table_format_print
from utils.settings import Settings

CMD = "command"

def swtich_and_delegate(parse_dict: Dict, in_memory_tables: Dict[str, Table], 
    in_memory_indices: Dict, files_list: List[str] = None):

    command = parse_dict.get(CMD, None)

    parse_dict["mem_data"] = {
        "imt": in_memory_tables,
        "imi": in_memory_indices,
        "fl": files_list
    }

    if command.upper() not in DEFINED_CLAUSES:
        raise ParseSyntaxException("", 0, msg="Clause not detected.")

    function_ptr = DEFINED_CLAUSES[command]
    sign = inspect.signature(function_ptr)

    for function_arg in sign.parameters.keys():
        if function_arg not in parse_dict:
            raise ParseSyntaxException("", 0, msg="Malformed SQL Statement.")

    del parse_dict[CMD]

    function_ptr(**parse_dict)


def check_path_list(tname: str, file_paths: List[str]):
    # TODO: implement, and call function to load the file if needed.
    return False


def load_table(tname: str, file_paths: List[str]) -> Table:
    raise NotImplementedError


def load_indices(tname: str, file_paths: List[str]) -> List[object]:
    raise NotImplementedError


def get_table(tname: str, in_mem_tables: Dict, in_mem_idx: Dict, file_paths: List[str], creation_mode=False):
    """
    Finds the table name in list of memory tables and disk tables.
    """
    tname = tname.lower()

    if creation_mode:
        if tname in in_mem_tables:
            raise ValueError(f"Table named {tname} already exists!")
        elif check_path_list(tname, file_paths):
            raise ValueError(f"Table named {tname} already exists!")
        else:
            return False

    else:
        if tname in in_mem_tables:
            return in_mem_tables[tname]
    
        elif check_path_list(tname, file_paths):
            in_mem_tables[tname] = load_table(tname, file_paths)
            in_mem_idx[tname] = load_indices(tname, file_paths)
            return in_mem_tables[tname]

        else:
            raise FileNotFoundError(f"Table named {tname} not found!")


def create_table(table_name: str, column_list: List[Dict], mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])
    
    if not get_table(table_name, imt, imi, fl, creation_mode=True):
        imt[table_name] = Table.create_table(
            {
                "table_name": table_name,
                "column_list": column_list
            },
            page_size=Settings.get_page_size()
        )

        print(f"Table {table_name} created! \n")

    return

def create_index(table_name: str, column_name: str, mem_data: Dict):
    # should create a new b tree object in memory
    raise NotImplementedError

def drop_table(table_name: str, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])

    if table_obj := get_table(table_name, imt, imi, fl):
        del imt[table_name]
        del imi[table_name]
        del table_obj
        
    return

def insert_row(table_name: str, column_name_list: List[str], value_list: List[str], mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])

    table_obj: Table = get_table(table_name, imt, imi, fl)

    table_obj.insert(
        {
            "column_name_list": column_name_list,
            "value_list": value_list
        }
    )

    return

def delete_row(table_name: str, condition: Dict, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])

    table_obj: Table = get_table(table_name, imt, imi, fl)

    if condition:
        table_obj.delete(condition)

    else:
        table_obj.delete()

    return

def update_row(table_name: str, operation: Dict, condition: Dict, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])

    table_obj: Table = get_table(table_name, imt, imi, fl)

    table_obj.update(
        update_op=operation,
        condition=condition
    )

    return

def select_rows(table_name: str, column_name_list: List[str], condition: Dict, mem_data: Dict):
    table_name = table_name.lower()
    imt, imi, fl = map(lambda x: mem_data[x], ["imt", "imi", "fl"])

    table_obj: Table = get_table(table_name, imt, imi, fl)


    selections, cname_list = table_obj.select(
        {
            "column_name_list": column_name_list,
            "condition": condition
        }
    )

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