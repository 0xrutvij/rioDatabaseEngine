import inspect
from typing import Dict, List
from pyparsing import ParseSyntaxException

CMD = "command"

def swtich_and_delegate(parse_dict: Dict):

    command = parse_dict.get(CMD, None)

    if command not in DEFINED_CLAUSES:
        raise ParseSyntaxException("", 0, msg="Clause not detected.")

    function_ptr = DEFINED_CLAUSES[command]
    sign = inspect.signature(function_ptr)

    for function_arg in sign.parameters.keys():
        if function_arg not in parse_dict:
            raise ParseSyntaxException("", 0, msg="Malformed SQL Statement.")

    del parse_dict[CMD]

    function_ptr(**parse_dict)


def create_table(table_name: str, column_list: List[Dict]):
    raise NotImplementedError

def create_index(table_name: str, column_name: str):
    raise NotImplementedError

def drop_table(table_name: str):
    raise NotImplementedError

def insert_row(table_name: str, column_name_list: List[str], value_list: List[str]):
    raise NotImplementedError

def delete_row(table_name: str, condition: Dict):
    raise NotImplementedError

def update_row(table_name: str, operation: Dict, condition: Dict):
    raise NotImplementedError

def select_rows(table_name: str, column_name_list: List[str], condition: Dict):
    raise NotImplementedError

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