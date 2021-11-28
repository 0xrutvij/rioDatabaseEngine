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
    # should create a new b+ tree object in memory
    raise NotImplementedError

def create_index(table_name: str, column_name: str):
    # should create a new b tree object in memory
    raise NotImplementedError

def drop_table(table_name: str):
    # should delete the b+ object associated with the table name.
    raise NotImplementedError

def insert_row(table_name: str, column_name_list: List[str], value_list: List[str]):
    # should query the B+ tree object, use B+ insertion algo and locate the node
    # node should be able to locate correct location in list
        # if no overflow, store it in node 
            # AND update cell count.
            # before storage validate type, else raise Syntax or Type Exception
        # else should be able to propogate the overflow via the B+ object
            # repeat the above process at new node after the internal state is updated.
    raise NotImplementedError

def delete_row(table_name: str, condition: Dict):
    # should query the B+ tree object, use B+ deletion algo and locate the node
    # node should be able to locate correct location in list
        # and delete the row based on condition if any
            # AND update cell count.
    # NOTE: multiple page nodes could have deleted rows and there should be support for it.
    raise NotImplementedError

def update_row(table_name: str, operation: Dict, condition: Dict):
    # should query the B+ tree object, use B+ search algo and locate the node
    # node should be able to locate correct location in list
        # if no overflow, store it in node 
            # before storage validate type, else raise Syntax or Type Exception
        # else should be able to propogate the overflow via the B+ object
            # repeat the above process at new node after the internal state is updated.
    # NOTE: multiple page nodes could need row update and there should be support for it.
    raise NotImplementedError

def select_rows(table_name: str, column_name_list: List[str], condition: Dict):
    # should query the B+ tree object, use B+ search algo and locate the nodes
    # node should be able to locate correct location in list
        # and return it
    # NOTE: multiple page nodes could return rows and there should be support for it.
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