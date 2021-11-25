import sys
import os
import json
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

from src.command_switcher import swtich_and_delegate, CMD
import query_test_cases as qpc


function_call_args = [
    json.loads(qpc.select_test_result1),
    json.loads(qpc.select_test_result2),
    json.loads(qpc.create_table_test_result),
    json.loads(qpc.create_index_test_result),
    json.loads(qpc.insert_row_test_result),
    json.loads(qpc.update_row_test_result),
    json.loads(qpc.delete_row_test_result),
    json.loads(qpc.drop_table_test_result),
]


for cargs in function_call_args:
    try: 
        swtich_and_delegate(cargs.copy())
    except NotImplementedError as e:
        pass
    finally:
        print(f"Case: [{cargs[CMD]}] cleared.\n")


"""
# Code to introspect a function name @call time!
frame = inspect.currentframe()
print(f"Arg Match for {inspect.getframeinfo(frame).function}")
"""