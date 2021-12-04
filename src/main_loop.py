import os
import traceback
from typing import Dict
from utils.utils import splash_screen, SEP_LINE
from utils.settings import Settings
import query_parser as qp
from command_switcher import swtich_and_delegate
import readline

DEBUG = False
Settings.set_prompt("riodb> ")
Settings.set_page_size(1024)


def repl():
    splash_screen()

    in_memory_tables = {}
    in_memory_indices = {}

    while not Settings.is_exit():
        print("\n")
        usr_in = [input(Settings.get_prompt())]

        cont = not usr_in[0].endswith(";")

        while cont:
            temp = input()
            if temp.endswith(";") or temp == "":
                cont = False
            usr_in += [temp]

        usr_in = " ".join(usr_in)
        try:
            query_delegator(usr_in, in_memory_tables, in_memory_indices)
        except Exception as e:
            if DEBUG:
                traceback.print_exc()
            else:
                print("\n")
                print(SEP_LINE)
                print(traceback.format_exception_only(e.__class__, e)[-1])
                print(SEP_LINE)
            continue



def query_delegator(usr_input: str, tables: Dict, indices: Dict):

    if usr_input.lower() == "exit;":
        Settings.set_exit(True)
        return

    elif usr_input.lower() == "clear;":
        os.system("clear")

    else:
        parse_dict = qp.statement.parse_string(usr_input)[0]
        swtich_and_delegate(parse_dict, tables, indices)



if __name__ == "__main__":
    repl()