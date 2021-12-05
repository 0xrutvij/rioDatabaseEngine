import os
import traceback
from typing import Dict

import pandas as pd
from table import Table
from utils.utils import splash_screen, SEP_LINE, blockPrint, enablePrint
from utils.settings import Settings
import query_parser as qp
from command_switcher import swtich_and_delegate
from utils.create_database import (create_riobase_columns, 
                                   create_riobase_tables, 
                                   fill_riobase_tables, 
                                   fill_riobase_columns,
                                   riobase_tables_cdata,
                                   riobase_columns_cdata
                                   )
import readline
import shutil
from zipfile import ZipFile, is_zipfile

DEBUG = False
TBL_FILE_EXT = ".tbl"
IDX_FILE_EXT = ".ndx"
DATABASE_FOLDER = "rio.db"
DATABASE_ARCHIVE = "rio.db.zip"

Settings.set_prompt("riodb> ")
Settings.set_page_size(512)


def repl():
    splash_screen()

    in_memory_tables = {}
    in_memory_indices = {}
    
    load_db(in_memory_tables, in_memory_indices)

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

    elif usr_input.lower() == "init db;":
        create_database(tables, indices)

    elif usr_input.lower() == "save;":
        save_to_disk(tables, indices)

    else:
        parse_dict = qp.statement.parse_string(usr_input)[0]
        swtich_and_delegate(parse_dict, tables, indices)


def create_database(tables: Dict, indices: Dict):
    swtich_and_delegate(create_riobase_tables, tables, indices)
    swtich_and_delegate(create_riobase_columns, tables, indices)
    
    for entry in fill_riobase_tables:
        try:
            swtich_and_delegate(entry, tables, indices)
        except:
            traceback.print_exc()
        
    for entry in fill_riobase_columns:
        try:
            swtich_and_delegate(entry, tables, indices)
        except:
            traceback.print_exc()
    
    save_to_disk(tables, indices)


def save_to_disk(tables: Dict[str, Table], indices: Dict):
    #TODO: repeat process for indices...

    # create the archive file if it doesn't exist
    # else make exisiting one as a backup.
    create_db_archive()

    # check if it is a valid zip file
    is_zipfile(DATABASE_FOLDER)

    # open database folder, save all tables as files.
    with ZipFile(DATABASE_FOLDER, "a") as database:
        for k, tab in tables.items():
            fname = f"{k}.tbl"
            database.writestr(fname, tab.to_byte_stream())


def create_db_archive():

    if os.path.exists(DATABASE_FOLDER):
        os.rename(DATABASE_FOLDER, f"{DATABASE_FOLDER}.bkp")
        
    x = ZipFile(DATABASE_FOLDER, "x")
    x.close()
    
        
def load_db(tables: Dict, indices: Dict):
    
    # check if it is a valid zip file
    if os.path.exists(DATABASE_FOLDER) and is_zipfile(DATABASE_FOLDER):
        with ZipFile(DATABASE_FOLDER, "r") as database:
            
            table_of_tables_bytes = database.read("riobase_tables.tbl")
            blockPrint()
            table_of_tables = Table.from_byte_stream(table_of_tables_bytes, 512, 2, riobase_tables_cdata, "riobase_tables")
            enablePrint()
            tables["riobase_tables"] = table_of_tables
            
            table_of_columns_bytes = database.read("riobase_columns.tbl")
            blockPrint()
            table_of_columns = Table.from_byte_stream(table_of_columns_bytes, 512, 11, riobase_columns_cdata, "riobase_columns")
            enablePrint()
            tables["riobase_columns"] = table_of_columns
            
            # get record cound of tot
            pdict = qp.statement.parse_string("select record_count from riobase_tables where table_name = 'riobase_tables';")[0]
            pdict["ret_mode"] = True
            table_rec_count = swtich_and_delegate(pdict, tables, indices)
            
            pdict = qp.statement.parse_string("select record_count from riobase_tables where table_name = 'riobase_columns';")[0]
            pdict["ret_mode"] = True
            col_rec_count = swtich_and_delegate(pdict, tables, indices)
            
            tables["riobase_tables"].record_count = table_rec_count[-1][-1]
            tables["riobase_columns"].record_count = col_rec_count[-1][-1]
            
            
            #get all table names
            pdict = qp.statement.parse_string("show tables;")[0]
            pdict["ret_mode"] = True
            table_names = swtich_and_delegate(pdict, tables, indices)
            for tname_l in table_names:
                tname = tname_l[-1]
                if tname not in tables:
                    tbytes = database.read(f"{tname}.tbl")
                    tables[tname] = Table.from_byte_stream(tbytes)
                    
        
    
    
     

if __name__ == "__main__":
    repl()