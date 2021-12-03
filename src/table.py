from __future__ import annotations
from random import random
from typing import Dict, List
import numpy as np

from bplus_tree import BPlusTree
from file_abstractions import Record, DataType
from btree import DataPointer

class Table:

    def __init__(self) -> None:

        self.bptree = BPlusTree()


    @classmethod
    def from_byte_stream(cls, byte_stream: bytes) -> Table:
        # load table file
        # find root page
        # reconstruct tree using root page
        # simple implementation:
        #           - insert one at a time
        # better implementation:
        #           - bulk load
        raise NotImplementedError

    def to_byte_stream(self) -> bytes:
        # converting a table to byte stream
        # all aspects of writing the table file header and page-file offsets
        # ordering the pages for de-serialization later
        # saving the file to disk
        # TODO: add logging and backup mechanism for failure protection
        raise NotImplementedError


    def insert(self, insert_dict: Dict) -> None:
        """
        A record information dictionary of the form 
        {
            "column_name_list": [
                "organisation_id",
                "name",
                "organisation_type",
                "parent_organisation_id"
            ],
            "table_name": "organisation",
            "value_list": [
                "123",
                "Company",
                "123 Corp",
                "-1"
            ]
        }
        """
        # TODO: set up methods for retrieving info
        # input validation against column info in table 
        # create a record and insert.
        # self.bptree.insert(record)
        raise NotImplementedError
        

    def update(self, update_op: Dict, condition: Dict) -> None:
        """
        Where operation and condition are of the form...
        
        operation = { "operation_type": "SET", "column_name": "FirstString", 
        "column_ord": 0, "comparator": "=", "value": "Don Juan Quixote" }

        Where value is an object of the correct type as defined in the DataType Enum
        and matches the column type.

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """
        # check if an index exists for the condition col (name/ord)
        # TODO: Change condition.
        if round(random):
            # find the row-id/s to be updated
            # call the search method on each id
            # if a returned node has more than one id remove the others from list
            # update the record.
            pass

        else:
            # let the leaf decide whether the update is necessary.
            # search from uid min (0) up until the last assigned number
            # at least one of them should be in the table if the table is not empty
            # once leaf found, apply update
            # keep applying update to each leaf by calling leaf.next!
            pass
        raise NotImplementedError


    def delete(self, condition: Dict = None):
        """
        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }
        """
        # if no condition, clear all records.
        if condition is None:
            self.bptree = BPlusTree()
        # check if an index exists for the condition col (name/ord)
        # TODO: Change condition.
        # call delete row id, the tree handles removal.
        elif round(random):
            pass
        # repeat process similar to update
        # find all rowids matching the condition
        # delete them one by one, allowing the tree to maintain invariants.
        # TODO: generalize these methods for update, delete and select since they're essentially
        # locating an object. 
        # TODO:
        #   these ops at table level will call leaf nodes to do the job
        #   thus the leaf nodes should have methods similar to page_node to further manipulate it records.
        else:
            self.records = Record.filter_delete(self.records, condition)

        raise NotImplementedError


    def select(self, col_ord_list: List[int], condition: Dict = None) -> List[List]:
        """
        TODO: write a summary of functionality
        Column Ordinal List: Supplies the ordinal position of the columns to be selected

        Condition is optional, and if provided is of the form

        condition = { "negated": "FALSE", "column_name": "MiddleInt", 
        "column_ord": 1, "comparator": "=", "value": 0 }

        Returns: A list of list containing the values selected. 
        """
        raise NotImplementedError




if __name__ == "__main__":

    row_id=1,
    num_columns=np.uint8(3),
    data_types=[
        DataType.TEXT,
        DataType.INT,
        DataType.TEXT
    ],
    data_values=[
        "asdasda",
        10,
        "scsc"
    ]

    x = BPlusTree()

    for i in range(30):
        y = Record(
            i,
            num_columns=num_columns,
            data_types=data_types,
            data_values=data_values.copy()
        )
        y.data_values[1] *= i
        dp = DataPointer[Record](
            Record.get_id,
            y
        )
        x.insert(dp)


    x.show_tree()

    n, idx = x.search(29)

    print(n.keys[idx].data)

