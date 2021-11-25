import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

from src import query_parser as qp
import query_test_cases as qtc
import unittest
import json

class parsingTest(unittest.TestCase):

    def test_table_creation(self):
        res = qp.create_table_stmt.parse_string(qtc.create_table_test_case)
        res_json_string = json.dumps(res[0], indent="  ")
        self.assertEqual(res_json_string, qtc.create_table_test_result)

    def test_default_table_creation(self):
        res = qp.create_table_stmt.parse_string(qtc.create_default_table_test_case)
        res_json_str = json.dumps(res[0], indent="  ")
        self.assertEqual(res_json_str, qtc.create_default_table_test_result)

    def test_index_creation(self):
        res = qp.create_index_stmt.parse_string(qtc.create_index_test_case)
        res_json_string = json.dumps(res[0], indent="  ")
        self.assertEqual(res_json_string, qtc.create_index_test_result)

    def test_table_dropping(self):
        res = qp.drop_table_stmt.parse_string(qtc.drop_table_test_case)
        res_json_str = json.dumps(res[0], indent="  ")
        self.assertEqual(res_json_str, qtc.drop_table_test_result)

    def test_row_insertion(self):
        res = qp.insert_row_stmt.parse_string(qtc.insert_row_test_case)
        res_jstr = json.dumps(res[0], indent="  ")


if __name__ == "__main__":
    unittest.main()
        