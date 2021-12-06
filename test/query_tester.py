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

    def test_table_drop(self):
        res = qp.drop_table_stmt.parse_string(qtc.drop_table_test_case)
        res_json_str = json.dumps(res[0], indent="  ")
        self.assertEqual(res_json_str, qtc.drop_table_test_result)

    def test_row_insertion(self):
        res = qp.insert_row_stmt.parse_string(qtc.insert_row_test_case)
        res_jstr = json.dumps(res[0], indent="  ")
        self.assertEqual(res_jstr, qtc.insert_row_test_result)

    def test_show_tables(self):
        res1 = qp.statement.parse_string("SELECT table_name FROM riobase_tables;")
        res1 = json.dumps(res1[0], indent="  ")

        res2 = qp.statement.parse_string("SHOW TABLES;")
        res2 = json.dumps(res2.as_list()[0], indent="  ")
        
        self.assertEqual(res1, res2)

    def test_row_deletion(self):
        res = qp.delete_record_stmt.parse_string(qtc.delete_row_test_case_no_cond)
        res_jstr = json.dumps(res[0], indent="  ")
        
        self.assertEqual(res_jstr, qtc.delete_row_test_result_no_cond)
        
        
        
    def test_row_deletion_where(self):
        res = qp.delete_record_stmt.parse_string((qtc.delete_row_test_case))
        res_jstr = json.dumps(res[0], indent="  ")
        self.assertEqual(res_jstr, qtc.delete_row_test_result)

    def test_row_updates(self):
        res = qp.update_record_stmt.parse_string(qtc.update_row_test_case)
        res_jstr = json.dumps(res[0], indent="  ")
        self.assertEqual(res_jstr, qtc.update_row_test_result)

    def test_select_star_where(self):
        res = qp.select_statement.parse_string(qtc.select_test_case2)
        res_jstr = json.dumps(res[0], indent="  ")
        self.assertEqual(res_jstr, qtc.select_test_result2)

    def test_select_columns(self):
        res = qp.statement.parse_string(qtc.select_test_case1)
        res_jstr = json.dumps(res[0], indent="  ")
        self.assertEqual(res_jstr, qtc.select_test_result1)



if __name__ == "__main__":
    unittest.main()