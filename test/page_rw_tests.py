import gc
import io
import os
import sys
import unittest
import numpy as np
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")


from enums import DataType
from file_abstraction import Record, PageHeader, PageNode


class FileAbstractDevTests(unittest.TestCase):

    def setUp(self) -> None:
        self.output = io.BytesIO()
        self.perm_record = Record(
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
        )
        return super().setUp()

    def tearDown(self) -> None:
        gc.collect(2)
        return super().tearDown()

    def header_to_bytes_test(self):
        ex = PageHeader.default_header(True)
        b = self.output.write(ex.to_byte_stream())
        # print(self.output.getvalue())
        self.assertEqual(b, 16)

    def record_to_bytes_test(self):
        # print(self.perm_record.to_byte_stream())
        x = self.output.write(self.perm_record.to_byte_stream())
        self.assertEqual(x, 25)

    def page_to_bytes_test(self):
        y = PageNode(
            2,
            records=[self.perm_record],
            offsets=[]
        )
        op_len = self.output.write(y.to_byte_stream())

        with open("filedump2.hex", "wb") as op:
            op.write(self.output.getvalue())

        self.assertEqual(op_len, 512)

    def bytes_to_header_test(self):
        header = PageHeader.default_header(True)
        b_stream = header.to_byte_stream()
        y = PageHeader.from_byte_stream(b_stream)
        self.assertEqual(header, y)

    def bytes_to_record_test(self):
        b_stream = self.perm_record.to_byte_stream()
        y = Record.from_byte_stream(b_stream)
        self.assertEqual(self.perm_record,  y)

    def bytes_to_page_test(self):
        y = PageNode(
            1,
            records=[self.perm_record],
            offsets=[]
        )
        b_stream = y.to_byte_stream()
        x = PageNode.from_byte_stream(b_stream, 1)
        self.assertEqual(x, y)

if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(FileAbstractDevTests("header_to_bytes_test"))
    suite.addTest(FileAbstractDevTests("record_to_bytes_test"))
    suite.addTest(FileAbstractDevTests("page_to_bytes_test"))
    suite.addTest(FileAbstractDevTests("bytes_to_header_test"))
    suite.addTest(FileAbstractDevTests("bytes_to_record_test"))
    suite.addTest(FileAbstractDevTests("bytes_to_page_test"))
    runner = unittest.TextTestRunner()
    runner.run(suite)