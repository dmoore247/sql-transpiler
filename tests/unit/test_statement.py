import unittest

import sqlglot
from sqlglot.errors import ParseError

from statement import Statement


class TestStatement(unittest.TestCase):
    def test_construtor(self):
        s = Statement("select 1", file_path="myfile")
        self.assertIsNotNone(s)

    def test_parse(self):
        s = Statement("select 1", file_path="myfile.sql")
        s.parse()

    def test_transpile(self):
        s = Statement("select 1", file_path="myfile.sql")
        s.parse()
        s.transpile()

    def test_transpile_error(self):
        s = Statement("select 1", file_path="myfile.sql")
        s.parse()
        s.transpile()
        s.error()

    def test_classify(self):
        s = Statement("select 1", file_path="myfile.sql")
        self.assertIsNotNone(s.parse())
        self.assertIsNotNone(s.transpile())
        self.assertIsNotNone(s.classify_statement())

    def test_parse_error(self):
        s = Statement("INTO SELECT FROM", file_path="myfile.sql")
        self.assertIsNone(s.parse())
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, "ParseError")
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertIsNotNone(s.result())

    def test_set_statement(self):
        s = Statement("SET KEY VALUE")
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        s.classify_statement()

    def test_truncate_statement(self):
        s = Statement("truncate table common..t_reporting_table")
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        s.classify_statement()


# if __name__ == '__main__':
#    import logging
#    logger = logging.getLogger('SQLTranspile')
#    logger.setLevel(level=logging.DEBUG)
#    unittest.main(verbosity=True)
