import logging
import unittest

from sqlglot.errors import ParseError

from control import ParseControler
from statement import Statement

logging.basicConfig(level=logging.DEBUG)


class TestStatement(unittest.TestCase):
    def test_constructor(self):
        c = ParseControler()
        self.assertIsNotNone(c)

    def test_tsql_split(self):
        c = ParseControler(validate=False, save=False)
        with open("../../resources/p_reporting_user.sql", "r") as f:
            sql_blob = f.read()
        for statement in c.run_tsql(sql_blob):
            logging.info(statement)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main(verbosity=True)
