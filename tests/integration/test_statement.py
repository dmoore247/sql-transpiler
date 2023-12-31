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

    def test_validate(self):
        s = Statement("select 1", file_path="myfile.sql")
        self.assertIsNotNone(s.parse())
        self.assertIsNotNone(s.transpile())
        self.assertIsNotNone(s.classify_statement())
        self.assertIsNotNone(s.validate())

    def test_result(self):
        s = Statement("select 1", file_path="myfile.sql")
        self.assertIsNotNone(s.parse())
        self.assertIsNotNone(s.transpile())
        self.assertIsNotNone(s.classify_statement())
        self.assertIsNotNone(s.validate())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.error_class, None)
        self.assertIsNotNone(s.result().record)

    def test_parse_error(self):
        s = Statement("INTO SELECT FROM", file_path="myfile.sql")
        self.assertIsNone(s.parse())
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, "ParseError")
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception.__class__, ParseError)
        self.assertIsNotNone(s.result())

    def test_parse_analysis_error(self):
        s = Statement("SELECT * FROM newtable19293", file_path="myfile.sql")
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        plan = s.validate()
        s.classify_statement()
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())

    def test_parse_nonvalidating(self):
        s = Statement("SELECT * INTO newtable FROM oldtable", file_path="myfile.sql")
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        plan = s.validate()
        s.classify_statement()
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertEqual(s.error_class, "PARSE_SYNTAX_ERROR")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())
        self.assertTrue("PARSE_SYNTAX_ERROR" in str(s.result().exception))
        self.assertTrue("PARSE_SYNTAX_ERROR" in s.result().record()["exception"])

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
        plan = s.validate()
        self.assertIsNotNone(plan)

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
        plan = s.validate()
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertEqual(s.error_class, "PARSE_SYNTAX_ERROR")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())
        self.assertTrue("PARSE_SYNTAX_ERROR" in str(s.result().exception))
        self.assertTrue("PARSE_SYNTAX_ERROR" in s.result().record()["exception"])
        self.assertEqual("TRUNCATE", s.statement_type)
        self.assertEqual("transpile", s.strategy)

    def test_update_statistics_statement(self):
        s = Statement("update statistics mytable")
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        s.classify_statement()
        plan = s.validate()
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertEqual(s.error_class, "PARSE_SYNTAX_ERROR")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())
        self.assertTrue("PARSE_SYNTAX_ERROR" in str(s.result().exception))
        self.assertTrue("PARSE_SYNTAX_ERROR" in s.result().record()["exception"])
        self.assertEqual("UPDATE STATISTICS", s.statement_type)
        self.assertEqual("COMPUTE STATISTICS", s.strategy)

    def test_create_procedure_statement(self):
        s = Statement(
            """CREATE      Procedure [dbo].[p_reporting_user]--00:06:39
as
BEGIN

set nocount on"""
        )
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        s.classify_statement()
        plan = s.validate()
        self.assertEqual("CREATE PROCEDURE", s.statement_type)
        self.assertEqual("Notebook", s.strategy)
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertEqual(s.error_class, "PARSE_SYNTAX_ERROR")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())
        self.assertTrue("PARSE_SYNTAX_ERROR" in str(s.result().exception))
        self.assertTrue("PARSE_SYNTAX_ERROR" in s.result().record()["exception"])

    def test_unresolved_function(self):
        s = Statement(
            """SELECT isnull(nullif(common.dbo.RegexReplace(prescriber_zip, '[^\d]', ''), ''), 'Unknown') as zip FROM common.dbo.mytable s"""
        )
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNone(s.result().target_sql)
        self.assertEqual(s.result().exception, None)
        s.classify_statement()
        plan = s.validate()
        self.assertEqual("SELECT", s.statement_type)
        self.assertEqual("translate function", s.strategy)
        self.assertIsNone(plan)
        self.assertEqual(s.exception_context, "validate")
        self.assertEqual(s.error_class, "UNRESOLVED_ROUTINE")
        self.assertIsNotNone(s.error())
        self.assertIsNotNone(s.exception)
        self.assertIsNotNone(s.result())
        self.assertTrue("UNRESOLVED_ROUTINE" in str(s.result().exception))
        self.assertTrue("UNRESOLVED_ROUTINE" in s.result().record()["exception"])

    def test_create_1(self):
        sql = """CREATE TABLE [dbo].[T_Users](
	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
 CONSTRAINT [PK_T_Users] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]"""

        self.assertTrue("ASC" in sql)

        s = Statement(sql)
        self.assertIsNotNone(s.parse())
        self.assertIsNone(s.error())
        s.transpile()
        self.assertIsNotNone(s.result())
        self.assertEqual(s.result().error_class, None)
        self.assertIsNotNone(s.result().sql)
        self.assertIsNotNone(s.result().target_sql)

        target_sql = s.result().target_sql
        self.assertFalse("ASC" in target_sql, target_sql)
        self.assertFalse("CLUSTERED" in target_sql, target_sql)
        self.assertFalse("[PRIMARY]" in target_sql, target_sql)

        self.assertEqual(s.result().exception, None)
        s.classify_statement()
        plan = s.validate()
        self.assertEqual("CREATE TABLE", s.statement_type)
        self.assertEqual("transpile", s.strategy)
        self.assertIsNotNone(plan)


if __name__ == "__main__":
    import logging

    logger = logging.getLogger("SQLTranspile")
    logger.setLevel(level=logging.DEBUG)
    unittest.main(verbosity=True)
