from pyspark.errors import ParseException, AnalysisException
from sqlglot import expressions as exp
from sqlglot.errors import ParseError
import sqlglot
import os
import glob
import re

import logging
logger = logging.getLogger('SQLTranspile')

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.profile('E2DEMO').getOrCreate()

from dataclasses import dataclass

@dataclass
class ParseResult:
    file_path:str
    sql:str
    target_sql:str
    ast:str
    plan:str
    statement_type:str
    strategy:str
    error_class:str
    exception:str
    context:str
    sqlglot_version:str
    def schema(self):
        return """file_path STRING, 
                sql STRING, 
                target_sql STRING, 
                ast STRING, 
                plan STRING, 
                statement_type STRING, 
                strategy STRING, 
                error_class STRING, 
                exception STRING, 
                context STRING"""

    def record(self):
        return {
            "file_path": self.file_path,            # path to input file
            "sql": self.sql,                        # sql statement
            "target_sql": self.target_sql,          # transpiled sql
            "ast": str(self.ast),                   # ast
            "plan": self.plan,                      # spark plan
            "statement_type": self.statement_type,  # statement category
            "strategy": self.strategy,              # migration strategy
            "error_class": self.error_class,        # python class used in exception/error
            "exception": self.exception if self.exception is None else self.exception.__repr__(),       # serialized exception object
            "context": self.context,                # context of exception (e.g. table)
            "sqlglot": self.sqlglot_version,        # library version
        }


class Statement:
    def __init__(self, sql:str, file_path:str=None, dialect:str = "tsql", error_level=sqlglot.ErrorLevel.RAISE):
        self.sql = sql
        self.dialect = dialect
        self.file_path = file_path
        self.error_level = error_level
        self.exception = None
        self.exception_context = None
        self.ast:exp.Expression = None
        self.plan = None
        self.statement_type = None
        self.strategy = None
        self.write = "databricks"
        self.write_sql = None
        self.error_class = None
        self.context = None
        self.sqlglot_version = sqlglot.__version__
    
    def result(self) -> ParseResult:
        """Return result of parse/transpiling/validation operations

        Returns:
            ParseResult: Members of the statement class
        """
        return ParseResult(
            file_path = self.file_path,
            sql = self.sql,
            target_sql = self.write_sql,
            ast = self.ast.__repr__(),
            plan = self.plan,
            statement_type = self.statement_type,
            strategy = self.strategy,
            error_class = self.error_class,
            exception = self.exception,
            context = self.context,
            sqlglot_version = self.sqlglot_version
        )

    def error(self):
        if self.exception is not None:
            return {
                'file_path': self.file_path,
                'sql': self.sql,
                'error_class': self.error_class,
                'exception': self.exception
            }
        else:
            return None
        
    def get_ast(self):
        if not self.ast:
            self.parse()
        return self.ast

    def parse(self):
        try:
            self.ast = sqlglot.parse_one(self.sql, read=self.dialect, error_level=self.error_level)
            return self.ast
        except ParseError as e:
            self.handle_exception("parsing", self.sql, e)
        return self.ast
     
    def transpile(self):
        if not self.write_sql and self.get_ast():
            self.write_sql = self.get_ast().sql(dialect=self.write)
        return self.write_sql
    
    def classify_statement(self):
        if not self.ast:
            return
        r = self.ast.find_all(exp.Expression)
        keys = [_.key for _ in r]
        if 'select' in keys and 'into' in keys:
            self.statement_type = "SELECT INTO"
            self.strategy = "CTAS"
        elif 'update' in keys and 'from' in keys:
            self.statement_type = "UPDATE FROM"
            self.strategy = "MERGE INTO"
        elif 'create' in keys and 'TABLE' in self.ast.find(exp.Create).args['kind']:
            self.statement_type, self.strategy =  "CREATE TABLE", "transpile"
        elif 'create' in keys and 'VIEW' in self.ast.find(exp.Create).args['kind']:
            self.statement_type, self.strategy =  "CREATE VIEW", "transpile"
        elif 'create' in keys and 'INDEX' in self.ast.find(exp.Create).args['kind']:
            self.statement_type, self.strategy =  "CREATE INDEX", "OPTIMIZE ZORDER"
        elif 'create' in keys and 'PROC' in self.ast.find(exp.Create).args['kind']:
            self.statement_type, self.strategy =  "CREATE PROCEDURE", "Notebook"
        elif 'create' in keys and 'PROCEDURE' in self.ast.find(exp.Create).args['kind'].upper():
            self.statement_type, self.strategy =  "CREATE PROCEDURE", "Notebook"
        elif 'drop' == str(self.ast.key):
            self.statement_type, self.strategy = "DROP", "transpile"
        elif 'command' in keys and 'literal' in keys and 'DECLARE' in self.ast.alias_or_name:
            self.statement_type, self.strategy = "DECLARE", "SET var.<variable> = "
        elif 'command' == str(self.ast.key) and 'SET' == self.ast.this:
            self.statement_type, self.strategy = "SET", "skip"
        elif 'command' == str(self.ast.key) and 'TRUNCATE' == self.ast.this.upper():
            self.statement_type, self.strategy = "TRUNCATE", "transpile"
        elif 'command' == str(self.ast.key) and 'UPDATE STATISTICS' == self.ast.this.upper():
            self.statement_type, self.strategy = "UPDATE STATISTICS", "COMPUTE STATISTICS"
        elif 'use' == str(self.ast.key):
            self.statement_type, self.strategy = str(self.ast.key).upper(), "transpile"
        else:
            self.statement_type = str(self.ast.key).upper()

        return self.statement_type, self.strategy

    def handle_exception(self, context:str, sql:str, e:Exception):
        self.exception = e
        self.exception_context = context
        self.error_class = str(e.__class__.__name__)

        if self.error_class == "ParseError":
            self.strategy = "File sqlglot parse issue"
        else:
            pattern = '\[(.*)\]'
            text = e.args[0]
            message_search = re.search(pattern,text)
            if message_search:
                self.error_class = message_search.group(1)
    
        logger.error(f"While performing [{context}] on sql:[{sql}] statement_type: [{self.statement_type}] exception [{e}] occurred. error report [{self.error()} ]")

    def parse_analysis_exception(self, message:str):
        """Parse spark exception message for additional insight"""
        pattern = '\[(.*)\] '
        text = message

        error_class=None
        message_search = re.search(pattern, text)
        if message_search:
            self.error_class = message_search.group(1)

        object_search = re.search('Cannot resolve function (.*) on search path', text)
        if object_search:
            self.context = object_search.group(1)
            self.strategy = f"translate function"        
        else:
            object_search = re.search('The table or view (.*) cannot be found.', text)
            if object_search:
                self.context = object_search.group(1)
                self.strategy = f"Create {self.context} first"

        self.exception = str(self.plan.split('AnalysisException:')[1:])
        self.plan = None

    def validate(self) -> str:
        """
            Run spark explain COST to parse the databricks sql statement.
        """
        try:
            sql = self.transpile()
            if sql:
                logger.debug("explain COST " + sql)
                self.plan = spark.sql("explain COST " + sql).collect()[0][0]
                if "AnalysisException" in self.plan:
                    self.parse_analysis_exception(self.plan)
                    self.plan = None
                    self.exception_context = 'validate'
        except Exception as e:
            self.handle_exception('validate', sql, e)
        return self.plan
