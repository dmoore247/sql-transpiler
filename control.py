import os
import glob
import re
import logging
logging.basicConfig(filename="transpile.log",
                    format='%(asctime)s %(levelname)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('SQLTranspile')
    
from statement import Statement, ParseResult

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.profile('E2DEMO').getOrCreate()


class ParseControler:
    """
        The SQL parse & transpile controler. 
        This class runs the sqlglot parser/transpiler over all the statements in all the files. 
        The results are logged to a delta table.
    """
    def __init__(self, table_name:str, validate=True, save=False, overwrite=False, read="tsql"):
        self.table_name = table_name
        self.validate = validate
        self.read = read
        self.save = save
        self.overwrite = overwrite

    def drop(self):
        if self.overwrite:
            logger.debug(f'DROP TABLE IF EXISTS {self.table_name}')
            spark.sql(f'DROP TABLE IF EXISTS {self.table_name}')

    def save_results(self, results:list):
        from pyspark.sql.functions import expr
        logger.info(f"Saving {len(results)} results to {self.table_name}")
        r = [result.record() for result in results]
        schema = results[0].schema()
        df = spark.createDataFrame(r, schema).withColumn('insert_dt',expr('now()'))
        mode = 'append'
        
        logger.debug(f'saveAsTable({self.table_name})')
        df.write.mode(mode).option('mergeSchema','true').saveAsTable(self.table_name)

    def run_statement(self, sql:str, file_path:str) -> ParseResult:
        """Operate on a single SQL statement

        Args:
            sql (str): SQL text to be parsed
            file_path (str): path to file where the SQL came from

        Returns:
            ParseResult: The result of the parsing, transpiling, validation.
        """
        s = Statement(sql, file_path=file_path, dialect=self.read)
        s.parse()
        s.transpile()
        s.classify_statement()
        if self.validate:
            s.validate()

        return s.result()

    def run_parse(self, sqls:str, file_path = None):
        logger.debug(sqls)
        statement = 0
        for i,sql in enumerate(sqls.split(';')):
            if sql is not None and len(sql) > 3:
                yield self.run_statement(sql, file_path)

    def run_files(self, path:str):
        """ For path with glob, yield sql text"""
        paths = glob.glob(path)
        for sql_file in paths:
            with open(sql_file,'r') as f:
                logger.info(f"Processing {sql_file}")
                yield sql_file, f.read()

    def run_tsql(self, sql_blob:str):
        """
            Split SQL Blob on GO or ';' or SQL comments (--)
        """
        pattern = '\nGO\n|;|^(--.*)+$'
        for statement in re.split(pattern, sql_blob):
            if not statement:
                continue
            statement = statement.strip()
            logger.debug(statement)
            yield statement

    def run_all(self, path:str):
        assert path is not None
        
        self.drop()
        for file_name, sql_blob in self.run_files(path):
            logger.info(f"Running {file_name}")
            results = []
            for sql_statement in self.run_tsql(sql_blob):
                for result in self.run_parse(sql_statement, file_name):
                    results.append(result)
            if self.save:
                self.save_results(results)

if __name__ == "__main__":
    import sqlglot
    
    logger.setLevel(level=logging.DEBUG)
    logger.info("Running")
    c = ParseControler(table_name="douglas_moore.sqlglot.project1", validate=True, save=True, overwrite=True)
    c.run_all("resources/*.sql")
    logger.info("Complete")