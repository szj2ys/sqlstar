# *_*coding:utf-8 *_*
import getpass
import re
import sys
import traceback
import typing
import click
import pandas as pd

import warnings
import pymysql
from sqlstar import logger

from sqlstar.core import DatabaseURL
from sqlstar.interfaces import ConnectionBackend, DatabaseBackend
from sqlstar.utils import check_dtype_mysql

warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')


class MySQLBackend(DatabaseBackend):

    def __init__(self, database_url: typing.Union[DatabaseURL, str],
                 **options: typing.Any) -> None:
        self._database_url = DatabaseURL(database_url)
        self._host = self._database_url.hostname
        self._port = self._database_url.port or 3306
        self._user = self._database_url.username or getpass.getuser()
        self._password = self._database_url.password
        self._db = self._database_url.database
        self._autocommit = True
        self._options = options
        self._connection = None

    def _get_connection_kwargs(self) -> dict:
        url_options = self._database_url.options

        kwargs = {}
        ssl = url_options.get("ssl")

        if ssl is not None:
            kwargs["ssl"] = {"true": True, "false": False}[ssl.lower()]

        return kwargs

    def connect(self) -> None:
        assert self._connection is None, "DatabaseBackend is already running"
        kwargs = self._get_connection_kwargs()
        self._connection = pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            db=self._db,
            autocommit=self._autocommit,
            # cursorclass=pymysql.cursors.DictCursor,
            **kwargs,
        )

    def disconnect(self) -> None:
        assert self._connection is not None, "DatabaseBackend is not running"
        self._connection.cursor().close()
        self._connection = None

    def connection(self) -> "MySQLConnection":
        return MySQLConnection(self, self._connection)


class MySQLConnection(ConnectionBackend):

    def __init__(self, database: MySQLBackend, connection: pymysql.Connection):
        self._database = database
        self._connection = connection

    @property
    def connection(self) -> pymysql.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection

    def fetch_all(self, query):
        """Fetch all the rows"""
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    def fetch_df(self, query: typing.Union[str], *args: typing.Any,
                 **kwargs: typing.Any):
        """Fetch data, and format result into Dataframe

        :param query:
        :return: Dataframe
        """
        assert self._connection is not None, "Connection is not acquired"

        return pd.read_sql(query, self._connection, *args, **kwargs)

    def export_csv(self,
                   query: typing.Union[str],
                   fname: typing.Union[str],
                   sep: typing.Any = ','):
        """Export result to csv"""
        df = self.fetch_df(query)
        return df.to_csv(fname, sep=sep, encoding='utf-8', index=False)

    def export_excel(self, query: typing.Union[str], fname: typing.Union[str]):
        """Export result to excel"""
        df = self.fetch_df(query)
        return df.to_excel(fname, encoding='utf-8', index=False)

    def fetch_many(self, query, size: int = None):
        """Fetch several rows"""
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        limit_match = re.search(r'\bLIMIT\s+(\d+)', query, re.IGNORECASE)
        if not size and limit_match:
            size = int(limit_match.group(1))
        try:
            cursor.execute(query)
            result = cursor.fetchmany(size)
            return result
        finally:
            cursor.close()

    def execute(self, query):
        """Execute a query

                :param str query: Query to execute.

                :return: Number of affected rows
                :rtype: int
        """
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        try:
            result = cursor.execute(query)
            return result
        except Exception:
            raise Exception(f"This SQL execution failed👇\n{query}")
        finally:
            cursor.close()

    def execute_many(self, query):
        """Run several data against one query

                :param query: query to execute on server
                :return: Number of rows affected, if any.
                :rtype: int

                This method improves performance on multiple-row INSERT and
                REPLACE. Otherwise it is equivalent to looping over args with
                execute().
        """
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        try:
            result = cursor.execute_many(query)
            return result
        finally:
            cursor.close()

    def insert_many(self, table, data: typing.Union[list, tuple],
                    cols: typing.Union[list, tuple]):
        """Insert many records

        :param table: table name
        :param data: data
        :param cols: columns
        :return:
        """
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        # 构建列名部分
        cols_str = ", ".join([f"`{col}`" for col in cols])
        # 构建占位符部分
        placeholders = ", ".join(["%s" for _ in cols])
        # 构建UPDATE部分
        update_stmt = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in cols])

        INSERT_MANY = f"""
                INSERT INTO {table} ({cols_str}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_stmt}
            """

        cursor.executemany(INSERT_MANY, data)
        logger.info(f"{table} inserts "
                    f"{len(data)} records ✨🍰✨")
        cursor.close()

    def insert_df(self, table, df: pd.DataFrame, dropna=False, **kwargs):
        """Insert Dataframe type of data

        # transform dtype
        >>> df.loc[:, col] = df.loc[:, col].astype(str)

        :param table:
        :param df: Dataframe
        :param dropna: bool

        :return:
        """
        if df.empty:
            logger.warning('There seems no data 😅')
        else:
            cols = df.columns.tolist()

            if dropna:
                df.dropna(axis=kwargs.get('axis', 0),
                          how=kwargs.get('how', 'any'),
                          thresh=kwargs.get('thresh'),
                          subset=kwargs.get('subset'),
                          inplace=True)
            else:
                df = df.astype(object).where(pd.notnull(df), None)
                df = df.replace(
                    ['None', 'NULL', 'NAN', 'NA', 'nan', 'na', 'null'], None)
            data = [tuple(row) for row in df[cols].values]
            self.insert_many(table, data, cols)

    def truncate_table(self, table):
        """Truncate table's data, but keep the table structure

        :param table:
        :return:
        """
        TRUNCATE_TABLE = """TRUNCATE TABLE {};""".format(table)

        self.execute(TRUNCATE_TABLE)
        logger.info(f"Table {table} was truncated ✨🍰✨")

    def drop_column(self, table, column: typing.Union[str, list, tuple]):
        """Drop column"""
        if isinstance(column, str):
            DROP_COLUMN = """ALTER TABLE {} DROP COLUMN {} ;""".format(
                table, column)
        if isinstance(column, (list, tuple)):
            DROP_COLUMN = """ALTER TABLE {} DROP COLUMN {} ;""".format(
                table, ',DROP COLUMN '.join([col for col in column]))

        self.execute(DROP_COLUMN)
        logger.info("Column was dropped ✨🍰✨")

    def drop_table(self, table, assure):
        """Drop table with safety checks

        Args:
            table (str): Table name to drop
            assure (bool): Whether to confirm before dropping non-empty table
        """
        if not table:
            raise ValueError("Table name cannot be empty")

        table = table.strip('`')  # 移除可能存在的反引号
        drop_sql = f"DROP TABLE IF EXISTS `{table}`"

        try:
            # 只检查是否存在数据,不需要实际获取数据
            has_data = bool(
                self.fetch_all(f'''SELECT * FROM {table} LIMIT 10;'''))

            if has_data:
                if assure:
                    if not click.confirm(
                            f"Table '{table}' contains data. Confirm drop?",
                            default=False):
                        logger.info(f"Drop table '{table}' cancelled")
                        return False

            self.execute(drop_sql)
            logger.info(f"Table '{table}' dropped successfully ✨🍰✨")
            return True

        except:
            logger.error(
                f"Failed to drop table '{table}':\n{str(traceback.format_exc())}"
            )

    def update(self, table, where: dict, target: dict):
        """Update table's data

        :param table:
        :param where:
        :param target:
        :return:
        """
        locs = []
        targets = []
        for key, value in where.items():
            locs.append(f'{key}={value}')
        for key, value in target.items():
            targets.append(f'{key}={value}')
        SQL = f"""UPDATE {table} 
        SET {' ,'.join(targets)} 
        WHERE {' and '.join(locs)};
            """
        self.execute(SQL)
        logger.info(f"Update data succsess ✨🍰✨")

    def create_table(self,
                     table: str,
                     df: pd.DataFrame = None,
                     comments: dict = None,
                     primary_key: typing.Union[str, list, tuple] = 'id',
                     dtypes: dict = None):
        """Create a MySQL table with the specified configuration.

        Args:
            table: Name of the table to create
            df: Optional DataFrame used to infer column types
            comments: Optional dict mapping column names to comment strings
            primary_key: Column(s) to use as primary key, defaults to 'id'
            dtypes: Optional dict mapping column names to MySQL data types
        """
        # Build the CREATE TABLE statement pieces
        create_prefix = f'CREATE TABLE IF NOT EXISTS `{table}` (\n'
        charset_suffix = '\n) \nDEFAULT CHARSET=utf8mb4;'

        # Handle column types
        types = dtypes or {}
        cols = df.columns.tolist() if df is not None else list(types.keys())
        comments = comments or {}

        # Normalize primary key to list
        primary_key_fields = [primary_key] if isinstance(
            primary_key, str) else list(primary_key)

        # Add auto-increment ID if needed
        columns = []
        if 'id' in primary_key_fields and 'id' not in cols:
            columns.append(
                '`id` INT AUTO_INCREMENT COMMENT "auto increment id"')

        # Build column definitions
        for col in cols:
            comment = comments.get(col, "")
            dtype = types.get(col)

            if dtype:
                col_def = f'`{col}` {dtype}'
            else:
                max_len = df[col].dropna().astype(str).str.len().max()
                col_def = f'`{col}` {check_dtype_mysql(df[col].dtypes, max_len)}'

            if comment:
                col_def += f' COMMENT "{comment}"'
            columns.append(col_def)

        # Add primary key constraint
        pk_cols = '`, `'.join(primary_key_fields)
        primary_key_def = f'\n, PRIMARY KEY (`{pk_cols}`)'

        # Assemble and execute final SQL
        create_sql = create_prefix + '\n, '.join(
            columns) + primary_key_def + charset_suffix

        self.execute(create_sql)
        logger.info(f"Table {table} was created ✨🍰✨")
        return create_sql

    def rename_table(self, table: str, name: str):
        """Rename table

        :param table:
        :param name:
        :return:
        """
        RENAME_TABLE = """ALTER TABLE {} RENAME TO {} ;""".format(table, name)
        self.execute(RENAME_TABLE)
        logger.info("Renamed table {} to {} ✨🍰✨".format(table, name))

    def rename_column(self, table: str, column: str, name: str, dtype: str):
        """Rename column

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ALTER  TABLE `table` CHANGE [column] [new column] [new type];

        :param table:
        :param column:
        :param name:
        :param dtype:
        :return:
        """
        RENAME_COLUMN = """ALTER  TABLE {} CHANGE COLUMN {} {} {};""".format(
            table, column, name, dtype)
        self.execute(RENAME_COLUMN)
        logger.info("Renamed column {} to {} ✨🍰✨".format(column, name))

    def add_column(
        self,
        table: str,
        column: str,
        dtype: str,
        comment: str = "...",
        after: str = None,
    ):
        """Add new column

        :param table:
        :param column:
        :param dtype:
        :param comment:
        :param after: insert column after which column, the default is insert
                                into the end
        :return:
        """
        MYSQL_KEYWORDS = ["CHANGE", "SCHEMA", "DEFAULT"]
        if column.upper() in MYSQL_KEYWORDS:
            logger.warning("%(column)s was SQL keyword or reserved word 😯\n" %
                           {"column": column})
            sys.exit(1)

        if after:
            ADD_COLUMN = (
                """ALTER TABLE {} ADD {} {} COMMENT '{}' AFTER {} ;""".format(
                    table, column, dtype, comment, after))
        else:
            ADD_COLUMN = """ALTER TABLE {} ADD {} {} COMMENT '{}' ;""".format(
                table, column, dtype, comment)

        self.execute(ADD_COLUMN)
        logger.info(f"Added column {column} to {table} ✨🍰✨")

    def add_table_comment(self, table: str, comment: str):
        """Add comment for table"""
        ADD_TABLE_COMMENT = """ALTER TABLE {} COMMENT '{}' ;""".format(
            table, comment)
        self.execute(ADD_TABLE_COMMENT)
        logger.info("Table comment added ✨🍰✨")

    def change_column_attribute(
        self,
        table: str,
        column: str,
        dtype: str,
        notnull: bool = False,
        comment: str = None,
    ):
        """Change column's attribute

        :param table:
        :param column:
        :param dtype:
        :param notnull:
        :param comment:
        :return:
        """
        comment = 'COMMENT "{}"'.format(comment) if comment else ""
        CHANG_COLUMN_ATTRIBUTE = """ALTER  TABLE {} MODIFY {} {} {} {};""".format(
            table, column, dtype, "NOT NULL" if notnull else "DEFAULT NULL",
            comment)
        self.execute(CHANG_COLUMN_ATTRIBUTE)
        logger.info("Column {}'s attribute was modified "
                    "✨🍰✨".format(column))

    def add_primary_key(self, table: str, primary_key: typing.Union[str, list,
                                                                    tuple]):
        """Set primary key

        :param table:
        :param primary_key:
        :return:
        """
        # checkout whether exist primary key
        result = self.execute(f'''SELECT COUNT(*) PrimaryNum
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE t
                WHERE t.TABLE_NAME ="{table}"''')

        # if primary key exist, delete it first
        if (result is not True) and (result >= 1):
            DROP_PRIMARIY_KEY = f'ALTER TABLE {table} DROP PRIMARY KEY;'
            self.execute(DROP_PRIMARIY_KEY)

        PRIMARY_KEY = ''
        if isinstance(primary_key, str):
            PRIMARY_KEY = f'`{primary_key}`'
        elif isinstance(primary_key, (list, tuple)):
            PRIMARY_KEY = f'`{"`,`".join(primary_key)}`'

        ADD_PRIMARY_KEY = f"""ALTER TABLE {table} ADD PRIMARY KEY ({PRIMARY_KEY});"""
        self.execute(ADD_PRIMARY_KEY)
        logger.info("Well done ✨🍰✨")
