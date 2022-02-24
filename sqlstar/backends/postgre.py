# *_*coding:utf-8 *_*
import getpass
import sys
import typing
import click
import pandas as pd
from rich.console import Console
import warnings
import psycopg
# https://www.psycopg.org/psycopg3

from sqlstar.core import DatabaseURL
from sqlstar.interfaces import ConnectionBackend, DatabaseBackend
from sqlstar.utils import check_dtype_postgre

warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')


class PostgreBackend(DatabaseBackend):
    def __init__(self, database_url: typing.Union[DatabaseURL, str],
                 **options: typing.Any) -> None:
        self._database_url = DatabaseURL(database_url)
        self._host = self._database_url.hostname
        self._port = self._database_url.port or 5432
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
        self._connection = psycopg.connect(dbname=self._db,
                                           user=self._user,
                                           password=self._password,
                                           host=self._host,
                                           port=self._port,
                                           autocommit=self._autocommit,
                                           **kwargs)

    def disconnect(self) -> None:
        assert self._connection is not None, "DatabaseBackend is not running"
        self._connection.cursor().close()
        self._connection = None

    def connection(self) -> "PostgreConnection":
        assert self._connection is not None, "Connection is not acquired"
        return PostgreConnection(self, self._connection)


class PostgreConnection(ConnectionBackend):
    def __init__(self, database: PostgreBackend, connection: psycopg.connect):
        self._database = database
        self._connection = connection

    @property
    def connection(self) -> psycopg.connect:
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

    def fetch_df(self, query: typing.Union[str]):
        """Fetch data, and format result into Dataframe

        :param query:
        :return: Dataframe
        """
        data = self.fetch_all(query)
        return pd.DataFrame(data)

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
        INSERT_MANY = "INSERT INTO {table} ({cols}) VALUES ({values})".format(
            table=table,
            cols=", ".join(cols),
            values=", ".join(["%s" for col in cols]))

        cursor.executemany(INSERT_MANY, data)
        Console().print(f"[bold cyan]{table}[/bold cyan] inserts [bold cyan]"
                        f"{len(data)}[/bold cyan] records ✨ 🍰 ✨")
        cursor.close()

    def insert_df(self, table, df: pd.DataFrame, dropna=True, **kwargs):
        """Insert Dataframe type of data

        # transform dtype
        >>> df.loc[:, col] = df.loc[:, col].astype(str)

        :param table:
        :param df: Dataframe
        :param dropna: bool

        :return:
        """
        if df.empty:
            Console().print('There seems to be no data 😅', style='red')
        else:
            cols = df.columns.tolist()
            if dropna:
                df.dropna(axis=kwargs.get('axis', 0),
                          how=kwargs.get('how', 'any'),
                          thresh=kwargs.get('thresh'),
                          subset=kwargs.get('subset'),
                          inplace=True)
            data = [tuple(row) for row in df[cols].values]
            self.insert_many(table, data, cols)

    def truncate_table(self, table):
        """Truncate table's data, but keep the table structure

        :param table:
        :return:
        """
        TRUNCATE_TABLE = """TRUNCATE TABLE {};""".format(table)

        self.execute(TRUNCATE_TABLE)
        Console().print(
            f"Table [bold cyan]{table}[/bold cyan] was truncated ✨ 🍰 ✨")

    def drop_column(self, table, column: typing.Union[str, list, tuple]):
        """Drop column"""
        if isinstance(column, str):
            DROP_COLUMN = """ALTER TABLE {} DROP COLUMN {} ;""".format(
                table, column)
        if isinstance(column, (list, tuple)):
            DROP_COLUMN = """ALTER TABLE {} DROP COLUMN {} ;""".format(
                table, ',DROP COLUMN '.join([col for col in column]))

        self.execute(DROP_COLUMN)
        Console().print("Column was dropped ✨ 🍰 ✨")

    def drop_table(self, table):
        """Drop table"""
        DROP_TABLE = f"""DROP TABLE IF EXISTS {table};"""
        data = self.fetch_all(f'''SELECT * FROM {table} LIMIT 10;''')

        # if the table is not empty, warning user
        if data:
            confirm = click.confirm(f"Are you sure to drop table {table} ?",
                                    default=False)
            if confirm:
                self.execute(DROP_TABLE)
        else:
            self.execute(DROP_TABLE)
        Console().print(
            f"Table [bold cyan]{table}[/bold cyan] was dropped ✨ 🍰 ✨")

    def create_table(self,
                     table,
                     df: pd.DataFrame = None,
                     comments: dict = None,
                     primary_key: typing.Union[str, list, tuple] = None,
                     dtypes: dict = None):
        """Create table"""
        from toolz import merge
        PREFIX = f'''CREATE TABLE IF NOT EXISTS {table} ('''
        SUFFIX = ''') DEFAULT CHARSET=utf8mb4;'''

        types = {}
        if dtypes:
            for dtype, type_cols in dtypes.items():
                types = merge(types, {col: dtype for col in type_cols})

        cols = df.columns.tolist() if df is not None else types.keys()

        # if there is no id, add an auto_increment id
        if ('id' not in cols) or ('id' not in primary_key):
            PREFIX += '''id INT AUTO_INCREMENT COMMENT 'id','''

        COLUMNS = []

        for col in cols:
            comment = comments.get(col, "...") if comments else "..."
            dtype = types.get(col, None)

            if dtype:
                COLUMNS.append(f'''{col} {dtype} COMMENT "{comment}"''')
            else:
                infer_dtype = check_dtype_postgre(df[col].dtypes)
                COLUMNS.append(f'''{col} {infer_dtype} COMMENT "{comment}"''')

        PRIMARY_SEG = f' ,PRIMARY KEY (id)'
        if isinstance(primary_key, str) and (not primary_key == 'id'):
            PRIMARY_SEG = f' ,PRIMARY KEY (id, {primary_key})'
        elif isinstance(primary_key, (list, tuple, set)):
            PRIMARY_SEG = f' ,PRIMARY KEY (id, {",".join(primary_key)})'
        else:
            pass

        CREATE_TABLE = PREFIX + ','.join(COLUMNS) + PRIMARY_SEG + SUFFIX

        self.execute(CREATE_TABLE)
        Console().print(
            f"Table [blod cyan]{table}[/blod cyan] was created ✨ 🍰 ✨")

    def rename_table(self, table: str, name: str):
        """Rename table

        :param table:
        :param name:
        :return:
        """
        RENAME_TABLE = """ALTER TABLE {} RENAME TO {} ;""".format(table, name)
        self.execute(RENAME_TABLE)
        Console().print(
            "Renamed table [bold red]{}[/bold red] to [bold cyan]{}[/bold "
            "cyan] ✨ 🍰 ✨".format(table, name))

    def rename_column(self, table: str, column: str, name: str, dtype: str):
        """Rename column

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ALTER  TABLE table RENAME [column] TO [new column];

        :param table:
        :param column:
        :param name:
        :param dtype:
        :return:
        """
        RENAME_COLUMN = """ALTER  TABLE {} RENAME {} TO {};""".format(
            table, column, name)
        self.execute(RENAME_COLUMN)
        Console().print("Renamed column {} to {} ✨ 🍰 ✨".format(column, name))

    def add_column(
        self,
        table: str,
        column: str,
        dtype: str,
        comment: str = "...",
        after: str = None,
    ):
        """Add new column
    comment on column test2.id is '...';

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
            Console().print("%(column)s was SQL keyword or reserved word 😯\n" %
                            {"column": column},
                            style='red')
            sys.exit(1)

        if after:
            ADD_COLUMN = (
                """ALTER TABLE {} ADD {} {} COMMENT '{}' AFTER {} ;""".format(
                    table, column, dtype, comment, after))
        else:
            ADD_COLUMN = """ALTER TABLE {} ADD {} {} COMMENT '{}' ;""".format(
                table, column, dtype, comment)

        self.execute(ADD_COLUMN)
        Console().print(f"Added column {column} to {table} ✨ 🍰 ✨")

    def add_table_comment(self, table: str, comment: str):
        """Add comment for table"""
        ADD_TABLE_COMMENT = """COMMENT ON TABLE {} IS '{}' ;""".format(
            table, comment)
        self.execute(ADD_TABLE_COMMENT)
        Console().print("Table comment added ✨ 🍰 ✨")

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
        Console().print(
            "Column [bold cyan]{}[/bold cyan]'s attribute was modified "
            "✨ 🍰 ✨".format(column))

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
            PRIMARY_KEY = f'{primary_key}'
        elif isinstance(primary_key, (list, tuple)):
            PRIMARY_KEY = f'{",".join(primary_key)}'

        ADD_PRIMARY_KEY = f"""ALTER TABLE {table} ADD PRIMARY KEY ({PRIMARY_KEY});"""
        self.execute(ADD_PRIMARY_KEY)
        Console().print("Well done ✨ 🍰 ✨")
