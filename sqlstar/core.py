# *_*coding:utf-8 *_*
import logging
import sys
import typing
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit
import pandas as pd

from sqlstar.importer import import_from_string
from sqlstar.interfaces import DatabaseBackend

if sys.version_info >= (3, 7):
    import contextvars as contextvars
else:
    import aiocontextvars as contextvars

try:
    import click

    # Extra log info for optional coloured terminal outputs.
    LOG_EXTRA = {
        "color_message": "Query: " + click.style("%s", bold=True) + " Args: %s"
    }
    CONNECT_EXTRA = {
        "color_message":
        "Connected to database " + click.style("%s", bold=True)
    }
    DISCONNECT_EXTRA = {
        "color_message":
        "Disconnected from database " + click.style("%s", bold=True)
    }
except ImportError:
    LOG_EXTRA = {}
    CONNECT_EXTRA = {}
    DISCONNECT_EXTRA = {}

logger = logging.getLogger("sqlstar")


class Database:
    SUPPORTED_BACKENDS = {
        "mysql": "sqlstar.backends.mysql:MySQLBackend",
        "postgre": "sqlstar.backends.postgre:PostgreBackend",
    }

    def __init__(
        self,
        url: typing.Union[str, "DatabaseURL"],
        **options: typing.Any,
    ):
        self.url = DatabaseURL(url)
        self.options = options
        self.is_connected = False

        backend_str = self.SUPPORTED_BACKENDS[self.url.scheme]
        backend_cls = import_from_string(backend_str)
        assert issubclass(backend_cls, DatabaseBackend)
        self._backend = backend_cls(self.url, **self.options)

        # Connections are stored as task-local state.
        self._connection_context = contextvars.ContextVar(
            "connection_context")  # type: contextvars.ContextVar

        self._global_connection = None  # type: typing.Optional[Connection]

    def connect(self) -> None:
        """
        Establish the connection pool.
        """
        if self.is_connected:
            logger.debug("Already connected, skipping connection")
            return None

        self._backend.connect()
        logger.info("Connected to database %s",
                    self.url.obscure_password,
                    extra=CONNECT_EXTRA)
        self.is_connected = True

    def disconnect(self) -> None:
        """
        Close all connections in the connection pool.
        """
        if not self.is_connected:
            logger.debug("Already disconnected, skipping disconnection")
            return None

        self._connection_context = contextvars.ContextVar("connection_context")

        self._backend.disconnect()
        logger.info(
            "Disconnected from database %s",
            self.url.obscure_password,
            extra=DISCONNECT_EXTRA,
        )
        self.is_connected = False

    def fetch_all(self, query: typing.Union[str]):
        """Fetch all the rows"""
        return self.connection().fetch_all(query)

    def fetch_many(self, query: typing.Union[str], size: int = None):
        """Fetch several rows"""
        return self.connection().fetch_many(query, size)

    def execute(self, query: typing.Union[str]):
        """Execute a query

                :param str query: Query to execute.

                :return: Number of affected rows
                :rtype: int
        """
        return self.connection().execute(query)

    def execute_many(self, query: typing.Union[str]):
        """Run several data against one query

                :param query: query to execute on server
                :return: Number of rows affected, if any.
                :rtype: int

                This method improves performance on multiple-row INSERT and
                REPLACE. Otherwise it is equivalent to looping over args with
                execute().
        """
        return self.connection().execute_many(query)

    def truncate_table(self, table: typing.Union[str]):
        """Truncate table's data, but keep the table structure

                :param table:
                :return:
        """
        return self.connection().truncate_table(table)

    def drop_table(self, table: typing.Union[str]):
        """Drop table"""
        return self.connection().drop_table(table)

    def update(self, table, where: dict, target: dict):
        """Update table's data"""
        return self.connection().update(table, where, target)

    def drop_column(self, table, column: typing.Union[str, list, tuple]):
        """Drop column"""
        return self.connection().drop_column(table, column)

    def fetch_df(self, query: typing.Union[str]):
        """Fetch data, and format result into Dataframe

        :param query:
        :return: Dataframe
        """
        return self.connection().fetch_df(query)

    def export_csv(self,
                   query: typing.Union[str],
                   fname: typing.Union[str],
                   sep: typing.Any = ','):
        """Export result to csv"""
        return self.connection().export_csv(query, fname, sep)

    def export_excel(self, query: typing.Union[str], fname: typing.Union[str]):
        """Export result to excel"""
        return self.connection().export_excel(query, fname)

    def create_table(self,
                     table,
                     df: pd.DataFrame = None,
                     comments: dict = None,
                     primary_key: typing.Union[str, list, tuple] = None,
                     dtypes: dict = None):
        """Create table"""
        return self.connection().create_table(table, df, comments, primary_key,
                                              dtypes)

    def insert_many(self, table, data: typing.Union[list, tuple],
                    cols: typing.Union[list, tuple]):
        """Insert many records

        :param table: table name
        :param data: data
        :param cols: columns
        :return:
        """
        return self.connection().insert_many(table, data, cols)

    def insert_df(self, table, df: pd.DataFrame, dropna=True, **kwargs):
        """Insert Dataframe type of data

        # transform dtype
        >>> df.loc[:, col] = df.loc[:, col].astype(str)

        :param table:
        :param df: Dataframe

        :return:
        """
        return self.connection().insert_df(table, df, dropna, **kwargs)

    def rename_table(self, table: str, name: str):
        """Rename table

        :param table:
        :param name:
        :return:
        """
        return self.connection().rename_table(table, name)

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
        return self.connection().rename_column(table, column, name, dtype)

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
        return self.connection().add_column(table, column, dtype, comment,
                                            after)

    def add_table_comment(self, table: str, comment: str):
        """Add comment for table"""
        return self.connection().add_table_comment(table, comment)

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
        return self.connection().change_column_attribute(
            table, column, dtype, notnull, comment)

    def add_primary_key(self, table: str, primary_key: typing.Union[str, list,
                                                                    tuple]):
        """Set primary key

        :param table:
        :param primary_key:
        :return:
        """
        return self.connection().add_primary_key(table, primary_key)

    def _new_connection(self) -> "Connection":
        connection = Connection(self._backend)
        self._connection_context.set(connection)
        return connection

    def connection(self) -> "Connection":
        if self._global_connection is not None:
            return self._global_connection

        try:
            return self._connection_context.get()
        except LookupError:
            return self._new_connection()


class Connection:
    def __init__(self, backend: DatabaseBackend):
        self._backend = backend
        self._connection = self._backend.connection()

    def fetch_all(self, query: typing.Union[str]):
        return self._connection.fetch_all(query)

    def fetch_many(self, query: typing.Union[str], size: int = None):
        return self._connection.fetch_many(query, size)

    def execute(self, query: typing.Union[str]):
        return self._connection.execute(query)

    def execute_many(self, query: typing.Union[str]):
        self._connection.execute_many(query)

    def truncate_table(self, table: typing.Union[str]):
        return self._connection.truncate_table(table)

    def fetch_df(self, query: typing.Union[str]):
        return self._connection.fetch_df(query)

    def export_csv(self, query: typing.Union[str], fname: typing.Union[str],
                   sep: typing.Any):
        return self._connection.export_csv(query, fname, sep)

    def export_excel(self, query: typing.Union[str], fname: typing.Union[str]):
        return self._connection.export_excel(query, fname)

    def insert_many(self, table, data: typing.Union[list, tuple],
                    cols: typing.Union[list, tuple]):
        return self._connection.insert_many(table, data, cols)

    def insert_df(self, table, df: pd.DataFrame, dropna=True, **kwargs):
        return self._connection.insert_df(table, df, dropna, **kwargs)

    def rename_table(self, table: str, name: str):
        return self._connection.rename_table(table, name)

    def rename_column(self, table: str, column: str, name: str, dtype: str):
        return self._connection.rename_column(table, column, name, dtype)

    def add_column(
        self,
        table: str,
        column: str,
        dtype: str,
        comment: str = "...",
        after: str = None,
    ):
        return self._connection.add_column(table, column, dtype, comment,
                                           after)

    def add_table_comment(self, table: str, comment: str):
        return self._connection.add_table_comment(table, comment)

    def change_column_attribute(
        self,
        table: str,
        column: str,
        dtype: str,
        notnull: bool = False,
        comment: str = None,
    ):
        return self._connection.change_column_attribute(
            table, column, dtype, notnull, comment)

    def drop_table(self, table: typing.Union[str]):
        return self._connection.drop_table(table)

    def update(self, table, where: dict, target: dict):
        """Update table's data"""
        return self._connection.update(table, where, target)

    def drop_column(self, table, column: typing.Union[str, list, tuple]):
        return self._connection.drop_column(table, column)

    def create_table(self,
                     table,
                     df: pd.DataFrame = None,
                     comments: dict = None,
                     primary_key: typing.Union[str, list, tuple] = None,
                     dtypes: dict = None):
        return self._connection.create_table(table, df, comments, primary_key,
                                             dtypes)

    def add_primary_key(self, table: str, primary_key: typing.Union[str, list,
                                                                    tuple]):
        return self._connection.add_primary_key(table, primary_key)


class _EmptyNetloc(str):
    def __bool__(self) -> bool:
        return True


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL"]):
        if isinstance(url, DatabaseURL):
            self._url: str = url._url
        elif isinstance(url, str):
            self._url = url
        else:
            raise TypeError(
                f"Invalid type for DatabaseURL. Expected str or DatabaseURL, got {type(url)}"
            )

    @property
    def components(self) -> SplitResult:
        if not hasattr(self, "_components"):
            # don't need parse '#', replace by 'æ◊' first, then we replace back
            self._components = urlsplit(self._url.replace('#', 'æ◊'))
        return self._components

    @property
    def scheme(self) -> str:
        return self.components.scheme

    @property
    def dialect(self) -> str:
        return self.components.scheme.split("+")[0]

    @property
    def driver(self) -> str:
        if "+" not in self.components.scheme:
            return ""
        return self.components.scheme.split("+", 1)[1]

    @property
    def userinfo(self) -> typing.Optional[bytes]:
        if self.components.username:
            info = self.components.username
            if self.components.password:
                info += ":" + self.components.password
            return info.encode("utf-8")
        return None

    @property
    def username(self) -> typing.Optional[str]:
        if self.components.username is None:
            return None
        return unquote(self.components.username)

    @property
    def password(self) -> typing.Optional[str]:
        if self.components.password is None:
            return None
        return unquote(self.components.password.replace('æ◊', '#'))

    @property
    def hostname(self) -> typing.Optional[str]:
        return self.components.hostname

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return unquote(path)

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if ("username" in kwargs or "password" in kwargs
                or "hostname" in kwargs or "port" in kwargs):
            hostname = kwargs.pop("hostname", self.hostname)
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", self.components.username)
            password = kwargs.pop("password", self.components.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if not kwargs.get("netloc", self.netloc):
            # Using an empty string that evaluates as True means we end up
            # with URLs like `sqlite:///database` instead of `sqlite:/database`
            kwargs["netloc"] = _EmptyNetloc()

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password="********")._url
        return self._url

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: typing.Any) -> bool:
        return str(self) == str(other)
