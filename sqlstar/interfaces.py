# *_*coding:utf-8 *_*
import typing
import pandas as pd


class DatabaseBackend:
    def connect(self) -> None:
        raise NotImplementedError()

    def disconnect(self) -> None:
        raise NotImplementedError()

    def connection(self) -> "ConnectionBackend":
        raise NotImplementedError()


class ConnectionBackend:
    def fetch_all(self, query: typing.Union[str]):
        raise NotImplementedError()

    def fetch_many(self, query: typing.Union[str], size: int):
        raise NotImplementedError()

    def execute(self, query: typing.Union[str]):
        raise NotImplementedError()

    def execute_many(self, queries: typing.List[typing.Union[str, dict]]):
        raise NotImplementedError()

    def truncate_table(self, table: typing.Union[str]):
        raise NotImplementedError()

    def update(self, table, where: dict, target: dict):
        raise NotImplementedError()

    def fetch_df(self, table: typing.Union[str]):
        raise NotImplementedError()

    def export_csv(self, query: typing.Union[str], fname: typing.Union[str],
                   sep: typing.Any):
        raise NotImplementedError()

    def export_excel(self, query: typing.Union[str], fname: typing.Union[str]):
        raise NotImplementedError()

    def drop_table(self, table):
        raise NotImplementedError()

    def drop_column(self, table, column: typing.Union[str, list, tuple]):
        raise NotImplementedError()

    def create_table(self,
                     table,
                     df: pd.DataFrame = None,
                     comments: dict = None,
                     primary_key: typing.Union[str, list, tuple] = None,
                     dtypes: dict = None):
        raise NotImplementedError()

    def insert_many(self, table, data: typing.Union[list, tuple],
                    cols: typing.Union[list, tuple]):
        raise NotImplementedError()

    def insert_df(self, table, df: pd.DataFrame, dropna=True, **kwargs):
        raise NotImplementedError()

    def rename_table(self, table: str, name: str):
        raise NotImplementedError()

    def rename_column(self, table: str, column: str, name: str, dtype: str):
        raise NotImplementedError()

    def add_column(
        self,
        table: str,
        column: str,
        dtype: str,
        comment: str = "...",
        after: str = None,
    ):
        raise NotImplementedError()

    def add_table_comment(self, table: str, comment: str):
        raise NotImplementedError()

    def change_column_attribute(
        self,
        table: str,
        column: str,
        dtype: str,
        notnull: bool = False,
        comment: str = None,
    ):
        raise NotImplementedError()

    def add_primary_key(self, table: str, primary_key: typing.Union[str, list,
                                                                    tuple]):
        raise NotImplementedError()
