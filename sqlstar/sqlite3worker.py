# Copyright (c) 2014 Palantir Technologies
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
"""Thread safe sqlite3 interface."""

__author__ = "Shawn Lee"
__email__ = "shawnl@palantir.com"
__license__ = "MIT"
__github__ = "https://github.com/palantir/sqlite3worker"

import traceback

try:
    import queue as Queue  # module re-named in Python 3
except ImportError:
    import Queue
import sqlite3
import threading
import time
import uuid
from rich.console import Console


class Sqlite3worker(threading.Thread):
    """Sqlite thread safe object.

    Example:
        from sqlite3worker import Sqlite3Worker
        sql_worker = Sqlite3Worker("/tmp/test.sqlite")
        sql_worker.execute(
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")
        sql_worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        sql_worker.execute(
            "INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
        sql_worker.execute("SELECT * from tester")
        sql_worker.close()
    """
    def __init__(self, file_name, max_queue_size=100):
        """Automatically starts the thread.

        Args:
            file_name: The name of the file.
            max_queue_size: The max queries that will be queued.
        """
        threading.Thread.__init__(self)
        self.daemon = True
        self.sqlite3_conn = sqlite3.connect(
            file_name,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES)
        self.sqlite3_cursor = self.sqlite3_conn.cursor()
        self.sql_queue = Queue.Queue(maxsize=max_queue_size)
        self.results = {}
        self.max_queue_size = max_queue_size
        self.exit_set = False
        # Token that is put into queue when close() is called.
        self.exit_token = str(uuid.uuid4())
        self.start()
        self.thread_running = True

    def run(self):
        """Thread loop.

        This is an infinite loop.  The iter method calls self.sql_queue.get()
        which blocks if there are not values in the queue.  As soon as values
        are placed into the queue the process will continue.

        If many executes happen at once it will churn through them all before
        calling commit() to speed things up by reducing the number of times
        commit is called.
        """
        execute_count = 0
        for token, query, values in iter(self.sql_queue.get, None):
            if token != self.exit_token:
                self.run_query(token, query, values)
                execute_count += 1
                # Let the executes build up a little before committing to disk
                # to speed things up.
                if (self.sql_queue.empty()
                        or execute_count == self.max_queue_size):
                    self.sqlite3_conn.commit()
                    execute_count = 0
            # Only exit if the queue is empty. Otherwise keep getting
            # through the queue until it's empty.
            if self.exit_set and self.sql_queue.empty():
                self.sqlite3_conn.commit()
                self.sqlite3_conn.close()
                self.thread_running = False
                return

    def run_query(self, token, query, values):
        """Run a query.

        Args:
            token: A uuid object of the query you want returned.
            query: A sql query with ? placeholders for values.
            values: A tuple of values to replace "?" in query.
        """
        if query.lower().strip().startswith("select"):
            try:
                self.sqlite3_cursor.execute(query, values)
                self.results[token] = self.sqlite3_cursor.fetchall()
            except sqlite3.Error as err:
                # Put the error into the output queue since a response
                # is required.
                self.results[token] = ("Query returned error: %s: %s: %s" %
                                       (query, values, err))
                traceback.print_exc()
        else:
            try:
                self.sqlite3_cursor.execute(query, values)
            except sqlite3.Error as err:
                traceback.print_exc()

    def close(self):
        """Close down the thread and close the sqlite3 database file."""
        self.exit_set = True
        self.sql_queue.put((self.exit_token, "", ""), timeout=5)
        # Sleep and check that the thread is done before returning.
        while self.thread_running:
            time.sleep(.01)  # Don't kill the CPU waiting.

    @property
    def queue_size(self):
        """Return the queue size."""
        return self.sql_queue.qsize()

    def query_results(self, token):
        """Get the query results for a specific token.

        Args:
            token: A uuid object of the query you want returned.

        Returns:
            Return the results of the query when it's executed by the thread.
        """
        delay = .001
        while True:
            if token in self.results:
                return_val = self.results[token]
                del self.results[token]
                return return_val
            # Double back on the delay to a max of 8 seconds.  This prevents
            # a long lived select statement from trashing the CPU with this
            # infinite loop as it's waiting for the query results.
            time.sleep(delay)
            if delay < 8:
                delay += delay

    def execute(self, query, values=None):
        """Execute a query.

        Args:
            query: The sql string using ? for placeholders of dynamic values.
            values: A tuple of values to be replaced into the ? of the query.

        Returns:
            If it's a select query it will return the results of the query.
        """
        if self.exit_set:
            return "Exit Called"
        values = values or []
        # A token to track this query with.
        token = str(uuid.uuid4())
        # If it's a select we queue it up with a token to mark the results
        # into the output queue so we know what results are ours.
        if query.lower().strip().startswith("select"):
            self.sql_queue.put((token, query, values), timeout=5)
            return self.query_results(token)
        else:
            self.sql_queue.put((token, query, values), timeout=5)


class path(Sqlite3worker):
    def __init__(self, file_name):
        super(path, self).__init__(file_name)

    def select(self, command: str):
        """select data

        :param command:
        :return:
        """
        try:
            fetchdata = self.execute(command)

            return fetchdata
        except Exception as why:
            traceback.print_exc()
            Console().print(f'Ops💥 💔 💥 \n{why}', style='red')

    def select_as_df(
        self,
        command: str,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
    ):
        """

        :param command:
        :param index_col: index column
        :param coerce_float:非常有用，将数字形式的字符串直接以float型读入
        :param params:
        :param parse_dates:将某一列日期型字符串转换为datetime型数据，与pd.to_datetime函数功能类似。
                                            可以直接提供需要转换的列名以默认的日期形式转换，也可以用字典的格式提供
                                            列名和转换的日期格式，比如{column_name: format string}
                                            （format string："%Y:%m:%H:%M:%S"）
        :param columns:要选取的列
        :return:dataframe类型的数据，所有列名
        """
        # return pd.read_sql(command, conn)
        """
        解决错误信息：ValueError: unsupported format character 'Y' (0x59) at index 146

       产生原因：因为python执行的sql中存在类似DATE_FORMAT(CREATE_TIME, ‘%Y-%m-%d’) 的写法,
    其中%Y与python的参数%s冲突
        """
        import pandas as pd

        try:
            df = pd.read_sql(
                command
                # , engine
                ,
                self.sqlite3_conn,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
            )
        except Exception as why:
            command_parse = command.replace(
                "%", "%%") if "%" in command else command
            df = pd.read_sql(
                command_parse
                # , engine
                ,
                self.sqlite3_conn,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
            )

        cols = df.columns.tolist()
        data_count = df.shape
        Console().print(f'The data shape is [bold cyan]{data_count}[/bold '
                        f'cyan]')
        if data_count[0] == 0:
            Console().print(command, style='green')
        return df, cols

    def add_column(self, table: str, column: str, newtype: str):
        """
        ALTER TABLE 表名 ADD [COLUMN] 字段名 字段类型 是否可为空 COMMENT '注释' AFTER 指定某字段 ;
        --COLUMN关键字可以省略不写
        :param table:
        :param column:
        :param newtype:
        :return:
        """
        ADD_COLUMN_SQL = """ALTER TABLE {} ADD COLUMN {} {} ;""".format(
            table, column, newtype)

        try:
            self.execute(ADD_COLUMN_SQL)
            Console().print(f'The column {column} was added to {table}',
                            style='white')
        except Exception as why:
            traceback.print_exc()

    def insert_one(self, table, data):
        """insert one piece of data every time"""
        SQL_INSERT_ONE_DATA = """
        INSERT INTO {} VALUES {};
        """.format(table, data)
        try:
            self.execute(SQL_INSERT_ONE_DATA)
        except Exception as why:
            traceback.print_exc()

    def insert_many(self, table, data: list, cols: list = []):
        if cols:
            SQL_INSERT_MANY_DATA = """
                                                INSERT OR IGNORE INTO `{}` ({}) VALUES ({});
                                                """.format(
                table,
                ",".join(["`%s`" % col for col in cols]),
                ",".join(["?" for i in range(len(cols))]),
            )
        else:
            SQL_INSERT_MANY_DATA = """
                                                INSERT OR IGNORE INTO `{}` VALUES ({});
                                                """.format(
                table,
                ",".join(["?" for i in range(len(data[0]))]),
            )

        try:
            self.sqlite3_conn.executemany(SQL_INSERT_MANY_DATA, data)

            Console().print(
                f"There are {len(data)} pieces of data has been inserted into"
                f" {table}✨ 🍰 ✨")
        except Exception as why:
            traceback.print_exc()

    def insert_df(self, df, table, cols):

        # df.dropna(axis=0, how='any', inplace=True)
        # process dataframe types
        for column in df.columns:
            column_type = df[column].dtypes
            handle_type = ["datetime64[ns]"]
            if column_type in handle_type:
                df[column] = df[column].astype(str)

        insertdata = []
        for index, row in df.iterrows():
            insertdata.append(tuple(row))

        self.insert_many(table=table, data=insertdata, cols=cols)

    def drop_duplicate_data(self, table: str, col):
        """delete duplicate data from table

        :param table: table name
        :param col: column that reprecent unique identifier
        :return:
        """
        DROP_DUPLICATE_SQL = """DELETE FROM {} 
                            WHERE rowid NOT IN (SELECT MAX(rowid) 
                            FROM {} GROUP BY {})""".format(table, table, col)

        try:
            self.execute(DROP_DUPLICATE_SQL)
            Console().print(f"Well done ✨ 🍰 ✨")
        except Exception as why:
            traceback.print_exc()

    def alter_table_name(self, oldname, newname):
        """
        重命名表
        :param oldname:
        :param newname:
        :return:
        """
        RENAME_SQL = """ALTER TABLE {} RENAME TO {};""".format(
            oldname, newname)
        try:
            self.execute(RENAME_SQL)
            Console().print(f'{oldname} has been rename to {newname}✨ 🍰 ✨ ')
        except Exception as why:
            traceback.print_exc()

    def truncate_db_table(self, table):
        """truncate table's data, but keep the table structure

        :return:
        """
        SQL_TRUNCATE_TABLE = """
        DELETE FROM {} where 1=1;
        """.format(table)
        try:
            self.execute(SQL_TRUNCATE_TABLE)
            Console().print(f'The table {table} was been truncated ✨ 🍰 ✨')
        except Exception as why:
            traceback.print_exc()

    def drop_db_table(self, table):
        """warning: delete the table data and structure

        :return:
        """
        SQL_DROP_TABLE = """
        DROP TABLE IF EXISTS {};
        """.format(table)
        try:
            self.execute(SQL_DROP_TABLE)
            Console().print(f"Table {table} was been deleted ✨ 🍰 ✨")
        except Exception as why:
            traceback.print_exc()
