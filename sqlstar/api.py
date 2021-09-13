# *_*coding:utf-8 *_*
"""
Author: szj
"""
import os
import traceback
from rich.console import Console
import pymysql, sys
import pandas as pd
import numpy as np
from toolz import merge

from sqlstar.utils import deprecated, check_dtype


class mysql(object):
    def __init__(self, **kwargs):
        # extract  parameters and convert names to lowercase
        params = {}
        for i in kwargs:
            params[i.lower()] = kwargs[i]

        if params:
            self.host = params.get("host", None)
            self.port = int(params.get("port", None))
            self.username = params.get("user", None)
            self.password = params.get("passwd", None)
            self.db = params.get("db", None)
            self.charset = params.get("charset", None)

        assert self.host, "host is required"
        assert self.port, "port is required"
        assert self.username, "username is required"
        assert self.password, "password is required"
        assert self.db, "db is required"

        # Connect to the database during initialization
        self.connection, self.cursor = self.initializeDB()

    def initializeDB(self):
        """Initialize mysql"""
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                passwd=self.password,
                db=self.db,
                # charset=self.charset,
            )
            cursor = connection.cursor()
            return connection, cursor
        except:
            raise Exception(
                "\nPlease checkout your database settings 💥 💔 💥\nHOST:{}\nPORT:{}\nUSER:{}\nPASSWD:{}\nDB:{}"
                .format(self.host, self.port, self.username, self.password,
                        self.db))

    def get_connect(self):
        """Get connection and cursor"""
        if not self.connection.open:
            self.connection, self.cursor = self.initializeDB()
        return self.connection, self.cursor

    def execute(self, command: str):
        """Execute sql command
        """
        connection, cursor = self.get_connect()
        try:
            cursor.execute(command)
            connection.commit()
            return True
        except:
            connection.rollback()
            # traceback.print_exc()
            raise Exception(
                f"Ops, fail to execute this command 💥 💔 💥\n{command}")

    def select(self, *, command: str):
        """Select data

        :param command:
        :return:fetchdata，nlines = data，line number
        """

        connection, cursor = self.get_connect()
        try:
            nlines = cursor.execute(command)
            fetchdata = cursor.fetchall()
            return fetchdata, nlines
        except Exception as e:
            raise e

    def select_count(self, table):
        """Get the table's line number """
        COUNT_SQL = """SELECT COUNT(*) FROM {}""".format(table)
        return self.select(command=COUNT_SQL)[0][0][0]  # (((110,),), 1)

    def select_as_df(
        self,
        command,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
    ):
        """Select data，and format result into dataframe

        :param command:
        :param index_col: the index column
        :param coerce_float: reading numeric strings directly as float
        :param params:
        :param parse_dates: parse string into datetime
        :param columns:
        :return: dataframe，columns
        """

        connection, cursor = self.get_connect()
        try:
            # https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html?highlight=read_sql#pandas.read_sql
            df = pd.read_sql(
                command,
                # , self.engine
                connection,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
            )
        except Exception:
            """
Fix ValueError: unsupported format character 'Y' (0x59) at index 146

Reason:
    When we insert time format like DATE_FORMAT(CREATE_TIME, '%Y-%m-%d'), 
    which %xxx was conflicts with the Python argument %s
            """
            command_parse = command.replace(
                "%", "%%") if "%" in command else command
            df = pd.read_sql(
                command_parse,
                # , self.engine
                connection,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
            )
        cols = df.columns.tolist()
        data_count = df.shape

        if data_count[0] == 0:
            return df, cols
        return df, cols

    def truncate_table(self, table):
        """Truncate table's data, but keep the table structure

        :param table:
        :return:
        """
        TRUNCATE_TABLE = """TRUNCATE TABLE {};""".format(table)
        if self.execute(command=TRUNCATE_TABLE):
            Console().print(
                f"Table [bold cyan]{table}[/bold cyan] was truncated ✨ 🍰 ✨")

    def insert_one(self, table, data, cols, ignore=True, echo=False):
        """just insert data one piece at a time

        :param table:
        :param data:
        :param cols:
        :param ignore: whether or no ignore duplicate data when repeat
        :return:
        """

        if ignore:
            SQL_INSERT_ONE_DATA = """
                    INSERT IGNORE INTO {}  ({})  VALUES {};
                    """.format(table, ",".join(["`%s`" % col for col in cols]),
                               data)
        else:
            SQL_INSERT_ONE_DATA = """
                    INSERT INTO {}  ({})  VALUES {};
                    """.format(table, ",".join(["`%s`" % col for col in cols]),
                               data)
        if self.execute(command=SQL_INSERT_ONE_DATA) and echo:
            Console().print("Well done ✨ 🍰 ✨")

    def insert_many(self, table, data: list, cols: list = [], ignore=True):
        """insert multiple pieces of data at once

        :param table: table name
        :param data: data
        :param cols: columns
        :param ignore: ignore duplicated data or no
        :return:
        """

        # 将list格式数据转成(),()...,()这种正确的sql插入语法格式
        insert_many_data = ",".join(str(i) for i in data)

        if ignore:
            SQL_INSERT_MANY_DATA = """
                    INSERT IGNORE INTO {}  ({})  VALUES {};
                    """.format(table, ",".join(["`%s`" % col for col in cols]),
                               insert_many_data)
        else:
            SQL_INSERT_MANY_DATA = """
                    INSERT INTO {}  ({})  VALUES {};
                    """.format(table, ",".join(["`%s`" % col for col in cols]),
                               insert_many_data)
        if self.execute(command=SQL_INSERT_MANY_DATA):
            Console().print(
                f"Table [cyan]{table}[/cyan] inserts [green]{len(data)}["
                f"/green] records✨ 🍰 ✨")

    @deprecated
    def insert_many_old(self, table, data, cols):
        """insert many data（保留此方法仅供参考）

        此方法容易引发如下错误:
                        TypeError: not all arguments converted during string formatting
        原因见:
                        https://blog.csdn.net/weixin_40580582/article/details/101032556
                        https://www.codeleading.com/article/50852193159/

        新增多条数据:
        :param table: 要插入的表名
        :param data: 要插入的数据
        :param cols: 一个列名的list
        :return:
        """

        SQL_INSERT_MANY_DATA = """
                                            INSERT IGNORE INTO `{}` ({}) VALUES ({});
                                            """.format(
            table,
            ",".join(["`%s`" % col for col in cols]),
            ",".join(["?" for i in range(len(cols))]),
        )

        connection, cursor = self.get_connect()
        try:
            cursor.executemany(SQL_INSERT_MANY_DATA, data)
            connection.commit()
            Console().print(
                "Table [bold cyan]{}[/bold cyan] successfully inserted data ✨ 🍰 ✨"
                .format(table))
        except Exception as why:
            connection.rollback()
            Console().print(
                "[bold red]Ops, failed to insert data[/bold red]💥 💔 "
                "💥\nReason:\n{}".format(why))

    def insert_df(
        self,
        table,
        df,
        cols: list,
        fillna=True,
        what=np.nan,
        dropna=False,
        axis=0,
        how="any",
        inplace=True,
    ):
        """Insert dataframe into table

        :param table:
        :param df: dataframe
        :param cols: columns
        :param fillna: fill NA or not
        :param what: if fillna is True, use what to fill the NA
        :param dropna: whether or not to drop NA
        :param axis:
        :param how:
        :param inplace: replace original data or not
        :return:
        """
        if df.empty:
            Console().print('No input data 😅', style='red')
        else:
            if dropna:
                df.dropna(axis=axis, how=how, inplace=inplace)
                # when we droped the nan, then don't need to fill nan
                fillna = False

            # process dataframe column' type
            for column in df.columns:
                column_type = df[column].dtypes
                handle_type = ["datetime64[ns]", "object"]
                if column_type in handle_type:
                    df[column] = df[column].astype(str)

            df_values = df[cols].fillna(
                what).values if fillna else df[cols].values
            insertdata = [tuple(row) for row in df_values]

            self.insert_many(table=table, data=insertdata, cols=cols)

    def show_create_table(self, table, echo=True):
        """Show create table command

        :param table:
        :return:
        """

        SQL = """SHOW CREATE TABLE {};""".format(table)
        result = self.select(command=SQL)[0][0][1]
        if echo:
            Console().print(result, style='green')
        return result

    def create_tmp_table(self, table: str = "tmp_table"):
        """create an tmp table"""
        CREATE_TMP_TABLE = """
    CREATE TABLE IF NOT EXISTS `{}` (
      `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
      `name` varchar(50) NOT NULL COMMENT 'name',
      `position` varchar(50) NOT NULL COMMENT 'job',
      `erp` varchar(50) NOT NULL COMMENT 'erp  account',
      `created_time` datetime DEFAULT NULL COMMENT 'create time',
      `updated_time` datetime DEFAULT NULL COMMENT 'update time',
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COMMENT='white list table' 
    """.format(table)
        if self.execute(command=CREATE_TMP_TABLE):
            Console().print(f"Table [bold cyan]{table}[/bold cyan] was "
                            f"created  ✨ 🍰 ✨")

    def drop_table(self, table: str = None, sure=False):

        DROP_TABLE = f"""DROP TABLE IF EXISTS `{table}`;"""
        df, cols = self.select_as_df(f'''select * from {table} limit 10''')

        ifnot = True
        # If the table is not empty or requires validation, warning user
        if sure or not df.empty:
            Console().print(
                f"Are you sure to delete the table [bold red]{table}[/bold red] ?"
            )
            ifnot = input(" (Y/n):")

        if ifnot in ['y', 'Y', 'yes'] or True:
            if self.execute(command=DROP_TABLE):
                Console().print(
                    f"Table [bold cyan]{table}[/bold cyan] was deleted ✨ 🍰 ✨")
        else:
            Console().print("Think again...", style='green')

    def drop_column(self, table: str, column: str):
        """Delete column"""

        DROP_COLUMN = """ALTER TABLE {} DROP COLUMN {} ;""".format(
            table, column)
        if self.execute(command=DROP_COLUMN):
            Console().print(f"The column [bold cyan]{column}[/bold cyan] of"
                            f" {table} was deleted✨ 🍰 ✨")

    def add_column(
        self,
        table: str,
        column: str,
        newtype: str,
        comment: str = None,
        after: str = None,
    ):
        """ALTER TABLE 表名 ADD [COLUMN] 字段名 字段类型 是否可为空 COMMENT '注释' AFTER 指定某字段 ;--COLUMN关键字可以省略不写

        :param table:
        :param column:
        :param newtype:
        :param comment:
        :param after:
        :return:
        """
        MYSQL_KEYWORDS = ["CHANGE", "SCHEMA", "DEFAULT"]
        if column.upper() in MYSQL_KEYWORDS:
            Console().print("%(column)s 是SQL关键字或保留字，请使用其他字段名💥 💔 💥\n" %
                            {"column": column},
                            style='red')
            sys.exit(1)

        if after:
            ADD_COLUMN = (
                """ALTER TABLE {} ADD {} {} COMMENT '{}' AFTER {} ;""".format(
                    table, column, newtype, comment, after))
        else:
            ADD_COLUMN = """ALTER TABLE {} ADD {} {} COMMENT '{}' ;""".format(
                table, column, newtype, comment)

        if self.execute(command=ADD_COLUMN):
            Console().print(f"Added column {column} to {table}✨ 🍰 ✨")

    def add_primary_key(self, table: str, keys: list):
        """Set primary key

        :param table:
        :param column:
        :return:
        """

        # try:
        #     # 如果原先有主键，要先删除原先的主键
        #     DROP_PRIMARIY_KEY = f'ALTER TABLE {table} DROP PRIMARY KEY;'
        #     cls.execute(command=DROP_PRIMARIY_KEY)
        # except:
        #     pass

        ADD_PRIMARY_KEY = """ALTER TABLE {} ADD PRIMARY KEY ({});""".format(
            table, ", ".join(keys))
        if self.execute(command=ADD_PRIMARY_KEY):
            Console().print(
                f"Added column {','.join(keys)} to primary key✨ 🍰 ✨")

    def alter_table_comment(self, table: str, comment: str):
        """Alter table's comment"""

        ALTER_TABLE_COMMENT = """ALTER TABLE {} COMMENT '{}' ;""".format(
            table, comment)
        if self.execute(command=ALTER_TABLE_COMMENT):
            Console().print("Table comment added successfully ✨ 🍰 ✨")

    def alter_table_name(self, table: str, newname: str):
        """Alter table's name

        ALTER TABLE `table` RENAME TO [new table] ;
        :param table:
        :param newname:
        :return:
        """

        ALTER_TABLE_NAME = """ALTER TABLE {} RENAME TO {} ;""".format(
            table, newname)
        if self.execute(command=ALTER_TABLE_NAME):
            Console().print(
                "Successfully renamed the table {} to {}✨ 🍰 ✨".format(
                    table, newname))

    def alter_column_name(self, table: str, column: str, newcolumn: str,
                          newtype: str):
        """Alter column's name

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ALTER  TABLE `table` CHANGE [column] [new column] [new type];

        :param table:
        :param column:
        :param newcolumn:
        :param newtype:
        :return:
        """

        ALTER_COLUMN_NAME = """ALTER  TABLE {} CHANGE COLUMN {} {} {};""".format(
            table, column, newcolumn, newtype if newtype else "")
        if self.execute(command=ALTER_COLUMN_NAME):
            Console().print("Successfully changed field {} to {} ✨ 🍰 ✨".format(
                column, newcolumn))

    def alter_column_attribute(
        self,
        table: str,
        column: str,
        newtype: str,
        defaultnull: bool = True,
        comment: str = None,
    ):
        """Alter table's field type, length, default value, comment...

ALTER  TABLE `table` MODIFY [COLUMN] field_name new_data_type new_type_length
new_default_value new_comment;
        :return:
        """

        about_default = "DEFAULT NULL" if defaultnull else "NOT NULL"
        comment = 'COMMENT "{}"'.format(comment) if comment else ""
        ALTER_COLUMN_ATTRIBUTE = """ALTER  TABLE {} MODIFY {} {} {} {};""".format(
            table, column, newtype, about_default, comment)
        if self.execute(command=ALTER_COLUMN_ATTRIBUTE):
            Console().print("The field [bold cyan]{}[/bold cyan]'s property "
                            "was successfully modified ✨ 🍰 ✨".format(column))

    def create_table(self,
                     table,
                     df: pd.DataFrame,
                     comments={},
                     primary_key=None,
                     dtypes={},
                     deduce_type=False):
        r'''Create table from dataframe

Pandas supported data types:
float、int、bool、datetime64[ns]、datetime64[ns, tz]、timedelta[ns]、category、object
    >>> mysql_client = sqlstar.mysql(...)
    >>> mysql_client.create_table(table='quant_news_analyse',
                          df=df,
                          comments={
                              "date_time": "日期",
                              "robust_stand": "稳健基准",
                              "grow_stand": "成长型基准",
                              "robust_group": "稳健型组合",
                              "grow_group": "成长型组合",
                          },
                          dtypes={
                              "datetime": ["pub_date", "update_time"],
                              "longtext": ["content"],
                              "varchar(100)": ["title"],
                              "decimal(10, 3)":
                              ["grow_stand", "robust_group", "grow_group"],
                          })
        '''
        PREFIX_SQL = f'''CREATE TABLE IF NOT EXISTS `{table}` ('''
        SUFIX_SQL = ''') DEFAULT CHARSET=utf8mb4;'''

        types = {}
        for dtype, colist in dtypes.items():
            types = merge(types, {col: dtype for col in colist})

        try:
            if deduce_type:
                # deduce and convert types, it's usually not accurately, but sometimes it will be useful
                df = df.convert_dtypes()
            cols = df.columns.tolist()

            # if there is no ID, add a self-increased ID
            if ('id' not in cols) or ('id' not in primary_key):
                PREFIX_SQL += '''`id` INT AUTO_INCREMENT COMMENT 'id','''
                # PREFIX_SQL += '''`id` INT AUTO_INCREMENT PRIMARY KEY COMMENT 'id','''

            ADDS = []
            for col in cols:
                COMMENT = ' COMMENT ""'
                DTYPE = None

                for key, comment in comments.items():
                    if str(col).strip().lower() == str(key).strip().lower():
                        COMMENT = f' COMMENT "{comment}"'

                USER_TYPE = False
                for key, dtype in types.items():
                    if str(col).strip().lower() == str(key).strip().lower():
                        DTYPE = dtype
                        USER_TYPE = True
                if not USER_TYPE:
                    DTYPE = check_dtype(df[col].dtypes)

                ADDS.append(f'''`{col}` {DTYPE} {COMMENT}''')

            PRIMARY_SQL = f' ,PRIMARY KEY (`id`)'
            if not primary_key or primary_key == 'id':
                pass
            elif isinstance(primary_key, str):
                PRIMARY_SQL = f' ,PRIMARY KEY (`id`, `{primary_key}`)'
            elif isinstance(primary_key, (list, tuple)):
                PRIMARY_SQL = f' ,PRIMARY KEY (`id`, `{"`,`".join(primary_key)}`)'

            CREATE_TABLE = PREFIX_SQL + ','.join(
                ADDS) + PRIMARY_SQL + SUFIX_SQL

            self.execute(command=CREATE_TABLE)
            Console().print(
                f"Table [blod cyan]{table}[/blod cyan] was created ✨ 🍰 ✨")

        except Exception as why:
            Console().print(f"Failed to create table {table} 👀\n{why}",
                            style='red')

    def close(self):
        try:
            self.connection.commit()
            self.connection.close()
            Console().print("Database connection closed, bye...😴",
                            style='white')
        except:
            pass
