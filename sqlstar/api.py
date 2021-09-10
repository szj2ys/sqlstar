# *_*coding:utf-8 *_*
"""
Author:szj2ys
"""
import os
import traceback
from rich.console import Console
import pymysql, sys
import pandas as pd
from toolz import merge


class register(object):
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

        self.initializeDB()

    def initializeDB(self):
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                passwd=self.password,
                db=self.db,
                # charset=self.charset,
            )
            self.cur = self.conn.cursor()
        except:
            Console().print(
                "\n[bold red]Please checkout your database settings:[/bold red]\nHOST:{}\nPORT:{}\nUSER:{}\nPASSWD:{}\nDB:{}"
                .format(self.host, self.port, self.username, self.password,
                        self.db))
            raise Exception("Please checkout your database settings 💥 💔 💥")

    def get_host(self):
        return self.host

    def get_port(self):
        return self.port

    def get_username(self):
        return self.username

    def get_password(self):
        return self.password

    def get_db(self):
        return self.db

    def get_charset(self):
        return self.charset

    def get_connect(self):
        if not self.conn.open:
            self.initializeDB()
        return self.conn, self.cur

    def execute(self, command: str):
        """execute sql command

        :return: bool
        """
        conn, cur = self.get_connect()
        try:
            cur.execute(command)
            conn.commit()
            return True
        except:
            conn.rollback()
            # traceback.print_exc()
            Console().print(command, style='bold red')
            raise Exception("Ops, fail to execute this command 💥 💔 💥")

    def select(self, *, command: str):
        """select data

        :param command:
        :return:fetchdata，nlines = data，line number
        """

        conn, cur = self.get_connect()
        try:
            nlines = cur.execute(command)
            fetchdata = cur.fetchall()
            return fetchdata, nlines
        except Exception as why:
            Console().print(f"Oh no💥 💔 💥\n{why}", style='red')
            raise why

    def select_count(self, table):
        """ get the table's line number """
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
        """select data，return result as dataframe

        :param command:
        :param index_col: the index column
        :param coerce_float:非常有用，将数字形式的字符串直接以float型读入
        :param params:
        :param parse_dates:将某一列日期型字符串转换为datetime型数据，与pd.to_datetime函数功能类似。
                                            可以直接提供需要转换的列名以默认的日期形式转换，也可以用字典的格式提供
                                            列名和转换的日期格式，比如{column_name: format string}
                                            （format string:"%Y:%m:%H:%M:%S"）
        :param columns:
        :return:dataframe，columns
        """
        # return pd.read_sql(command, self.conn)
        """To fix the bug:ValueError: unsupported format character 'Y' (0x59) at index 146

       Reason:因为python执行的sql中存在类似DATE_FORMAT(CREATE_TIME, ‘%Y-%m-%d’) 的写法,
    其中%Y与python的参数%s冲突
        """

        conn, cur = self.get_connect()
        try:
            df = pd.read_sql(
                command,
                # , self.engine
                conn,
                index_col=index_col,
                coerce_float=coerce_float,
                params=params,
                parse_dates=parse_dates,
                columns=columns,
            )
        except Exception as e:
            command_parse = command.replace(
                "%", "%%") if "%" in command else command
            df = pd.read_sql(
                command_parse,
                # , self.engine
                conn,
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

    def select_all(self, table):
        """select all data from table"""
        SELECT_ALL = """SELECT * FROM {}""".format(table)
        df, cols = self.select_as_df(command=SELECT_ALL)
        return df, cols

    def truncate_table(self, table):
        """truncate table's data, but keep the table structure

        :param table:
        :return:
        """
        TRUNCATE_SQL = """TRUNCATE {};""".format(table)
        if self.execute(command=TRUNCATE_SQL):
            Console().print(f"Table [bold cyan]{table}[/bold cyan] was "
                            f"truncated ✨ 🍰 ✨")

    def delete_through_time(self, table, fild, time):
        """delete data through time"""

        DELETE_SQL = """DELETE FROM  {}  WHERE {} < "{}";""".format(
            table, fild, time)
        if self.execute(command=DELETE_SQL):
            Console().print(f"Well done ✨ 🍰 ✨")

    def insert_one(self, table, data, cols, ignore=True):
        """just insert data one piece at a time

        :param table:
        :param data:
        :param cols:
        :param ignore: Whether or no ignore duplicate data when repeat
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
        if self.execute(command=SQL_INSERT_ONE_DATA):
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
                f"There are {len(data)} pieces of data has been inserted into "
                f"table {table}✨ 🍰 ✨")

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

        conn, cur = self.get_connect()
        try:
            cur.executemany(SQL_INSERT_MANY_DATA, data)
            conn.commit()
            Console().print("Table [bold cyan]{}[/bold cyan] successfully "
                            "inserted data ✨ 🍰 ✨".format(table))
        except Exception as e:
            conn.rollback()
            Console().print("[bold red]Failed to insert data[/bold red]💥 💔 "
                            "💥\nReason:\n{}".format(e))

    def insert_df(
        self,
        table,
        df,
        cols: list,
        fillna=True,
        what="",
        dropna=False,
        axis=0,
        how="any",
        inplace=True,
    ):
        """insert dataframe to table

        :param table:
        :param df: dataframe
        :param cols: columns
        :param fillna:fill NA or not
        :param what:if fillna is True, use what to fill the NA
        :param dropna:drop NA or not
        :param axis:
        :param how:
        :param inplace: replace original data or not
        :return:
        """
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

        df_values = df[cols].fillna(what).values if fillna else df[cols].values
        insertdata = [tuple(row) for row in df_values]

        self.insert_many(table=table, data=insertdata, cols=cols)

    def show_create_table(self, table):
        """show create table command

        :param table:
        :return:
        """

        SQL = """SHOW CREATE TABLE {};""".format(table)
        result = self.select(command=SQL)
        return result[0][0][1]

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
                    f"[bold cyan]{table}[/bold cyan] was deleted ✨ "
                    f"🍰 ✨")
        else:
            Console().print("Think again...", style='green')

    def drop_column(self, table: str, column: str):

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
        """set primary key

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

        ALTER_TABLE_COMMENT = """ALTER TABLE {} COMMENT '{}' ;""".format(
            table, comment)
        if self.execute(command=ALTER_TABLE_COMMENT):
            Console().print("Table comment added successfully ✨ 🍰 ✨")

    def alter_table_name(self, table: str, newname: str):
        """alter table name
        ALTER TABLE 旧表名 RENAME TO 新表名 ;
        :param table:
        :param newname:
        :return:
        """

        ALTER_TABLE_NAME = """ALTER TABLE {} RENAME TO {} ;""".format(
            table, newname)
        if self.execute(command=ALTER_TABLE_NAME):
            Console().print("Successfully renamed the table {} to {}✨ 🍰 "
                            "✨".format(table, newname))

    def alter_column_name(self, table: str, column: str, newcolumn: str,
                          newtype: str):
        """alter column name

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        ALTER  TABLE 表名 CHANGE [column] 旧字段名 新字段名 新数据类型;
        alter  table table1 change column1 column1 varchar(100) DEFAULT 1.2 COMMENT '注释'; -- 正常，此时字段名称没有改变，能修改字段类型、类型长度、默认值、注释
        alter  table table1 change column1 column2 decimal(10,1) DEFAULT NULL COMMENT '注释' -- 正常，能修改字段名、字段类型、类型长度、默认值、注释
        :param table:
        :param column:
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
        """modify field type, length, default value, comment

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
        r'''
Pandas supported data types:
float、int、bool、datetime64[ns]、datetime64[ns, tz]、timedelta[ns]、category、object
    >>> mysql_client = sqlstar.register(...)
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

            # If there is no ID, add a self-added ID
            if not 'id' in cols and not primary_key:
                PREFIX_SQL += '''`id` INT AUTO_INCREMENT PRIMARY KEY COMMENT 'id','''

            ADDS = []
            for col in cols:
                COMMENT = ' COMMENT ""'
                DTYPE = None

                for key, comment in comments.items():
                    if str(col).strip().lower() == str(key).strip().lower():
                        COMMENT = f' COMMENT "{comment}"'

                def check_dtype(pdtype):
                    if str(pdtype).__contains__("int"):
                        return 'INT'
                    elif str(pdtype).__contains__("float"):
                        # Decimal is more precise than float
                        return 'DECIMAL(19,6)'
                    elif str(pdtype).__contains__("bool"):
                        return 'VARCHAR(18)'
                    elif str(pdtype).__contains__("datetime"):
                        return 'DATETIME'
                    elif str(pdtype).__contains__("timedelta"):
                        return 'TIMESTAMP'
                    elif str(pdtype).__contains__("category"):
                        return 'VARCHAR(18)'
                    elif str(pdtype).__contains__("object"):
                        return 'VARCHAR(500)'
                    else:
                        return 'VARCHAR(100)'

                USER_TYPE = False
                for key, dtype in types.items():
                    if str(col).strip().lower() == str(key).strip().lower():
                        DTYPE = dtype
                        USER_TYPE = True
                if not USER_TYPE:
                    DTYPE = check_dtype(df[col].dtypes)

                ADDS.append(f'''`{col}` {DTYPE} {COMMENT}''')

            SQL = PREFIX_SQL + ','.join(ADDS) + SUFIX_SQL
            self.execute(command=SQL)

        except:
            DROP_TABLE = f"DROP TABLE `{table}`;"
            self.execute(command=DROP_TABLE)

    def commit(self, command: str):
        self.cur.execute(command)
        self.conn.commit()

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
            Console().print("Database connection closed, bye...😴")
        except:
            pass
