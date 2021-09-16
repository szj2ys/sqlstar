<p align="center">
    <img width="200" src="https://cdn.jsdelivr.net/gh/szj2ys/sqlstar/sqlstar/logo.png"/>
</p>

<h3 align="center">
    <p>Awesome SQL client you are waiting for ✨</p>
</h3>


<p align="center">
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://badge.fury.io/py/sqlstar.svg" alt="Version"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://img.shields.io/pypi/l/sqlstar.svg?color=orange" alt="License"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://static.pepy.tech/badge/sqlstar?color=blue" alt="pypi total downloads"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://img.shields.io/pypi/dm/sqlstar?color=blue" alt="pypi downloads"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://img.shields.io/github/last-commit/szj2ys/sqlstar?color=blue" alt="GitHub last commit"/>
    </a>
    <a href="https://github.com/szj2ys/sqlstar">
        <img src="https://visitor-badge.glitch.me/badge?page_id=szj2ys.sqlstar" alt="Visitors"/>
    </a>
    <a href="https://github.com/szj2ys/sqlstar">
        <img src="https://img.shields.io/github/stars/szj2ys/sqlstar?style=social" alt="Stars"/>
    </a>
</p>


# Installation
```shell
pip install sqlstar
```
you may want to checkout the version
```shell
sqlstar version
```
Haha, `sqlstar` is now on your environment, having fun with it, enjoy ...

# Usage
## MySQL
### Select 
```python
import sqlstar


mysql = sqlstar.mysql(
    host="localhost", port=3306, user="root", passwd="root", db="adv_center"
)

df, cols = mysql.select_as_df(command='''SELECT * FROM table LIMIT 10''')
```
or
```python
data, nlines = mysql.select(command='''SELECT * FROM table LIMIT 10''')
```

### Create table
```python
mysql = sqlstar.mysql(...)
mysql.create_table(
    table='news_spider',
    df=df,
    comments={
        "create_time": "插入时间",
        "title": "标题",
        "content": "正文",
        "author": "作者",
        "publish_time": "发布时间",
        "read_num": "阅读量",
    },
    # if type is not given, SQLStar will automatically inference
    dtypes={
        "datetime": ["create_time", "publish_time"],
        "longtext": ["content"],
        "varchar(100)": ["title", "author"],
        "decimal(10, 3)": ["read_num"]
    })
```
You don't need to fill in everything, and you just need to fill in 
comment or data type that you want to specify, then 
SQLStar will do the rest for you.

## MySQL to SQLite3

Transfer data from MySQL to SQLite.
And it transfers all data from a MySQL database to a SQLite3 database.


### How to run

```bash
mysql2sqlite --help
```


### Example
```
mysql2sqlite -f test.sqlite -d jydb -h localhost -u root -p
```

# SQLite3 to MySQL
Transfer data from SQLite 3 to MySQL
### How to run

```bash
sqlite2mysql --help
```

### Usage
```shell
...
```
# SQLite

<details>
  <summary>Quick Start</summary>

```python
import sqlstar

if __name__ == '__main__':
    sqliting = sqlstar.sqlite("./test.db")
    sqliting.execute("CREATE TABLE IF NOT EXISTS tester (timestamp DATETIME, uuid TEXT)")
    sqliting.execute("INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
    sqliting.execute("INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))

    results, cols = sqliting.select_as_df("SELECT * from tester")
    print(results)
    print(cols)

    sqliting.close()
```

</details>

## SQLite Web
### Usage

```sh
$ sqlite_web /path/to/database.db
```

### Features


* Works with your existing SQLite databases, or can be used to create new databases.
* Add or drop:
  * Tables
  * Columns (yes, you can drop and rename columns!)
  * Indexes
* Export data as JSON or CSV.
* Import JSON or CSV files.
* Browse table data.

### Screenshots

The index page shows some basic information about the database, including the number of tables and indexes, as well as its size on disk:

![](http://media.charlesleifer.com/blog/photos/s1415479324.32.png)

The `structure` tab displays information about the structure of the table, including columns, indexes, and foreign keys (if any exist). From this page you can also create, rename or drop columns and indexes.

![](http://media.charlesleifer.com/blog/photos/s1415479418.23.png)

The `content` tab displays all the table data. Links in the table header can be used to sort the data:

![](http://media.charlesleifer.com/blog/photos/s1415479502.61.png)

The `query` tab allows you to execute arbitrary SQL queries on a table. The query results are displayed in a table and can be exported to either JSON or CSV:

![](http://media.charlesleifer.com/blog/photos/s1415487149.3.png)

The `import` tab supports importing CSV and JSON files into a table. There is an option to automatically create columns for any unrecognized keys in the import file:

![](http://media.charlesleifer.com/blog/photos/s1415479625.44.png)

### Command-line options

The syntax for invoking sqlite_web is:

```console

$ sqlite_web [options] /path/to/database-file.db
```

The following options are available:

* ``-p``, ``--port``: default is 8080
* ``-H``, ``--host``: default is 127.0.0.1
* ``-d``, ``--debug``: default is false
* ``-x``, ``--no-browser``: do not open a web-browser when sqlite_web starts.
* ``-P``, ``--password``: prompt for password to access sqlite_web.
  Alternatively, the password can be stored in the "SQLITE_WEB_PASSWORD"
  environment variable, in which case the application will not prompt for a
  password, but will use the value from the environment.
* ``-r``, ``--read-only``: open database in read-only mode.
* ``-u``, ``--url-prefix``: URL prefix for application, e.g. "/sqlite_web".
* ``-c``, ``--cert`` and ``-k``, ``--key`` - specify SSL cert and private key.
* ``-a``, ``--ad-hoc`` - run using an ad-hoc SSL context.


## Acknowlegements
- [mysql-to-sqlite3](https://github.com/techouse/mysql-to-sqlite3)
- [sqlite_web](https://github.com/coleifer/sqlite-web)
- [sqlite3-to-mysql](https://github.com/techouse/sqlite3-to-mysql)







