<p align="center">
    <img width="200" src="https://cdn.jsdelivr.net/gh/szj2ys/sqlstar/sqlstar/logo.png"/>
</p>

<h3 align="center">
    <p>✨ Awesome databases framework that shines ✨</p>
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
you may want to get information from sqlstar
```shell
sqlstar -h
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

### Execute command
```python
COMMAND = '''
  SELECT *
  FROM GIRLS
  WHERE AGE BETWEEN 20 AND 24
      AND BOYFRIEND IS NULL
  ORDER BY BEAUTY DESC;
'''
result = mysql.execute(COMMAND)
'''
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



## Acknowlegements
- [mysql-to-sqlite3](https://github.com/techouse/mysql-to-sqlite3)
- [sqlite_web](https://github.com/coleifer/sqlite-web)
- [sqlite3-to-mysql](https://github.com/techouse/sqlite3-to-mysql)







