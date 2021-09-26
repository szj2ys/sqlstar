<p align="center">
    <img width="350" height="150" src="https://cdn.jsdelivr.net/gh/szj2ys/sqlstar/sqlstar/logo.png"/>
</p>

<h3 align="center">
    <p>✨ Awesome databases framework that shines ✨</p>
</h3>

<p align="center">
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://badge.fury.io/py/sqlstar.svg" alt="Version"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://img.shields.io/pypi/l/sqlstar.svg?color=blue" 
alt="License"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://static.pepy.tech/badge/sqlstar?color=blue" alt="pypi total downloads"/>
    </a>
    <a href="https://python.org/pypi/sqlstar">
        <img src="https://img.shields.io/github/last-commit/szj2ys/sqlstar?color=blue" alt="GitHub last commit"/>
    </a>
</p>


Breaking changes ⚙ 🔑 🎉 🎠 ❌
- **easy to use:** lots of out-of-the-box methods.
- **less bug:** not like others, I don't want to name it, and if you 
  unluckily enough to encounter, it's easy to solve by yourself.


## Installation ⚙🙈
```shell
pip install sqlstar
```
if you need help
```shell
sqlstar -h
```

## Tips and tricks ✅

<details>
  <summary>Guides 📝</summary>

>for now, there is only mysql backend...

## connection
```python
import sqlstar

# driver://user:passwd@host:port/dbname
mysql = sqlstar.Database('mysql://root:***@localhost/tmp')
mysql.connect()
```
## Query
```python
QUERY = '''
    SELECT *
    FROM Girls
    WHERE AGE BETWEEN 20 AND 24
        AND BOYFRIEND IS NULL
    ORDER BY WHITE, RICH, BEAUTY DESC;
'''
```
### Fetch data, and format result into Dataframe
```python
df = mysql.fetch_df(QUERY)
```
Fetch all the rows
```python
data = mysql.fetch_all(QUERY)
```
Fetch several rows
```python
data = mysql.fetch_many(QUERY, 3)
```

## Execute
```python
mysql.execute("""
    CREATE TABLE `users` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `email` varchar(255) COLLATE utf8_bin NOT NULL,
        `password` varchar(255) COLLATE utf8_bin NOT NULL,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    AUTO_INCREMENT=1 ;
    """)
```

## Insert
### Insert many records
```python
mysql.insert_many(table, data, cols)
```
### Insert Dataframe type of data
```python
mysql.insert_df(table, df)
```

## Export
### Export result to csv
```python
mysql.export_csv(query, fname, sep)
```
### Export result to excel
```python
mysql.export_excel(query, fname)
```

</details>

<details>
  <summary>Nice Features ✨</summary>

### Create table
```python
mysql.create_table(
    "users",
    comments={
        "name": "姓名",
        "height": "身高",
        "weight": "体重"
    },
    dtypes={
        "varchar(30)": [
            "name",
            "occupation",
        ],
        "float": ["height", "weight"],
        "int": ["age"],
    },
)
```
if you have data, you can make it more simple, just like this
```python
mysql.create_table("users", df)
```
if you only want to specify some of them
```python
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
    # if type is not given, sqlstar will automatically inference
    dtypes={
        "datetime": ["create_time", "publish_time"],
        "longtext": ["content"],
        "varchar(100)": ["title", "author"],
        "decimal(10, 3)": ["read_num"]
    })
```
You don't need to fill in everything, and you just need to fill in 
comment or data type that you want to specify, then 
`sqlstar` will do the rest for you.

### Rename table
```python
mysql.rename_table(table, name)
```

### Rename column
```python
mysql.rename_column(table, column, name, dtype)
```

### Add new column
```python
mysql.add_column(table, column, dtype, comment, after)
```
### Add comment for table
```python
mysql.add_table_comment(table, comment)
```

### Change column's attribute
```python
mysql.change_column_attribute(table, column, dtype, notnull, comment)
```

### Set primary key
```python
mysql.add_primary_key(table, primary_key)
```

### Truncate table's data, but keep the table structure
```python
mysql.truncate_table(table)
```

### Drop table
```python
mysql.drop_table(table)
```

### Drop column
```python
mysql.drop_column(table, column)
```

</details>

