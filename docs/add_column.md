
```python
import sqlstar

mysql = sqlstar.register(
    host="localhost", port=3306, user="root", passwd="root", db="test"
)
mysql.add_column(table="", column="", newtype="", comment="", after="")
```