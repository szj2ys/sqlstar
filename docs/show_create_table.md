
```python
import sqlstar

mysql = sqlstar.register(
    host="localhost", port=3306, user="root", passwd="root", db="test"
)
CREATE_SQL = mysql.show_create_table("news_focusing")
print(CREATE_SQL)
```