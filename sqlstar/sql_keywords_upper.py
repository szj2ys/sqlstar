# *_*coding:utf-8 *_*
"""
Descri：
"""
import re


def keywords_upper(command):
    command = re.sub(r"\bselect\b", "SELECT", command)
    command = re.sub(r"\bas\b", "AS", command)
    command = re.sub(r"\bwith\b", "WITH", command)
    command = re.sub(r"\bcreate\b", "CREATE", command)
    command = re.sub(r"\binsert\b", "INSERT", command)
    command = re.sub(r"\bshow\b", "SHOW", command)
    command = re.sub(r"\bdrop\b", "DROP", command)
    command = re.sub(r"\btable\b", "TABLE", command)
    command = re.sub(r"\bif\b", "IF", command)
    command = re.sub(r"\bnot\b", "NOT", command)
    command = re.sub(r"\bcase\b", "CASE", command)
    command = re.sub(r"\bwhen\b", "WHEN", command)
    command = re.sub(r"\bexists\b", "EXISTS", command)
    command = re.sub(r"\bnull\b", " NULL", command)
    command = re.sub(r"\bint\b", " INT", command)
    command = re.sub(r"\bdecimal\b", " DECIMAL", command)
    command = re.sub(r"\bfloat\b", " FLOAT", command)
    command = re.sub(r"\btext\b", " TEXT", command)
    command = re.sub(r"\blongtext\b", " LONGTEXT", command)
    command = re.sub(r"\bvarchar\b", " VARCHAR", command)
    command = re.sub(r"\bdefault\b", " DEFAULT", command)
    command = re.sub(r"\bauto_increment\b", "AUTO_INCREMENT", command)
    command = re.sub(r"\bprimary\b", "PRIMARY", command)
    command = re.sub(r"\bkey\b", "KEY", command)
    command = re.sub(r"\bdelete\b", "DELETE", command)
    command = re.sub(r"\bupdate\b", "UPDATE", command)
    command = re.sub(r"\bfrom\b", "FROM", command)
    command = re.sub(r"\bdistinct\b", "DISTINCT", command)
    command = re.sub(r"\bcount\b", "COUNT", command)
    command = re.sub(r"\bsum\b", "SUM", command)
    command = re.sub(r"\bavg\b", "AVG", command)
    command = re.sub(r"\bmin\b", "MIN", command)
    command = re.sub(r"\bmax\b", "MAX", command)
    command = re.sub(r"\bwhere\b", "WHERE", command)
    command = re.sub(r"\bis\b", "IS", command)
    command = re.sub(r"\bbetween\b", "BETWEEN", command)
    command = re.sub(r"\blike\b", "LIKE", command)
    command = re.sub(r"\brlike\b", "RLIKE", command)
    command = re.sub(r"\bregexp\b", "REGEXP", command)
    command = re.sub(r"\bon\b", "ON", command)
    command = re.sub(r"\bleft\b", "LEFT", command)
    command = re.sub(r"\bright\b", "RIGHT", command)
    command = re.sub(r"\binner\b", "INNER", command)
    command = re.sub(r"\bouter\b", "OUTER", command)
    command = re.sub(r"\bjoin\b", "JOIN", command)
    command = re.sub(r"\bgroup\b", "GROUP", command)
    command = re.sub(r"\border\b", "ORDER", command)
    command = re.sub(r"\bby\b", "BY", command)
    command = re.sub(r"\blimit\b", "LIMIT", command)
    command = re.sub(r"\bdesc\b", "DESC", command)
    command = re.sub(r"\basc\b", "ASC", command)
    command = re.sub(r"\bhaving\b", "HAVING", command)
    command = re.sub(r"\bstr_to_date\b", "STR_TO_DATE", command)
    command = re.sub(r"\bdate_format\b", "DATE_FORMAT", command)
    command = re.sub(r"\bdate_sub\b", "DATE_SUB", command)
    command = re.sub(r"\brow_number\b", "ROW_NUMBER", command)
    command = re.sub(r"\bpartition\b", "PARTITION", command)
    command = re.sub(r"\bindex\b", "INDEX", command)
    command = re.sub(r"\bcommit\b", "COMMIT", command)
    command = re.sub(r"\band\b", "AND", command)
    command = re.sub(r"\bin\b", "IN", command)
    return command


if __name__ == "__main__":
    SQL = """

    """

    UPPERED_COMMAND = keywords_upper(command=SQL)
    print(UPPERED_COMMAND)
