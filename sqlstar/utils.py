# *_*coding:utf-8 *_*
import pandas as pd


def check_dtype_postgre(pdtype):
    if str(pdtype).__contains__("int"):
        return 'INT'
    elif str(pdtype).__contains__("float"):
        # decimal is more precise than float
        return 'DECIMAL(19,6)'
    elif str(pdtype).__contains__("bool"):
        return 'VARCHAR(18)'
    elif str(pdtype).__contains__("datetime"):
        return 'TIMESTAMP'
    elif str(pdtype).__contains__("timedelta"):
        return 'TIMESTAMP'
    elif str(pdtype).__contains__("category"):
        return 'VARCHAR(18)'
    elif str(pdtype).__contains__("object"):
        return 'VARCHAR(50)'
    else:
        return 'VARCHAR(50)'


def check_dtype_mysql(pdtype, max_content_len, charset_len=4, min_len=4):
    """
    将 Pandas 数据类型转换为 MySQL 数据类型

    Args:
        pdtype: Pandas数据类型
        max_content_len: 内容最大长度,若小于4则自动设为4
        charset_len: 字符集编码长度,默认为4(UTF8MB4)

    Returns:
        str: 对应的MySQL数据类型
    """
    max_content_len = min_len if pd.isna(max_content_len) else max_content_len
    # 考虑25%的冗余空间
    max_content_len = max(min_len, int(max_content_len * 1.25))
    pdtype_str = str(pdtype).lower()

    # 基本数值类型判断
    if 'int' in pdtype_str:
        if 'int8' in pdtype_str:
            return 'TINYINT'
        if 'int16' in pdtype_str:
            return 'SMALLINT'
        if 'int32' in pdtype_str:
            return 'INT'
        if any(t in pdtype_str for t in ['int64', 'uint64']):
            return 'BIGINT'
        return 'INT'

    if 'float' in pdtype_str or 'decimal' in pdtype_str:
        if any(t in pdtype_str for t in ['float16', 'float32']):
            return 'FLOAT'
        if 'decimal' in pdtype_str:
            return 'DECIMAL(19,6)'
        return 'DOUBLE'

    # 布尔类型
    if 'bool' in pdtype_str:
        return 'TINYINT(1)'

    # 时间日期类型
    if any(t in pdtype_str for t in ['datetime', 'date', 'time']):
        if 'datetime' in pdtype_str:
            return 'DATETIME'
        if 'date' in pdtype_str:
            return 'DATE'
        if 'time' in pdtype_str:
            return 'TIME'
        if 'timedelta' in pdtype_str:
            return 'TIMESTAMP'

    # 类别类型
    if 'category' in pdtype_str:
        return f'VARCHAR({max_content_len})' if max_content_len <= 255 else 'TEXT'

    # 字符串类型
    if 'string' in pdtype_str or 'object' in pdtype_str:
        return _get_string_type(max_content_len, charset_len)

    # 二进制类型
    if 'bytes' in pdtype_str:
        return _get_binary_type(max_content_len)

    # 默认返回文本类型
    return _get_string_type(max_content_len, charset_len)


def _get_string_type(length, charset_len):
    """获取合适的字符串类型"""
    if length <= 255:
        return f'VARCHAR({length})'
    if length <= 65535 // charset_len:
        return 'TEXT'
    if length <= 16777215 // charset_len:
        return 'MEDIUMTEXT'
    return 'LONGTEXT'


def _get_binary_type(length):
    """获取合适的二进制类型"""
    if length <= 255:
        return f'VARBINARY({length})'
    if length <= 65535:
        return 'BLOB'
    if length <= 16777215:
        return 'MEDIUMBLOB'
    return 'LONGBLOB'
