# *_*coding:utf-8 *_*


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


def check_dtype_mysql(pdtype):
    if str(pdtype).__contains__("int"):
        return 'INT'
    elif str(pdtype).__contains__("float"):
        # decimal is more precise than float
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
        return 'VARCHAR(50)'
    else:
        return 'VARCHAR(50)'
