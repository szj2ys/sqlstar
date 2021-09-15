# *_*coding:utf-8 *_*
import codecs
import importlib
import os
import sys
import warnings

from sqlstar.vendor.decorator import decorator


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
        return 'VARCHAR(50)'
    else:
        return 'VARCHAR(50)'


@decorator
def deprecated(func, *args, **kwargs):
    """ Marks a function as deprecated. """
    warnings.warn(
        f"{func} is deprecated and should no longer be used.",
        DeprecationWarning,
        stacklevel=3,
    )
    return func(*args, **kwargs)


def deprecated_option(option_name, message=""):
    """ Marks an option as deprecated. """
    def caller(func, *args, **kwargs):
        if option_name in kwargs:
            warnings.warn(
                f"{option_name} is deprecated. {message}",
                DeprecationWarning,
                stacklevel=3,
            )

        return func(*args, **kwargs)

    return decorator(caller)
