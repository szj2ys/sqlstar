# *_*coding:utf-8 *_*
import sys
from loguru import logger

from sqlstar.core import Database, DatabaseURL
from .__version__ import version, __version__

logger.remove()
logger.add(
    sys.stderr,
    format=
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> | <cyan>{file}:{line}</cyan> - <level>{message}</level>"
)

__all__ = ["Database", "DatabaseURL"]
