# -*- coding: utf-8 -*-
#
#   Package : dbpool.common
#   File : common.py
#


import logging


def set_logging(template):
    return logging.getLogger(template)


class Error(Exception):
    def __init__(self, errno, errspec, errmsg, remark=None):
        self.errno = errno
        self.errspec = errspec
        self.errmsg = errmsg
        self.remark = remark

    def __repr__(self):
        return "%s" \
               "pgcode=%s\n" \
               "pgerror=%s\n" \
               "pgmessage=%s" \
               % (
                   ('' if self.remark is None else "%s\n" % self.remark),
                   self.errno,
                   self.errspec,
                   self.errmsg,
               )


class InterfaceError(Error):
    """Ошибки, связанные с неправильным использованием интерфейса к базе данных, но не с самой БД."""
    pass


class DatabaseError(Error):
    """Ошибки, имеющие отношение к самой БД."""
    pass


class DataError(Error):
    """Ошибки, связанные с обработкой данных. Например, недопустимое преобразование типов, деление на ноль итд."""
    pass


class OperationalError(Error):
    """Ошибки, связанные с работой самой БД. Например, потеря соединения."""
    pass


class IntegrityError(Error):
    """Ошибки, связанные с нарушением целостности БД."""
    pass


class InternalError(Error):
    """Внутренняя ошибка БД. Например, обращение к устаревшему курсору."""
    pass


class ProgrammingError(Error):
    """Ошибки в запросах SQL."""
    pass


class NotSupportedError(Error):
    """Ошибки обращения к методам программного интерфейса, которые не поддерживаются БД."""
    pass


class SQLexecError(Error):
    """Ошибки в стиле dbworker"""
    pass


CMND_EXIT = 0
CMND_COMMIT = 1
CMND_ROLLBACK = 2
CMND_EXECSQL = 3
CMND_GATHER = 4
CMND_FETCHONE = 5
CMND_FETCHMANY = 6
CMND_FETCHALL = 7
