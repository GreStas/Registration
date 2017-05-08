# -*- coding: utf-8 -*-
#
#   Package : mpdbpoll.dbworkerbase
#   File : dbworkerbase.py
#

import multiprocessing
from .common import *


class FetchProperty(object):
    def __init__(self, value=0):
        self.name = "_fetch"
        if not isinstance(value, int) or value < 0:
            raise TypeError("Value must be int>=0")
        self.value = value

    def __get__(self, instance, cls):
        return getattr(instance, self.name, self.value)

    def __set__(self, instance, value):
        if not isinstance(value, int) or value < 0:
            raise TypeError("Value must be int>=0")
        self.value = value

    def __delete__(self, instance):
        raise AttributeError("Cannot delete attribute")


class DBWorkerBase(multiprocessing.Process):
    def __init__(self, dbconn, p_name, p_lock, p_input, p_output, p_client_evt, p_server_evt, p_error):
        """
        BASE DBWorker class
        :param dbconn: Dictionary for describing DB adapter connect and other parameters
        :param p_name: Process identifier
        :param p_lock: Internal lock
        :param p_input: Queue for commands
        :param p_output: Queue for results
        :param p_client_evt: Event for Client process
        :param p_server_evt: Event for Server process
        :param p_error: Dictionary {errno, errspec, errmsg, remark} for describing error
        """
        self._log = set_logging("%s[%s][%s]" % (self.__class__.__name__, self.__hash__(), p_name))
        # multiprocessing.Process.__init__(self)
        super(DBWorkerBase, self).__init__()
        self._dbconn, self._curr = self.connect(dbconn)
        # Internal variables to control of process
        self._name = p_name
        self._lock = p_lock
        self._input = p_input
        self._output = p_output
        self._client_evt = p_client_evt
        self._client_evt.clear()
        self._server_evt = p_server_evt
        self._server_evt.clear()
        self._error = p_error
        self._clear_error()
        self._working = True  # Признак, что можно работать - управляет циклом в run()
        self._fetchscope = FetchProperty()

    def __repr__(self):
        return """
        DBconnection: %s
        Name: '%s'
        Input is empty: %s
        Output is full: %s
        Server event is set: %s
        Client event is set: %s
        Is working:  %s
        Last error: %s
        """ % (
            self._dbconn,
            self._name,
            self._input.empty(),
            self._output.full(),
            self._server_evt.is_set(),
            self._client_evt.is_set(),
            self._working,
            self._error,
        )

    def connect(self, dbconn):
        """ Connect to DB and get cursor

        :param dbconn: Dict with parameter for database adapter
        :return: (database connection object, database cursor object)
        """
        return None, None

    def disconnect(self):
        pass

    def _clear_error(self):
        self._error = {
            "errno": 0,
            "errspec": None,
            "errmsg": None,
            "remark": None, }

    def _set_error(self, err_no, err_spec, err_msg, err_remark, ):
        self._error = {
            "errno": err_no,
            "errspec": err_spec,
            "errmsg": err_msg,
            "remark": err_remark,
        }

    def _pass_control(self):
        """ Pause server and pass control to client by events

        :return: void
        """
        # pause server side
        if self._server_evt.is_set():
            self._server_evt.clear()
        # continue client side
        if not self._client_evt.is_set():
            self._client_evt.set()

    def set_param(self, param_name, value):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def exec_simple_sql(self, sql):
        pass

    def exec_gather_sql(self, sql):
        return [(sql,)]

    def shutdown(self, immediate=False):
        self._working = False
        if immediate:
            self._input.put((None, None))
            self._server_evt.set()

    def run(self):
        while self._working:
            try:
                self._server_evt.wait()   # Ждём новую команду от клиента
            except EOFError as e:
                self._working = False
                continue
            with self._lock:
                try:
                    (cmnd, param) = self._input.get()   # Раз событие пришло, то в очереди что-то должно заваляться
                    if (cmnd, param) == (None, None):
                        break
                    elif cmnd == CMND_COMMIT:
                        self.commit()
                    elif cmnd == CMND_ROLLBACK:
                        self.rollback()
                    elif cmnd == CMND_EXECSQL:
                        self.exec_simple_sql(param)
                    elif cmnd == CMND_GATHER:
                        self.exec_gather_sql(param)
                    elif cmnd in (CMND_FETCHONE, CMND_FETCHMANY, CMND_FETCHALL):
                        rows = list()
                        if cmnd == CMND_FETCHALL:
                            rows = self._curr.fetchall()
                        elif cmnd == CMND_FETCHONE:
                            rows = self._curr.fetchone()
                        elif cmnd == CMND_FETCHMANY:
                            self._fetchscope = param
                            rows = self._curr.fetchmany(self._fetchscope)
                        for row in rows:
                            self._output.put(row)
                        self._output.put(None)
                    self._clear_error()
                except SQLexecError as e:
                    self._set_error(e.errno, e.errspec, e.errmsg, e.remark)
                    self._log.error("[%s]: has got error '%s'" % (cmnd, str(self._error)))
                except Error:
                    # по любой непонятной ошибке - завершаем работу цикла
                    self._set_error(e.errno, e.errspec, e.errmsg, e.remark)
                    self._log.error("[%s]: has got error '%s'" % (cmnd, str(self._error)))
                    self._working = False
                    continue
                except EOFError as e:
                    if __debug__:
                        self._log.critical("Has got unexpected EOF of input queue.")
                    self._working = False
                    continue
                finally:
                    self._pass_control()
        # финализация, если вышли из цикла не нормальным способом (continue или break)
        self._pass_control()
