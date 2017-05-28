# -*- coding: utf-8 -*-
#
#   Package : dbpool.dbproxybase
#   File : dbproxybase.py
#

import logging
from common import *


class DBProxyBase(object):
    def __init__(self, p_name, p_lock, p_input, p_output, p_client_evt, p_server_evt, p_error):
        """
        BASE DBProxy class
        :param p_name: Process identifier
        :param p_lock: Internal lock
        :param p_input: Queue for commands
        :param p_output: Queue for results
        :param p_client_evt: Event for Client process
        :param p_server_evt: Event for Server process
        :param p_error: Dictionary {errno, errspec, errmsg, remark} for describing error
        """
        self._log = logging.getLogger("DBProxyBase[%s][%s]" % (self.__hash__(), p_name))
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

    def __repr__(self):
        return """
        Name: %d
        Input is empty: %s
        Output is full: %s
        Server event is set: %s
        Client event is set: %s
        Last error: %s
        """ % (
            self._name,
            self._input.empty(),
            self._output.full(),
            self._server_evt.is_set(),
            self._client_evt.is_set(),
            self._error,
        )

    @property
    def name(self):
        return self._name

    def _clear_error(self):
        self._error = {
            "errno": 0,
            "errspec": None,
            "errmsg": None,
            "remark": None, }

    def raise_sql_exec(self, p_error):
        """ преобразует словарь p_error в вызов исключения SQLexecError"""
        if p_error["errno"] is None or p_error["errno"] == 0:
            return
        self._log.error(str(p_error))
        raise SQLexecError(
            errno=p_error["errno"],
            errspec=p_error["errspec"],
            errmsg=p_error["errmsg"],
            remark=p_error["remark"],
        )

    def _pass_control(self, data):
        """ Pause client, send  command and pass control to server by events

        :param data: default touple(cmnd, param)
        :return: void
        """
        with self._lock:
            self._input.put(data)
            # pause client side
            if self._client_evt.is_set():
                self._client_evt.clear()
            # continue server side
            if not self._server_evt.is_set():
                self._server_evt.set()
        self._client_evt.wait()
        self.raise_sql_exec(self._error)

    def commit(self):
        self._pass_control((CMND_COMMIT, None))

    def rollback(self):
        self._pass_control((CMND_ROLLBACK, None))

    def exec_simple_sql(self, sql):
        self._pass_control((CMND_EXECSQL, sql))

    def exec_gather_sql(self, sql):
        self._pass_control((CMND_GATHER, sql))

    def _fetch_rows(self):
        rows = list()
        while not self._output.empty():
            row = self._output.get()
            if row is None:
                break
            else:
                rows.append(row)
        return rows

    def fetchone(self):
        self._pass_control((CMND_FETCHONE, 1))
        return self._fetch_rows()

    def fetchmany(self, count):
        self._pass_control((CMND_FETCHMANY, int(count)))
        return self._fetch_rows()

    def fetchall(self):
        self._pass_control((CMND_FETCHALL, 0))
        return self._fetch_rows()
