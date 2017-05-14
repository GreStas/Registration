# -*- coding: utf-8 -*-
#
#   Package : dbpool.pgdbpool
#   File : __init__.py
#

import threading
import multiprocessing
from dbpool import DBPoolBase, DBPoolMP, DBPoolMT, SQLexecError
from dbpool.dbworkerbase import DBWorkerBase
from dbpool.dbproxybase import DBProxyBase
import psycopg2


class PGDBWorker(DBWorkerBase):
    def __init__(self, dbconn, p_name, p_lock, p_input, p_output, p_client_evt, p_server_evt, p_error):
        """
        PostgreSQL Worker class
        :param dbconn: Dictionary for describing DB adapter connect and other parameters
            dbconn["pg_database"]
            dbconn["pg_user"]
            dbconn["pg_hostname"]
            dbconn["pg_passwd"]
            dbconn["pg_role"]
            dbconn["pg_schema"]
        :param p_name: Process identifier
        :param p_lock: Internal lock
        :param p_input: Queue for commands
        :param p_output: Queue for results
        :param p_client_evt: Event for Client process
        :param p_server_evt: Event for Server process
        :param p_error: Dictionary {errno, errspec, errmsg, remark} for describing error
        """
        super(PGDBWorker, self).__init__(
            dbconn, p_name, p_lock, p_input, p_output, p_client_evt, p_server_evt, p_error
        )
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).__init__ Started')
        # Set user's role in DB
        if dbconn["pg_role"]:
            self.set_param("role", dbconn["pg_role"])
        #  Include default DB schema in searchpath
        if dbconn["pg_schema"]:
            self.set_param("search_path", "pg_catalog,%s" % dbconn["pg_schema"])

    def connect(self, dbconn):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).connect Started')
        try:
            conn = psycopg2.connect(
                "dbname='%s' user='%s' host='%s' password='%s'"
                % (dbconn["pg_database"], dbconn["pg_user"], dbconn["pg_hostname"], dbconn["pg_passwd"])
            )
        except psycopg2.Error as e:
            self._set_error(e.pgcode, e.pgerror, e.message, "Logon Error:", )
            raise
        return conn, conn.cursor()

    def disconnect(self):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).disconnect Started.')
        self._dbconn.close()

    def commit(self):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).commit Started')
        try:
            self._dbconn.commit()
        except psycopg2.Error as e:
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "Commit Error:")

    def rollback(self):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).rollback Started')
        try:
            self._dbconn.rollback()
        except psycopg2.Error as e:
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "Rollback Error:")

    def set_param(self, param_name, value):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).set_param Started')
        return self.exec_simple_sql("SET %s = %s" % (param_name, value))

    def exec_simple_sql(self, sql):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).exec_simple_sql Started')
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            self._log.error("pgError in '%s':" % sql)
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "execSimpleSQL")

    def exec_gather_sql(self, sql):
        if __debug__:
            self._log.debug('PGDBWorker(DBWorkerBase).exec_gather_sql Started(%s)' % sql)
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            self._log.error("pgError in '%s'" % sql)
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "dbworkerprc[%s].execGatherSQL" % self._name)


class PGDBWorkerMP(PGDBWorker, multiprocessing.Process):
    pass


class PGDBWorkerMT(PGDBWorker, threading.Thread):
    pass


class PGDBPool(DBPoolBase):
    def __init__(self, dbconn, cls_manager, minconn=None, maxconn=None):
        super(PGDBPool, self).__init__(dbconn, cls_manager, DBProxyBase, PGDBWorker, minconn, maxconn)


class PGDBPoolMP(DBPoolMP):
    def __init__(self, dbconn, minconn=None, maxconn=None):
        super(PGDBPoolMP, self).__init__(dbconn, DBProxyBase, PGDBWorkerMP, minconn, maxconn)


class PGDBPoolMT(DBPoolMT):
    def __init__(self, dbconn, minconn=None, maxconn=None):
        super(PGDBPoolMT, self).__init__(dbconn, DBProxyBase, PGDBWorkerMT, minconn, maxconn)
