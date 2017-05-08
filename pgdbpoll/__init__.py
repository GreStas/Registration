# -*- coding: utf-8 -*-
#
#   Package : pgdbpoll
#   File : __init__.py
#

from mpdbpoll import DBPollBase, InterfaceError, SQLexecError
from mpdbpoll.dbworkerbase import DBWorkerBase
from mpdbpoll.dbproxybase import DBProxyBase
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
        # Set user's role in DB
        if dbconn["pg_role"]:
            self.set_param("role", dbconn["pg_role"])
        #  Include default DB schema in searchpath
        if dbconn["pg_schema"]:
            self.set_param("search_path", "pg_catalog,%s" % dbconn["pg_schema"])

    def connect(self, dbconn):
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
        self._dbconn.close()

    def commit(self):
        try:
            self._dbconn.commit()
        except psycopg2.Error as e:
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "Commit Error:")

    def rollback(self):
        try:
            self._dbconn.rollback()
        except psycopg2.Error as e:
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "Rollback Error:")

    def set_param(self, param_name, value):
        return self.exec_simple_sql("SET %s = %s" % (param_name, value))

    def exec_simple_sql(self, sql):
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            self._log.error("pgError in '%s':" % sql)
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "execSimpleSQL")

    def exec_gather_sql(self, sql):
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            self._log.error("pgError in '%s'" % sql)
            raise SQLexecError(e.pgcode, e.pgerror, e.message, "dbworkerprc[%s].execGatherSQL" % self.name)


class PGDBPoll(DBPollBase):
    def __init__(self, dbconn, minconn=None, maxconn=None):
        super(PGDBPoll, self).__init__(dbconn, DBProxyBase, PGDBWorker, minconn, maxconn)
