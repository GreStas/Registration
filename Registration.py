#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#   module  :   registration.py
#   version :   0.0.1.1
#   begin   :   25/03/2017
#   new:    адаптация к многозадачности
#       1. Добавляем диспетчер, который принимает на вход пакет данных и вызывает соответсвующий метод
#       2. Каналы, из которых будет читать отдельный процесс, должна организовывать вызывающая программа
#       3. Добавляем класс модуля, который порождает процесс.
#           В дальнейшем достаточно будет импортировать только её.
#

from future_builtins import ascii
import sys
import datetime
import hashlib
import logging

_log = logging.getLogger("Registration")
_log.debug("Started")

timestamp = datetime.datetime

from PGdbpool import DBpool, Error, DataError, eSQLexec


def getRegWorker(dbconn, minconn=None, maxconn=None):
    """ getRegWorker(dbconn, minconn=None, maxconn=None): RegWorker()
    """
    if __debug__: _log.debug("getRegWorker %d %d" % (minconn,maxconn))
    dbpool = DBpool(dbconn, minconn, maxconn)
    return RegWorker(dbpool)


class RegWorker(object):
    """ class RegWorker(p_dbpool, p_log=_Null)
        класс-контейнер методов-микросервисов
        - SaveRequest
        - SendMail
        - RegApprove
        - Garbage
    """
    ErrCodes = {
        "errNone": 0,
        "errInsRegistrations": -1,
        "errDupLognameReg": -2,
        "errDupLognameUsr": -3,
        "errRegStatusToProgress": -4,
        "errRegStatusToRejected": -5,
        "errRejected": -6,
        "errRegStatusToRegistered": -7,
        "errUnicode": -8,
        "errSQL": -600,
        "errNoDataFound": -601,
    }
    ErrMsgs = {
        -1: "Cannot insert request into registrations",
        -2: "Duplicate logname in registrations",
        -3: "Duplicate logname in users",
        -4: "Cannot set registrations.status to 'progress'",
        -5: "Cannot set registrations.status to 'rejected'",
        -6: "User registration request was rejected",
        -7: "Cannot set registrations.status to 'registered'",
        -8: "Cannot encode or decode unicode",
        -600: "SQL error",
        -601: "No data found",
    }

    def __init__(self, p_dbpool):
        self._dbpool = p_dbpool
        self._log = logging.getLogger("RegWorker[%s]" % (self.__hash__()))
        if __debug__: self._log.debug("Started")

    @staticmethod
    def ErrMsg(errcode):
        """ ErrMsg(errcode): str
        по мненонике возвращает текст ошибки"""
        if __debug__: _log.debug("ErrMsg(%s)" % errcode)
        return RegWorker.ErrMsgs[RegWorker.ErrCodes[errcode]]

    @staticmethod
    def ErrCode(errcode):
        """ ErrCode(errcode): int
        по мнемонике возвращает код ошибки"""
        if __debug__: _log.debug("ErrCode(%s)" % errcode)
        return RegWorker.ErrCodes[errcode]

    @staticmethod
    def _RequestHash(request_id, logname, alias, passwd):
        stri = str(request_id) + logname + ascii(alias) + passwd + str(timestamp.utcnow())
        return hashlib.md5(stri).hexdigest()

    def closeDBpool(self):
        self._dbpool.close()

    def SendMail(self, request_id, logname, alias, authcode):
        """ int SendMail(request_id, logname, alias, authcode)
        """
        if __debug__: self._log.debug("Started")
        dbconn = self._dbpool.connect()
        try:
            stri = "UPDATE registrations SET status='progress' where id=%d" % request_id
            dbconn.execSimpleSQL(stri)
            # COMMIT отложен до отправки почты.
        except eSQLexec as e:
            if __debug__: self._log.debug("errRegStatusToProgress: eSQLexec(%s) for '%s'" % (str(e)), stri)
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.ErrCode("errRegStatusToProgress")
        # Сюда добавить реальный код отправки запроса авторизации по почте
        # authURI = web_domain + registration_path + "?a=" + authcode
        # try:
        # except ___ as e:
        # Если почту отправить не удастся, то статус не изменится, чтобы потом разобраться с причинами
        #     self.dbconn.rollback()
        #     return -2 # Cannot send email
        self._log.info("Authcode %s is sent to %s for %s." % (authcode, logname, ascii(alias)))
        dbconn.commit()
        self._dbpool.disconnect(dbconn.name)
        return 0

    def SaveRequest(self, logname, alias, passwd):
        """  SaveRequest(logname, alias, passwd):int
        PURPOSE:
        - Зафиксировать желание нового пользователя получить доступ к сети.
        INPUT:
        - logname  - запрошенныйуникальный  логин пользователя, на начальном этапе e-mail
        - alias    - имя пользователя так, как он хочет чтобы он отображался в сети, не уникальное
        - passwd   - пароль пользователя в md5
        ROUTINE:
        - Выполнить проверку на уникальность log_name в таблице пользователей.
        - Получить из sequence новый код запроса регистрации request_id
        - Сгенерировать код авторизации (например, hash-функция)
        - Сохранить запрос в журнале регистрации
        - Вызвать функцию или послать сообщение микросервису по отправке подтверждающего кода на email
          (сейчас - заглушка в виде установки status в значение progress)
        OUTPUT:
            > 0: request_id - номер запроса, полученный из sequence и сохранённый в таблице регистрации.
            < 0: ошибка
        """
        if __debug__: self._log.debug("Started")
        dbconn = self._dbpool.connect()
        request_id = None
        try:  # Пока PGdbpool.DBworker не реализован для with - используем охватывающий try с универсальным Exception
            try:
                # Проверяем на дубликат в registrations
                stri = "select count(*) from registrations where logname='%s'" % logname
                if __debug__: self._log.debug("Running '%s'" % stri)
                dbconn.execGatherSQL(stri)
                if __debug__: self._log.debug("Has run '%s'" % stri)
                rows = dbconn.fetchone()
                if __debug__: self._log.debug("Has got rows %s" % str(rows))
                if len(rows) == 0:
                    request_id = RegWorker.ErrCode("errNoDataFound")
                    if __debug__: self._log.debug("errNoDataFound")
                    raise DataError(
                        errno=request_id,
                        errspec="errNoDataFound",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest cannot select count from registrations"
                    )
                elif rows[0] > 0:
                    if __debug__: self._log.debug("errDupLognameReg")
                    request_id = RegWorker.ErrCode("errDupLognameReg")
                    if __debug__: self._log.debug("errDupLognameReg,%d" % request_id)
                    raise DataError(
                        errno=request_id,
                        errspec="errDupLognameReg",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found %d rows in registrations for %s" % (rows[0], logname)
                    )

                # Проверяем на дубликат в users
                stri = "select count(*) from users where logname='%s'" % logname
                dbconn.execGatherSQL(stri)
                if __debug__: self._log.debug("Run '%s'" % stri)
                rows = dbconn.fetchone()
                if len(rows) == 0:
                    request_id = RegWorker.ErrCode("errNoDataFound")
                    if __debug__: self._log.debug("errNoDataFound")
                    raise DataError(
                        errno=request_id,
                        errspec="errNoDataFound",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found %d rows in users for %s" % (rows[0], logname)
                    )
                elif rows[0] > 0:
                    request_id = RegWorker.ErrCode("errDupLognameUsr")
                    if __debug__: self._log.debug("errDupLognameUsr")
                    raise DataError(
                        errno=request_id,
                        errspec="errDupLognameUsr",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found dupplicate logname in users"
                    )
            except eSQLexec as e:
                request_id = RegWorker.ErrCode("errSQL")
                if __debug__: self._log.debug("errSQL")
                raise
            except DataError:
                raise
            try:
                # Получаем новый ID запроса
                stri = "SELECT nextval('register_id_seq')"
                dbconn.execGatherSQL(stri)
                if __debug__: self._log.debug("Run '%s'" % stri)
                rows = dbconn.fetchone()
                request_id = rows[0]
                if __debug__: self._log.info("has got request_id=%d" % request_id)
                # Шифруем пароль в md5
                authcode = RegWorker._RequestHash(request_id, logname, alias, passwd)
                passwd5 = hashlib.md5(passwd).hexdigest()
                # Сохранить запрос
                stri = "INSERT INTO registrations " \
                       "(id,status,logname,alias,passwd,authcode) " \
                       "VALUES (%d, 'requested', '%s', '%s', '%s', '%s')" \
                       % (request_id, logname, alias, passwd5, authcode)
                if __debug__: self._log.debug("Run '%s'" % stri)
                dbconn.execSimpleSQL(stri)
                # if __debug__: self._log.debug("After insert into registration dbconn.error is {%s}" % str(dbconn.error))
                dbconn.commit()
                # if __debug__: self._log.debug("After commit dbconn.error is {%s}" % str(dbconn.error))
            except UnicodeEncodeError as e:
                request_id = RegWorker.ErrCode("errUnicode")
                if __debug__: self._log.debug("errUnicode")
                raise
            except eSQLexec as e:
                request_id = RegWorker.ErrCode("errInsRegistrations")
                if __debug__: self._log.debug("errInsRegistrations")
                raise
            if __debug__: self._log.debug("Success with request_id=%d" % request_id)
        except (eSQLexec, DataError) as e:
            self._log.error(str(e))
        except RuntimeError as e:
            self._log.error("Enter in Exception block with request_id=%d : %s" % (request_id, str(e)))
            # self._dbpool.disconnect(dbconn.name)
            if request_id is not None and request_id < 0:
                self._log.error("Error[%d] %s"
                                % (request_id, RegWorker.ErrMsgs[request_id]))
        finally:
            self._dbpool.disconnect(dbconn.name)
        # if request_id > 0:
        #     self.SendMail(request_id, logname, alias, authcode)
        return request_id

    def RegApprove(self, authcode):
        """ RegApprover(authcode): int
            Микросервис по обработке подтверждения регистрации
            PURPOSE:
            - Необходимо перенести запись из таблицы регистрации в таблицы учёта пользователей.
            ROUTINE:
            - найти пользователя по ранее сгенерированному коду авторизации
            - внести пользователя в таблицу users
            - удалить из журнала регистрации эту запись
        """
        if __debug__: self._log.debug("Started for authcode=%s" % authcode)
        dbconn = self._dbpool.connect()
        try:
            # Копируем данные в реестр пользователей users
            # Когда появится микросервис по работе с пользователями (users) -
            #   заменить INSERT на вызов команды создания пользователя.
            stri = "insert into users (logname, alias, passwd, status) " \
                   "select logname, alias, passwd, 'registering' " \
                   "  from registrations " \
                   " where status='progress'" \
                   "   and authcode = '%s'" \
                   % authcode
            if __debug__: self._log.debug("INSERTING INTO users for authcode=%s" % authcode)
            dbconn.execSimpleSQL(stri)
        except eSQLexec as e:
            if __debug__: self._log.error("errRejected: %s" % str(e))
            try:
                stri = "UPDATE registrations SET status='rejected' " \
                       "WHERE status='progress' AND authcode = '%s'" \
                       % authcode
                dbconn.execSimpleSQL(stri)
                dbconn.commit()
            except eSQLexec as e:
                if __debug__: self._log.error("errRegStatusToRejected: %s" % str(e))
                self._dbpool.disconnect(dbconn.name)
                return RegWorker.ErrCode("errRegStatusToRejected")
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.ErrCode("errRejected")

        # Помечаем обработанную строку в журнале регистраций
        try:
            stri = "UPDATE registrations SET status='registered' " \
                   "WHERE status='progress' AND authcode = '%s'" \
                   % authcode
            if __debug__: self._log.debug("Mark REGISTERED in registrations for authcode=%s" % authcode)
            dbconn.execSimpleSQL(stri)
            if __debug__: self._log.debug("Registering of authcode=%s is success." % authcode)
            dbconn.commit()
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.ErrCode("errNone")
        except eSQLexec as e:
            if __debug__: self._log.error("errRegStatusToRegistered: %s" % str(e))
            dbconn.rollback()
        self._dbpool.disconnect(dbconn.name)
        return RegWorker.ErrCode("errRegStatusToRegistered")

    def Garbage(self, timealive):
        """ Garbage(timealive, interval): void
        Сборка мусора в журнале регистраций
        PURPOSE:
        - Зачистить не подтверждённые  обращения
        """
        # timealive - допустимое время жизни в секундах обращения на регистрацию
        if __debug__: self._log.debug("Started with %ds" % timealive)
        dbconn = self._dbpool.connect()
        stri = "delete from registrations " \
               " where status in ('progress','rejected','registered','deleted') " \
               "   and (now()-created) > '%d seconds'" \
               % timealive
        try:
            dbconn.execSimpleSQL(stri)
            dbconn.commit()
        except eSQLexec as e:
            if __debug__: self._log.error("eSQLexec %s" % str(e))
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.ErrCode("errSQL")
        else:
            self._dbpool.disconnect(dbconn.name)
        if __debug__: self._log.debug("Finished")
        return RegWorker.ErrCode("errNone")

    def Gather(self,
               fields,  # list( ("название поля", "фильтр") )
               limit=1,  # ограничение по количеству строк
               ):  # список кортежей
        """ RegWorker.Gather(): list(touple())
            Конструирует запрос к registrtions из словаря, где
             - ключе - имя поля
             - значение - фильтр после имени поля, например {"loname":"like '%@gmail.com'"}
        """
        # Парсинг параметров и генерация SQL
        select_exp = ""
        where_exp = ""
        for field,expr in fields:
            if field in ("id","status","logname","alias","created","authcode",):
                select_exp += ","+field if select_exp else field
                if expr:
                    where_exp += "and %s %s " % (field,expr) if where_exp else "%s %s " % (field,expr)
        if select_exp:
            SQL = "select %s from registrations" % select_exp
            if where_exp:
                SQL += " where " + where_exp
            if limit is not None and limit > 0:
                SQL += " limit %d" % int(limit)
        else: # Если ни одно из переданных полей не в ходит в список разрешённых, то выйти
            self._log.warn("No field is in allowed")
            return None
        if __debug__: self._log.debug(SQL)
        # Получаем коннект к БД
        # rows = []
        try:
            dbconn = self._dbpool.connect()
            dbconn.execGatherSQL(SQL)
            rows = dbconn.fetchall()
        except Error as e:
            rows = None
            self._log.error(str(e))
        finally:
            self._dbpool.disconnect(dbconn.name)
        if __debug__: _log.debug("return %d" % len(rows))
        return rows

if __name__ == "__main__":
    defconnection = {"pg_hostname": "deboraws",
                     "pg_database": "test_db",
                     "pg_user": "tester",
                     "pg_passwd": "testing",
                     "pg_schema": "dev1",
                     "pg_role": "test_db_dev1_users"}

    log_rw1 = logging.getLogger("RegWorker1")
    # dbpool_rw1 = DBpool(defconnection, minconn=1)
    # rr = RegWorker(dbpool_rw1, log_rw1)

    rr = getRegWorker(defconnection, 12, 20)

    ###
    #  Тест для RegWorker.SaveRequest
    ##
    # print "Тест для RegWorker.SaveRequest"
    # request_id = rr.SaveRequest("grestas2000@mail.ru", "GreStas", "DefaultP@w0rd")
    # if request_id is None:
    #     print "main:Unknown SaveRequest result: None"
    # elif request_id < 0:
    #     print "main:SaveRequest for (%s,%s,%s) return errorcode(%d):%s" \
    #           % ("grestas2000@gmail.com",
    #              "GreStas",
    #              "DefaultP@w0rd",
    #              request_id,
    #              RegWorker.ErrMsgs[request_id])
    # else:
    #     print "Request #%d saved successfuly." % request_id
    ###

    ###
    #  Тест для RegWorker.RegApprover
    ##
    # print "Тест для RegWorker.RegApprover"
    # result = rr.RegApprover("d3f037ec6104d9f12872075f1b225e97")
    # print "Approve return", result
    ###

    ###
    #  Тест для RegWorker.Garbage
    ##
    print "Тест для RegWorker.Garbage"
    rr.Garbage(60*60*24*2)
    ###

    ###
    #  Тест для RegWorker
    ##
    # (its_p, mine_p) = multiprocessing.Pipe()
    # rprc = RegPrc(defconnection, (its_p, mine_p))
    # rprc.start()
    # request = ("SaveRequest","grestas2000@mail.ru","GreStas","DefaultP@w0rd");print "request=", request
    # mine_p.send(request)
    # request = ("RegApprover","7c0cb2904a30049325add8282f17e42a");print "request=", request
    # mine_p.send(request)
    # request = ("Garbage",600);print "request=", request
    # mine_p.send(request)
    # answer = mine_p.recv();print "answer=", answer
    # mine_p.close();print "mine_p.close()"
    # its_p.close();print "its_p.close()"
    # rprc.terminate()
    ###

    #rr.close()
    # dbpool_rw1.close()
    rr.closeDBpool()
    del rr
