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
import datetime
import hashlib
import logging

_log = logging.getLogger("Registration")
_log.debug("Started")

timestamp = datetime.datetime

from PGdbpool import DBpool, Error, DataError, SQLexecError


def getRegWorker(dbconn, minconn=None, maxconn=None):
    """ Создаёт экземпляр класса RegWorker
    :param dbconn:
    :param minconn:
    :param maxconn:
    :return: RegWorker()
    """
    if __debug__:
        _log.debug("getRegWorker %d %d" % (minconn, maxconn))
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
        if __debug__:
            self._log.debug("Started")

    @staticmethod
    def get_errmsg(err_mnemo):
        """ ErrMsg(errcode): str
        по мненонике возвращает текст ошибки"""
        if __debug__:
            _log.debug("ErrMsg(%s)" % err_mnemo)
        return RegWorker.ErrMsgs[RegWorker.ErrCodes[err_mnemo]]

    @staticmethod
    def get_errno(err_mnemo):
        """ ErrCode(errcode): int
        по мнемонике возвращает код ошибки"""
        if __debug__:
            _log.debug("ErrCode(%s)" % err_mnemo)
        return RegWorker.ErrCodes[err_mnemo]

    @staticmethod
    def _request_hash(request_id, logname, alias, passwd):
        stri = str(request_id) + ascii(logname) + ascii(alias) + passwd + str(timestamp.utcnow())
        return hashlib.md5(stri).hexdigest()

    def close_dbpool(self):
        self._dbpool.close()

    def sendmail(self, request_id, logname, alias, authcode):
        """ int SendMail(request_id, logname, alias, authcode)
        """
        if __debug__:
            self._log.debug("Started")
        dbconn = self._dbpool.connect()
        try:
            stri = "UPDATE registrations SET status='progress' where id=%d" % request_id
            dbconn.exec_simple_sql(stri)
            # COMMIT отложен до отправки почты.
        except SQLexecError as e:
            if __debug__:
                self._log.debug("errRegStatusToProgress: eSQLexec(%s) for '%s'" % (str(e)), stri)
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.get_errno("errRegStatusToProgress")
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

    def save_request(self, logname, alias, passwd):
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
        if __debug__:
            self._log.debug("Started")
        dbconn = self._dbpool.connect()
        request_id = None
        try:  # Пока PGdbpool.DBworker не реализован для with - используем охватывающий try с универсальным Exception
            try:
                # Проверяем на дубликат в registrations
                stri = "select count(*) from registrations where logname='%s'" % logname
                if __debug__:
                    self._log.debug("Running '%s'" % stri)
                dbconn.exec_gather_sql(stri)
                if __debug__:
                    self._log.debug("Has run '%s'" % stri)
                rows = dbconn.fetchone()
                if __debug__:
                    self._log.debug("Has got rows %s" % str(rows))
                if len(rows) == 0:
                    request_id = RegWorker.get_errno("errNoDataFound")
                    if __debug__:
                        self._log.debug("errNoDataFound")
                    raise DataError(
                        errno=request_id,
                        errspec="errNoDataFound",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest cannot select count from registrations"
                    )
                elif rows[0] > 0:
                    if __debug__:
                        self._log.debug("errDupLognameReg")
                    request_id = RegWorker.get_errno("errDupLognameReg")
                    if __debug__:
                        self._log.debug("errDupLognameReg,%d" % request_id)
                    raise DataError(
                        errno=request_id,
                        errspec="errDupLognameReg",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found %d rows in registrations for %s" % (rows[0], logname)
                    )

                # Проверяем на дубликат в users
                stri = "select count(*) from users where logname='%s'" % logname
                dbconn.exec_gather_sql(stri)
                if __debug__:
                    self._log.debug("Run '%s'" % stri)
                rows = dbconn.fetchone()
                if len(rows) == 0:
                    request_id = RegWorker.get_errno("errNoDataFound")
                    if __debug__:
                        self._log.debug("errNoDataFound")
                    raise DataError(
                        errno=request_id,
                        errspec="errNoDataFound",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found %d rows in users for %s" % (rows[0], logname)
                    )
                elif rows[0] > 0:
                    request_id = RegWorker.get_errno("errDupLognameUsr")
                    if __debug__:
                        self._log.debug("errDupLognameUsr")
                    raise DataError(
                        errno=request_id,
                        errspec="errDupLognameUsr",
                        errmsg=RegWorker.ErrMsgs[request_id],
                        remark="RegWorker.SaveRequest found dupplicate logname in users"
                    )
            except SQLexecError:
                request_id = RegWorker.get_errno("errSQL")
                if __debug__:
                    self._log.debug("errSQL")
                raise
            except DataError:
                raise
            try:
                # Получаем новый ID запроса
                stri = "SELECT nextval('register_id_seq')"
                dbconn.exec_gather_sql(stri)
                if __debug__:
                    self._log.debug("Run '%s'" % stri)
                rows = dbconn.fetchone()
                request_id = rows[0]
                if __debug__:
                    self._log.info("has got request_id=%d" % request_id)
                # Шифруем пароль в md5
                authcode = RegWorker._request_hash(request_id, logname, alias, passwd)
                passwd5 = hashlib.md5(passwd).hexdigest()
                # Сохранить запрос
                stri = "INSERT INTO registrations " \
                       "(id,status,logname,alias,passwd,authcode) " \
                       "VALUES (%d, 'requested', '%s', '%s', '%s', '%s')" \
                       % (request_id, logname, alias, passwd5, authcode)
                if __debug__:
                    self._log.debug("Run '%s'" % stri)
                dbconn.exec_simple_sql(stri)
                # if __debug__:
                #     self._log.debug("After insert into registration dbconn.error is {%s}" % str(dbconn.error))
                dbconn.commit()
                # if __debug__:
                #     self._log.debug("After commit dbconn.error is {%s}" % str(dbconn.error))
            except UnicodeEncodeError:
                request_id = RegWorker.get_errno("errUnicode")
                if __debug__:
                    self._log.debug("errUnicode")
                raise
            except SQLexecError:
                request_id = RegWorker.get_errno("errInsRegistrations")
                if __debug__:
                    self._log.debug("errInsRegistrations")
                raise
            if __debug__:
                self._log.debug("Success with request_id=%d" % request_id)
        except (SQLexecError, DataError) as e:
            self._log.error(str(e))
        except RuntimeError as e:
            self._log.error("Enter in Exception block with request_id=%d : %s" % (request_id, str(e)))
            # self._dbpool.disconnect(dbconn.name)
            if request_id is not None and request_id < 0:
                self._log.error("Error[%d] %s"
                                % (request_id, RegWorker.ErrMsgs[request_id]))
        finally:
            self._dbpool.disconnect(dbconn.name)
        if request_id > 0:
            self.sendmail(request_id, logname, alias, authcode)
        return request_id

    def approve(self, authcode):
        """ RegApprover(authcode): int
            Микросервис по обработке подтверждения регистрации
            PURPOSE:
            - Необходимо перенести запись из таблицы регистрации в таблицы учёта пользователей.
            ROUTINE:
            - найти пользователя по ранее сгенерированному коду авторизации
            - внести пользователя в таблицу users
            - удалить из журнала регистрации эту запись
        """
        if __debug__:
            self._log.debug("Started for authcode=%s" % authcode)
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
            if __debug__:
                self._log.debug("INSERTING INTO users for authcode=%s" % authcode)
            dbconn.exec_simple_sql(stri)
        except SQLexecError as e:
            self._log.error("errRejected: %s" % str(e))
            try:
                stri = "UPDATE registrations SET status='rejected' " \
                       "WHERE status='progress' AND authcode = '%s'" \
                       % authcode
                dbconn.exec_simple_sql(stri)
                dbconn.commit()
            except SQLexecError as e:
                self._log.error("errRegStatusToRejected: %s" % str(e))
                self._dbpool.disconnect(dbconn.name)
                return RegWorker.get_errno("errRegStatusToRejected")
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.get_errno("errRejected")

        # Помечаем обработанную строку в журнале регистраций
        try:
            stri = "UPDATE registrations SET status='registered' " \
                   "WHERE status='progress' AND authcode = '%s'" \
                   % authcode
            if __debug__:
                self._log.debug("Mark REGISTERED in registrations for authcode=%s" % authcode)
            dbconn.exec_simple_sql(stri)
            if __debug__:
                self._log.debug("Registering of authcode=%s is success." % authcode)
            dbconn.commit()
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.get_errno("errNone")
        except SQLexecError as e:
            self._log.error("errRegStatusToRegistered: %s" % str(e))
            dbconn.rollback()
        self._dbpool.disconnect(dbconn.name)
        return RegWorker.get_errno("errRegStatusToRegistered")

    def garbage(self, timealive):
        """ Garbage(timealive, interval): void
        Сборка мусора в журнале регистраций
        PURPOSE:
        - Зачистить не подтверждённые  обращения
        """
        # timealive - допустимое время жизни в секундах обращения на регистрацию
        if __debug__:
            self._log.debug("Started with %ds" % timealive)
        dbconn = self._dbpool.connect()
        stri = "delete from registrations " \
               " where status in ('progress','rejected','registered','deleted') " \
               "   and (now()-created) > '%d seconds'" \
               % timealive
        try:
            dbconn.exec_simple_sql(stri)
            dbconn.commit()
        except SQLexecError as e:
            self._log.error("eSQLexec %s" % str(e))
            self._dbpool.disconnect(dbconn.name)
            return RegWorker.get_errno("errSQL")
        else:
            self._dbpool.disconnect(dbconn.name)
        if __debug__:
            self._log.debug("Finished")
        return RegWorker.get_errno("errNone")

    def gather(self, fields, limit=1):
        """ Конструирует запрос к registrtions, выполняет и возвращает список кортежей
        :param fields: # list( ("название поля", "фильтр") ) [("logname":"like '%@gmail.com'")]
        :param limit: ограничение по количеству строк
        :return: [(),(), ...]
        """
        # Парсинг параметров и генерация SQL
        select_exp = ""
        where_exp = ""
        for field, expr in fields:
            if field in ("id", "status", "logname", "alias", "created", "authcode"):
                select_exp += ","+field if select_exp else field
                if expr:
                    where_exp += "and %s %s " % (field, expr) if where_exp else "%s %s " % (field, expr)
        if select_exp:
            sql = "select %s from registrations" % select_exp
            if where_exp:
                sql += " where " + where_exp
            if limit is not None and limit > 0:
                sql += " limit %d" % int(limit)
        else:   # Если ни одно из переданных полей не в ходит в список разрешённых, то выйти
            self._log.warn("No field is in allowed")
            return None
        if __debug__:
            self._log.debug(sql)
        # Получаем коннект к БД и делаем выборку
        try:
            dbconn = self._dbpool.connect()
        except Error as e:
            self._log.error(str(e))
            return None
        try:
            dbconn.exec_gather_sql(sql)
            rows = dbconn.fetchall()
        except Error as e:
            rows = None
            self._log.error(str(e))
        finally:
            self._dbpool.disconnect(dbconn.name)
        if __debug__:
            _log.debug("return %d" % len(rows))
        return rows
