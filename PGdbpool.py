# -*- coding: utf-8 -*-
#
#   Module : PGdbworker.py
#
try:
    import os
    import sys
    import logging
    import multiprocessing
    import psycopg2
except ImportError, info:
    print "Import Error:", info
    sys.exit()


class _Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, name): return self
    def __setattr__(self, name, value): pass
    def __delattr__(self,name): pass

logging.basicConfig(filename="PGdbpool.log",
                    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s:%(levelname)s:%(message)s",
                    level=logging.DEBUG)
                    # level=logging.ERROR)
_log = _Null()  # резервируем глобальную переменную для логирования
_log = logging.getLogger("PGdbpool")
_log.debug("Started")


# Common module exception Error
class Error(RuntimeError):
    def __init__(self, errno, errspec, errmsg, remark=None):
        self.args = (errno, errspec, errmsg, remark)
        self.errno = errno
        self.errspec = errspec
        self.errmsg = errmsg
        self.remark = remark
        # self.msg_detail = psycopg2.Error.diag.message_detail
    def ePrint(self):
        if self.remark is not None : print self.remark
        print "pgcode =", self.errno
        print "pgerror =", self.errspec
        print "pgmessage = ", self.errmsg
# Ошибки, связанные с неправильным использованием интерфейса к базе данных, но не с самой БД.
class InterfaceError(Error): pass
# Ошибки, имеющие отношение к самой БД.
class DatabaseError(Error): pass
# Ошибки, связанные с обработкой данных. Например, недопустимое преобразование типов, деление на ноль итд.
class DataError(Error): pass
# Ошибки, связанные с работой самой БД. Например, потеря соединения.
class OperationalError(Error): pass
# Ошибки, связанные с нарушением целостности БД.
class IntegrityError(Error): pass
# Внутренняя ошибка БД. Например, обращение к устаревшему курсору.
class InternalError(Error): pass
# Ошибки в запросах SQL.
class ProgrammingError(Error): pass
# Ошибки обращения к методам программного интерфейса, которые не поддерживаются БД.
class NotSupportedError(Error): pass
# Ошибки в стиле dbworker
class eSQLexec(Error): pass
def raiseSQLexec(p_error):
    """ преобразует словарь p_error в вызов исключения eSQLexec"""
    if p_error["errno"] is None or p_error["errno"] == 0:
        if __debug__: _log.debug(str(p_error))
        return
    if __debug__: _log.error(str(p_error))
    raise eSQLexec(
        errno=p_error["errno"],
        errspec=p_error["errspec"],
        errmsg=p_error["errmsg"],
        remark=p_error["remark"],
    )


class DBworker(object):
    instance = 0
    def __init__(self, p_name, p_lock, p_input, p_output, p_client_evt, p_server_evt, p_error):
        self._log = logging.getLogger("DBworker[%s][%s]" % (self.__hash__(),p_name))
        self.name = p_name
        self._lock = p_lock
        self._input = p_input
        self._output = p_output
        self._client_evt = p_client_evt; self._client_evt.clear()
        self._server_evt = p_server_evt; self._server_evt.clear()
        self.error = p_error; self._ClearError()
        if __debug__: self._log.debug("Created instance %d" % DBworker.instance)
        DBworker.instance += 1

    def _ClearError(self):
        self.error = {"errno": 0, "errspec": None, "errmsg": None, "remark": None, }

    def _pass_to_server(self, data):
        with self._lock:
            self._input.put(data)
            # pause client side
            if self._client_evt.is_set():
                if __debug__: self._log.debug("Clearing client_evt...")
                self._client_evt.clear()
                if __debug__: self._log.debug("    ...has cleared client_evt.")
            else:
                if __debug__: self._log.warn("client_evt already cleared.")
                pass
            # continue server side
            if not self._server_evt.is_set():
                if __debug__: self._log.debug("Setting server_evt...")
                self._server_evt.set()
                if __debug__: self._log.debug("    ...has set server_evt.")
            else:
                if __debug__: self._log.warn("server_evt already set.")
                pass
        if __debug__: self._log.debug("is waiting...")
        self._client_evt.wait()
        if __debug__: self._log.debug("    ...recieved error description %s" % str(self.error))
        raiseSQLexec(self.error)

    def close(self):
        """ close is DBworker destructor
            Зачищаем атрибуты класса, чтобы этим экземпляром уже никто не мог воспользоватся
        """
        with self._lock:
            del self.name
            del self._input
            del self._output
            self._client_evt.clear()
            del self._client_evt
            self._server_evt.clear()
            del self._server_evt
            del self.error
        del self._lock
        if __debug__: self._log.debug("Cleared successfully.")

    def commit(self):
        self._pass_to_server(("commit", None))

    def rollback(self):
        self._pass_to_server(("rollback", None))

    def execSimpleSQL(self, sql):
        if __debug__: self._log.debug("sending ('%s','%s')" % ("execSimpleSQL", sql))
        self._pass_to_server(("execSimpleSQL", sql))

    def execGatherSQL(self, sql):
        if __debug__: self._log.debug("sending ('%s','%s')" % ("execGatherSQL", sql))
        self._pass_to_server(("execGatherSQL", sql))

    def _FetchRows(self, fetchscope):
        if __debug__: self._log.debug("Running for %d rows ..." % fetchscope)
        self._pass_to_server(("fetch", fetchscope))
        rows = list()
        while not self._output.empty():
            row = self._output.get()
            if __debug__: self._log.debug("has got %s" % str(row))
            if row is None:
                break
            else:
                rows.append(row)
        if __debug__: self._log.debug("return %s" % str(rows))
        return rows

    def fetchone(self):
        return self._FetchRows(1)

    def fetchmany(self,count):
        return self._FetchRows(count)

    def fetchall(self):
        return self._FetchRows(0)


class FetchProperty(object):
    def __init__(self,value=0):
        self.name = "_fetch"
        if not isinstance(value,int) or value < 0: raise TypeError("Value must be int>=0")
        self.value = value
    def __get__(self, instance, cls):
        return getattr(instance,self.name,self.value)
    def __set__(self, instance, value):
        if not isinstance(value,int) or value < 0: raise TypeError("Value must be int>=0")
        self.value = value
    def __delete__(self, instance):
        raise AttributeError("Cannot delete attribute")

class DBworkerPrc(multiprocessing.Process):
    """ class dbworkerprc
        - использует внутренний коннект к БД и внутренний курсор для выполнения команд
        - обработчик принимает команды по названиям методов
    """
    def __init__(self,
                 dbconn,        # Dictionary for describing PGconnect and additional parameters
                 p_name,        # Process identifier
                 p_lock,        # Lock for unknown :)
                 p_input,       # Queue for commands
                 p_output,      # Queue for results
                 p_client_evt,  # Event for Client process
                 p_server_evt,  # Event for Server process
                 p_error,  # Dictionary for describing error
                 ):
        multiprocessing.Process.__init__(self)
        self._log = logging.getLogger("DBworkerPrc[%s][%s]" % (self.__hash__(),p_name))
        self._log.debug("Started")

        # Process variables
        self.name = p_name
        self._lock = p_lock
        self._input = p_input
        self._output = p_output
        self._client_evt = p_client_evt; self._client_evt.clear()
        self._server_evt = p_server_evt; self._server_evt.clear()
        self.error = p_error; self._ClearError()
        self._working = True  # Признак, что можно работать

        # DB variables
        # self.dbconn = dbconn
        # self.pg_hostname = dbconn["pg_hostname"]
        # self.pg_database = dbconn["pg_database"]
        # self.pg_user = dbconn["pg_user"]
        # self.pg_passwd = dbconn["pg_passwd"]
        # self.pg_defschema = dbconn["pg_schema"]
        # self.pg_defrole = dbconn["pg_role"]

        # myconn & mycurr - переменные для нотации dbworker
        self._dbconn = None
        try:
            self._dbconn = psycopg2.connect(
                "dbname='%s' user='%s' host='%s' password='%s'"
                % (dbconn["pg_database"], dbconn["pg_user"], dbconn["pg_hostname"], dbconn["pg_passwd"])
            )
        except psycopg2.Error as e:
            self._SetError(e.pgcode,e.pgerror,e.message,"Logon Error:",)
            raise InterfaceError(e.pgcode, e.pgerror, e.message, "Logon Error:")

        self._curr = self._dbconn.cursor()

        # _fetchscope определяем через свойство,
        #  чтобы контролировать тип передаваемых данных до начала выполнения команды
        #  ==0 - fetchall; ==1 - fetchone; > 1 - fetchmany
        self._fetchscope = FetchProperty()

        # Установим роль
        if dbconn["pg_role"]: self._SetParam("role", dbconn["pg_role"])
        #  Включим в строку поиска схему по-умолчанию
        if dbconn["pg_schema"]: self._SetParam("search_path", "pg_catalog,%s" % dbconn["pg_schema"])

        if __debug__: self._log.debug("Is created.")

    def _SetError(self, err_no, err_spec, err_msg, err_remark,):
        self.error = {
            "errno": err_no,
            "errspec": err_spec,
            "errmsg": err_msg,
            "remark": err_remark, }

    def _ClearError(self):
        self.error = {
            "errno": 0,
            "errspec": None,
            "errmsg": None,
            "remark": None, }

    def close(self, immediate=False):
        """ DBworkerPrc.closer(immediate=False): void"""
        if immediate:
            self._working = False
            self._input.put((None,None))
            self._server_evt.set()
        else:
            self._working = False

    def commit(self):
        if __debug__: self._log.debug(".")
        try:
            self._dbconn.commit()
        except psycopg2.Error as e:
            raise eSQLexec(e.pgcode, e.pgerror, e.message, "Commit Error:")

    def rollback(self):
        if __debug__: self._log.debug(".")
        try:
            self._dbconn.rollback()
        except psycopg2.Error as e:
            raise eSQLexec(e.pgcode, e.pgerror, e.message, "Rollback Error:")

    def execSimpleSQL(self, sql):
        if __debug__: self._log.debug(sql)
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            if __debug__ : self._log.error("pgError in '%s':" % sql)
            raise eSQLexec(e.pgcode, e.pgerror, e.message, "execSimpleSQL")

    def execGatherSQL(self, sql):
        if __debug__: self._log.debug(sql)
        try:
            self._curr.execute(sql)
        except psycopg2.Error as e:
            if __debug__ : self._log.error("pgError in '%s'" % sql)
            raise eSQLexec(e.pgcode, e.pgerror, e.message, "dbworkerprc[%s].execGatherSQL" % self.name)

    def _SetParam(self, param_name, value):
        return self.execSimpleSQL("SET %s = %s" % (param_name, value))

    def _pass_to_client(self):
        # pause server side
        if self._server_evt.is_set():
            if __debug__: self._log.debug("Clearing server_evt...")
            self._server_evt.clear()
            if __debug__: self._log.debug("    ...has cleared server_evt.")
        else:
            if __debug__: self._log.warn("server_evt already cleared.")
            pass
        # continue client side
        if not self._client_evt.is_set():
            if __debug__: self._log.debug("Setting client_evt...")
            self._client_evt.set()
            if __debug__: self._log.debug("    ...has set client_evt.")
        else:
            if __debug__: self._log.warn("client_evt already set.")
            pass

    def run(self):
        """ dbworkerprc.run():void """
        if __debug__: self._log.debug("Running...")
        while self._working:
            cmnd = None
            sql = None
            try:
                if __debug__: self._log.debug("Waiting server_evt...")
                self._server_evt.wait()   # Ждём новую команду от клиента
                if __debug__: self._log.debug("    ...has got server_evt.")
            except EOFError as e:
                if __debug__: self._log.critical("Has got unexpected EOF of server_evt.")
                self._working = False
                continue

            if __debug__: self._log.debug("Waiting lock...")
            with self._lock:
                try:

                    if __debug__: self._log.debug("    ...has locked")
                    (cmnd,sql) = self._input.get()   # Раз событие пришло, то в очереди что-то должно заваляться
                    if __debug__: self._log.debug("Has got command '%s'" % cmnd)

                    if cmnd == "commit": self.commit()
                    elif cmnd == "rollback": self.rollback()
                    elif cmnd == "execSimpleSQL": self.execSimpleSQL(sql)
                    elif cmnd == "execGatherSQL": self.execGatherSQL(sql)
                    elif cmnd == "fetch":
                        self._fetchscope = sql
                        rows = list()
                        if self._fetchscope == 0: rows = self._curr.fetchall()
                        elif self._fetchscope == 1: rows = self._curr.fetchone()
                        elif self._fetchscope > 1: rows = self._curr.fetchmany(self._fetchscope)
                        for row in rows:
                            if __debug__: self._log.debug("fetch put row %s" % str(row))
                            self._output.put(row)
                        self._output.put(None)
                    self._ClearError()

                except eSQLexec as e:
                    self.error = {"errno":e.errno,"errspec":e.errspec,"errmsg":e.errmsg,"remark":e.remark,}
                    if __debug__: self._log.error("[%s]: has got error '%s'" % (cmnd, str(self.error)))
                except Error:
                    # по любой непонятной ошибке - завершаем работу цикла
                    self.error = {"errno": e.errno, "errspec": e.errspec, "errmsg": e.errmsg, "remark": e.remark, }
                    if __debug__: self._log.error("[%s]: has got error '%s'" % (cmnd, str(self.error)))
                    self._working = False
                    continue
                except EOFError as e:
                    if __debug__: self._log.critical("Has got unexpected EOF of input queue.")
                    self._working = False
                    continue
                finally:
                    self._pass_to_client()
            if __debug__: self._log.debug("    ...has unlocked.")

        # финализация, если вышли из цикла не нормальным способом (continue или break)
        self._pass_to_client()
        if __debug__: self._log.debug("Finished with last command '%s'" % cmnd)


class DBpool(object):
    """ class dbpool
    - Создаёт от  mincount до maxcount процессов обработчиков dbworkerprc и для каждого из них окружение:
        - очереди in и out
        - блокировку, чтобы параллельно никто не смог использовать обрабочик, когда он работает
        - событие, чтобы сигнализировать, что выполнение SQL завершено
        - словарь, для хранения информации о возникшей ошибки
    - метод connect создаёт и выдаёт экземпляр прокси-объекта dbworker
    - метод disconnect удаляет экземпляр прокси-объекта dbworker
    """
    def __init__(self, dbconn, minconn=None, maxconn=None):
        self._log = logging.getLogger("DBpool[%s]" % self.__hash__())
        self._log.debug("Started")
        # Определить минимальное и максимальное количество процессов в пуле
        cpucount = multiprocessing.cpu_count()
        if minconn is None and maxconn is None:
            self.minconn = cpucount
            self.maxconn = cpucount
        elif minconn is None and maxconn is not None:
            self.maxconn = maxconn
            if maxconn > cpucount:
                self.minconn = cpucount
            else:
                self.minconn = maxconn
        elif minconn is not None and maxconn is None:
            self.minconn = minconn
            self.maxconn = minconn
        else:
            self.minconn = minconn
            self.maxconn = maxconn
        if __debug__: self._log.debug("cpucount:%d , minconn:%d , maxconn:%d" % (cpucount,self.minconn,self.maxconn))
        # Один менеджер в экземпляре класса на все просессы
        self.dbconn = dbconn
        self.manager = multiprocessing.Manager()
        # jobslock предназанчен не допустить одновременные изменения состоняния массивов, описывающих процессы
        self._jobslock = multiprocessing.Lock()
        # Инициируем массивы для пула
        self.prccount = 0   # практически всегда это номер следующего создаваемого процесса

        # jobs - состояние процесса:
        #   creating - в процессе создания
        #   created - создан
        #   ready - готов использованию
        #   busy - используется
        #   empty - очищен
        self._jobs = []

        # locks - блокировка на этапе выполнения и отдачи результата запроса
        self._locks = []

        # input_qs - очередь команд к обработчику
        self._input_qs = []

        # output_qs - очередь результатов
        self._output_qs = []

        # errors - хранит описание последней ошибки для обработчика в словаре с полями:
        #          {"errno":0,"errspec":None,"errmsg":None,"remark":None,}
        self._errors = []

        # _client_evts - события от обработчика к клиенту о завершении выполнения команды
        self._client_evts = []

        # _server_evts - события от клиента к обработчику об отправке команды на выполнение
        self._server_evts = []

        # _processes - экземпляр класса dbworkerprc, который непосредственно выполняет команды БД
        self._processes = []

        # _clients - экземпляр класса dbworker, прокси-объект, который непосредственно отправляет команды обработчику
        self._clients = []

        # Создаём и стартуем процессы
        for iprc in range(self.minconn): self._AppendPrc()
        if __debug__: self._log.debug("Created %d processes." % self.prccount)

    def _AppendPrc(self):
        """ append(void):boolean
            Добавляет ещё один процесс и запускает его
        """
        if self.prccount >= self.maxconn:
            if __debug__: self._log.debug("Лимит процессов исчерпан")
            return False # Лимит процессов исчерпан

        if __debug__: self._log.debug("Waiting jobslock ...")
        with self._jobslock:
            if __debug__: self._log.debug("has got jobslock")
            # if __debug__: print "dbpool.append[%d]: Started" % self.prccount

            self._jobs.append("creating")
            # if __debug__: print "dbpool.append: jobs[%d] set to %s" % (self.prccount,self._jobs[self.prccount])

            self._locks.append(self.manager.Lock())

            self._input_qs.append(self.manager.Queue())

            self._output_qs.append(self.manager.Queue())

            self._server_evts.append(self.manager.Event())
            self._server_evts[self.prccount].clear()

            self._client_evts.append(self.manager.Event())
            self._client_evts[self.prccount].clear()

            self._errors.append(self.manager.dict())
            self._errors[self.prccount] = {"errno": 0, "errspec": None, "errmsg": None, "remark": None, }

            try:
                self._processes.append(
                    DBworkerPrc(
                        self.dbconn,
                        str(self.prccount),
                        self._locks[self.prccount],
                        self._input_qs[self.prccount],
                        self._output_qs[self.prccount],
                        self._client_evts[self.prccount],
                        self._server_evts[self.prccount],
                        self._errors[self.prccount],
                    )
                )
                self._processes[self.prccount].name = str(self.prccount)
                self._processes[self.prccount].daemon = True
                self._jobs[self.prccount] = "created"
            except Exception as e:
                errmsg = "[%d] error:%s" % (self.prccount,e.message)
                # Удалить служебные объекты и выйти
                self._jobs[self.prccount] = "empty"
                if __debug__: self._log.error("[%d]: status set to %s" % (self.prccount,self._jobs[self.prccount]))
                raise RuntimeError(errmsg)
            if __debug__: self._log.debug("[%d]: status set to %s" % (self.prccount,self._jobs[self.prccount]))
            try:
                self._processes[self.prccount].start()
                if __debug__: self._log.debug("[%d]: started process" % self.prccount)
                self._clients.append(None)  # Обозначить, что к обработчику не присоединён ни один Клиент
                self._jobs[self.prccount] = "ready"
                if __debug__: self._log.debug("dbpool.append[%d]: status set to %s" % (self.prccount, self._jobs[self.prccount]))
            except Exception as e:
                errmsg = "dbpool.append[%d] error:%s" % (self.prccount,e.message)
                # Удалить процесс, служебные объекты и выйти
                self._processes[self.prccount].close()
                self._jobs[self.prccount] = "empty"
                if __debug__: self._log.error("[%d]: status set to %s" % (self.prccount, self._jobs[self.prccount]))
                raise RuntimeError(errmsg)
            if __debug__: self._log.debug("[%d]: status is set to %s" % (self.prccount, self._jobs[self.prccount]))
            self.prccount += 1
        if __debug__: self._log.debug("jobslock is cleared.")

    def _RemovePrc(self):
        """ remove(void):void
            останавливает последний созданный процесс
            удаляет и закрывает связанные с ним объекты
        """
        self.prccount -= 1
        if self._clients[self.prccount] is not None: # Если какой клиент не отсоединился - отсоединяем
            if __debug__: self._log.debug("Call disconnect for Client [%s]" % self._clients[self.prccount].name)
            self.disconnect(self._clients[self.prccount].name)
        with self._jobslock:
            self._jobs[self.prccount] = "stopping"
            self._processes[self.prccount].close() # Останавливаем серверный процесс обработчика
            self._jobs[self.prccount] = "empty"

    def close(self): # dbpool.close
        while self.prccount > 0:
            self._RemovePrc()
        self.manager.shutdown()

    def connect(self):
        """ dbpool.connect():connection
        Создаёт и выдаёт экземпляр прокси-объекта dbworker для обработчика dbworkerprc
        """
        neednewprc = False
        # Найти свободный процесс и занять его
        while True:
            if __debug__: self._log.debug("Waiting jobslock ...")
            with self._jobslock:
                if __debug__: self._log.debug("has got jobslock")
                neednewprc = False
                iprc = 0
                if self.prccount >= self.maxconn: # добавить обработчик уже не получится
                    while not self._jobs[iprc] == "ready":
                        if __debug__: self._log.debug("DBpool.connect: self.jobs[%d]=%s" % (iprc,self._jobs[iprc]))
                        iprc += 1
                        if iprc >= self.prccount:
                            iprc = 0
                            raise RuntimeError("DBpool.connect: free connection is not exist")
                    # Заняли найденный процесс
                    self._jobs[iprc] = "busy"
                else:
                    while iprc < self.prccount:
                        if self._jobs[iprc] == "ready": break
                        iprc += 1
                    if iprc == self.prccount:
                        neednewprc = True
                    else:
                        # Заняли найденный процесс
                        self._jobs[iprc] = "busy"
                # Сюда можно попасть либо с найденным процессом для которого установлен статус "busy"
                # либо с установленным флагом neednewprc
                # Так как массивы лучше менять под блокировкой, то создаём экземпляр прокси-обекта здесь
                if not neednewprc and self._jobs[iprc] == "busy":
                    self._clients[iprc] = \
                        DBworker(
                            iprc,
                            self._locks[iprc],
                            self._input_qs[iprc],
                            self._output_qs[iprc],
                            self._client_evts[iprc],
                            self._server_evts[iprc],
                            self._errors[iprc]
                        )
            if __debug__: self._log.debug("jobslock is cleared.")
            if neednewprc:
                self._AppendPrc()
            else:
                break
        if __debug__: self._log.debug("[%d]: success. Prccount = %d. New length of arrays(jobs) is %d" % (iprc,self.prccount,len(self._jobs)))
        return self._clients[iprc]

    def disconnect(self, p_name):
        if __debug__: self._log.debug("Waiting jobslock ...")
        with self._jobslock:
            if __debug__: self._log.debug("Has got jobslock: jobs[%d] is %s" % (p_name,self._jobs[p_name]))
            # удаляем экземпляр прокси-объекта, чтобы никто его случайно не использовал
            if __debug__: self._log.debug("Deleting clients[%s]=%s" % (p_name,str(self._clients[p_name])))
            self._clients[p_name].close()
            self._clients[p_name] = None
            # Устанавливаем статус, что процесс не занят
            self._jobs[p_name] = "ready"
            if __debug__: self._log.debug("jobs[%d] set to %s" % (p_name,self._jobs[p_name]))
        if __debug__: self._log.debug("jobslock is cleared.")


if __name__ == "__main__":

    defconnection = {}
    defconnection["pg_hostname"] = "deboraws"
    defconnection["pg_database"] = "test_db"
    defconnection["pg_user"] = "tester"
    defconnection["pg_passwd"] = "testing"
    defconnection["pg_role"] = "test_db_dev1_users"
    defconnection["pg_schema"] = "dev1"

    # pause = 0.001

    pool = DBpool(defconnection, minconn=2, maxconn=4)

    # conn1 = pool.connect(); print "main: conn1 is process #%s" % conn1.name
    # conn2 = pool.connect(); print "main: conn2 is process #%s" % conn2.name
    # conn3 = pool.connect(); print "main: conn3 is process #%s" % conn3.name
    # print "sleeping after 3 connects"
    # time.sleep(pause)

    # conn1.execSimpleSQL("select count(*) from registrations")

    # pool.disconnect(conn1.name)
    # print "main: sleeping after disconnect #1"
    # time.sleep(pause)

    conn4 = pool.connect(); print "main: conn4 is process #%s" % conn4.name

    conn5 = pool.connect(); print "main: conn5 is process #%s" % conn5.name
    # test_sql = "select count(*) from registrations"
    test_sql = "select count(*) from registrations where logname='%s'" % "GreStas"
    print "main.sql = '%s'" % test_sql
    try:

        print "main: Test execSimpleSQL"
        conn4.execSimpleSQL(test_sql)

        print "main: Test execGatherSQL.fetchone()"
        conn5.execGatherSQL(test_sql)
        rows = conn5.fetchone()
        for row in rows: print "main fetched: ", row

        # print "main: Test execGatherSQL.fetchmany(20)"
        # conn4.execGatherSQL("select * from registrations")
        # rows = conn4.fetchmany(20)
        # for row in rows: print "main fetched: ", row

        # print "main: Test execGatherSQL.fetchall()"
        # conn5.execGatherSQL("select * from registrations")
        # rows = conn5.fetchall()
        # for row in rows: print "main fetched: ", row

    except Error as e:
        print "main: error "
        e.ePrint()

    # pool.disconnect(conn2.name)
    # pool.disconnect(conn3.name)
    # pool.disconnect(conn4.name)
    # pool.disconnect(conn5.name)

    pool.close()
