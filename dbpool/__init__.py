# -*- coding: utf-8 -*-
#
#   Package : dbpool
#   File : __init__.py
#

import logging
import threading
import multiprocessing
import Queue
from common import *

_log = logging.getLogger("dbpool")

_HNDL_WORKER = 'worker'   # DBWorkerBase
_HNDL_PROXY = 'proxy'     # DBProxyBase
_HNDL_STATUS = 'status'   # str
_HNDL_LOCK = 'lock'       # multiprocessing.Manager.Lock(),
_HNDL_INPUT = 'input'     # multiprocessing.Manager.Queue(),
_HNDL_OUTPUT = 'output'   # multiprocessing.Manager.Queue(),
_HNDL_CLNTEVT = 'client'  # multiprocessing.Manager.Event(),
_HNDL_SRVREVT = 'server'  # multiprocessing.Manager.Event(),
_HNDL_ERROR = 'error'     # multiprocessing.Manager.Dict(),

_STS_CREATING = 'creating'
_STS_WORKER = 'worker'
_STS_PROXY = 'proxy'
_STS_CREATED = 'created'
_STS_STARTED = 'started'
_STS_READY = 'ready'
_STS_BUSY = 'busy'
_STS_STOPPING = 'stopping'
_STS_STOPPED = 'stopped'


class DBPoolBase(object):
    def __init__(self, dbconn, cls_manager, cls_proxy, cls_worker, minconn=None, maxconn=None):
        """
        Organize DataBase connection Pool by multiprocessing.Manager control
        :param dbconn: Dictionary for describing PGconnect and additional parameters
        :param cls_proxy: DBProxyBase child class
        :param cls_worker: DBWorkerBase child class
        :param minconn: min connections
        :param maxconn: max connections
        """
        self._log = logging.getLogger("%s[%s]" % (self.__class__.__name__, self.__hash__()))
        if __debug__:
            # print "%s[%s] Started" % (self.__class__.__name__, self.__hash__())
            self._log.debug('Started')
        self._dbconn = dbconn
        self._manager = cls_manager()
        self._proxy = cls_proxy
        self._worker = cls_worker
        self._freelist = self._manager.get_queue()
        self._jobslock = self._manager.get_lock()  # jobslock  для целостности _conns и _prccount
        self._handles = []    # состоит из словарей с объектами для связки между Proxy и Worker
        self._prccount = 0  # практически всегда это номер следующего создаваемого процесса, len(_cons)
        self._working = True
        # Определить минимальное и максимальное количество процессов в пуле
        cpucount = multiprocessing.cpu_count()
        if minconn is None and maxconn is None:
            self._minconn = cpucount
            self._maxconn = cpucount
        elif minconn is None and maxconn is not None:
            self._maxconn = maxconn
            if maxconn > cpucount:
                self._minconn = cpucount
            else:
                self._minconn = maxconn
        elif minconn is not None and maxconn is None:
            self._minconn = minconn
            self._maxconn = minconn
        else:
            self._minconn = minconn
            self._maxconn = maxconn
        # Создаём и стартуем процессы
        for iprc in range(self._minconn):
            self._append()

    def _append(self):
        """
        Добавление пар Proxy+Worker в _handles и в очередь свободных
        :return: boolean
        """
        handle = {}
        handle[_HNDL_LOCK] = self._manager.get_lock()
        handle[_HNDL_INPUT] = self._manager.get_queue()
        handle[_HNDL_OUTPUT] = self._manager.get_queue()
        handle[_HNDL_CLNTEVT] = self._manager.get_event()
        handle[_HNDL_SRVREVT] = self._manager.get_event()
        handle[_HNDL_ERROR] = self._manager.get_dict()
        # handle = {
        #     'lock': self._manager.get_lock(),
        #     'input': self._manager.get_queue(),
        #     'output': self._manager.get_queue(),
        #     'client': self._manager.get_event(),
        #     'server': self._manager.get_event(),
        #     'error': self._manager.get_dict(),
        # }
        handle['status'] = _STS_CREATING
        with self._jobslock:
            if self._prccount >= self._maxconn:
                self._log.warning("Лимит процессов исчерпан")
                return False    # Лимит процессов исчерпан
            try:
                handle['worker'] = self._worker(self._dbconn,
                                                self._prccount,
                                                handle[_HNDL_LOCK],
                                                handle[_HNDL_INPUT],
                                                handle[_HNDL_OUTPUT],
                                                handle[_HNDL_CLNTEVT],
                                                handle[_HNDL_SRVREVT],
                                                handle[_HNDL_ERROR])
            except Exception as e:
                errmsg = "[%d] error:%s" % (self._prccount, e.message)
                self._log.error("[%d]: status set to %s" % (self._prccount, handle['status']))
                raise RuntimeError(errmsg)
            handle['status'] = _STS_WORKER
            try:
                handle[_HNDL_PROXY] = self._proxy(self._prccount,
                                                  handle[_HNDL_LOCK],
                                                  handle[_HNDL_INPUT],
                                                  handle[_HNDL_OUTPUT],
                                                  handle[_HNDL_CLNTEVT],
                                                  handle[_HNDL_SRVREVT],
                                                  handle[_HNDL_ERROR])
            except Exception as e:
                errmsg = "[%d] error:%s" % (self._prccount, e.message)
                self._log.error("[%d]: status set to %s" % (self._prccount, handle['status']))
                raise RuntimeError(errmsg)
            handle['status'] = _STS_PROXY
            self._handles.append(handle)
            handle['status'] = _STS_CREATED
            handle['worker'].start()
            handle['status'] = _STS_STARTED
            self._freelist.put(self._prccount)
            handle['status'] = _STS_READY
            self._prccount += 1

    def connect(self, timeout=None):
        """
        Get connection to DBPool as object DBProxyBase
        :param timeout: in seconds, None equals nowait
        :return: DBProxyBase
        """
        timedout = False
        while self._working and not timedout:  # Если не shutdown и если не истекло время
            # Найти свободный процесс и занять его
            try:
                nprc = self._freelist.get(timeout=timeout)
            except Queue.Empty:
                timedout = True
                self._append()  # Ответ не важен потому, что дальше будет get_nowait
                try:
                    nprc = self._freelist.get_nowait()
                except Queue.Empty:
                    return None
            # Сюда можно попасть только с nprc is not None
            assert nprc is not None, "nprc cannot be None. Check all returns!"
            # Поэтому дальше работаем с реестром и под блокировкой
            with self._jobslock:
                if self._handles[nprc][_HNDL_STATUS] == _STS_READY:
                    self._handles[nprc][_HNDL_STATUS] = _STS_BUSY
                    return self._handles[nprc][_HNDL_PROXY]
                else:
                    # Логируем рассинхронизированность и делаем следующую попытку
                    self._log.warning("Bad handle[%d] from freelist has status='%s'"
                                      % (nprc, self._handles[nprc][_HNDL_STATUS]))
        return None

    def disconnect(self, proxy):
        nprc = proxy.name
        with self._jobslock:
            self._handles[nprc][_HNDL_STATUS] = _STS_READY
        self._freelist.put(nprc)

    def shutdown(self, immediate=False):
        self._working = False   # Запрещаем новые коннекты
        for handle in self._handles:
            handle[_HNDL_STATUS] = _STS_STOPPING
            self.disconnect(handle[_HNDL_PROXY])
            del handle[_HNDL_PROXY]
            handle[_HNDL_WORKER].shutdown(immediate)
            del handle[_HNDL_WORKER]
            handle[_HNDL_STATUS] = _STS_STOPPED
        self._manager.shutdown()


class MPManager(object):
    def __init__(self):
        self._manager_ = multiprocessing.Manager()

    @staticmethod
    def get_lock():
        return multiprocessing.Lock()

    def get_queue(self):
        return self._manager_.Queue()

    def get_event(self):
        return self._manager_.Event()

    def get_dict(self):
        return self._manager_.dict()

    def shutdown(self):
        self._manager_.shutdown()


class MTManager(object):

    @staticmethod
    def get_lock():
        return threading.Lock()

    @staticmethod
    def get_queue():
        return Queue.Queue()

    @staticmethod
    def get_event():
        return threading.Event()

    @staticmethod
    def get_dict():
        return {}

    @staticmethod
    def shutdown():
        pass


class DBPoolMP(DBPoolBase):
    def __init__(self, dbconn, cls_proxy, cls_worker, minconn=None, maxconn=None):
        super(DBPoolMP, self).__init__(dbconn, MPManager, cls_proxy, cls_worker, minconn, maxconn)


class DBPoolMT(DBPoolBase):
    def __init__(self, dbconn, cls_proxy, cls_worker, minconn=None, maxconn=None):
        super(DBPoolMT, self).__init__(dbconn, MTManager, cls_proxy, cls_worker, minconn, maxconn)
