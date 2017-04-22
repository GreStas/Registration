#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from time import sleep
import SocketServer

try:
    from config import Config
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()

cfg = Config(
    [
        {'name': 'pghost', 'section': 'POSTGRESQL', },
        {'name': 'pgdb', 'section': 'POSTGRESQL', },
        {'name': 'pguser', 'section': 'POSTGRESQL', },
        {'name': 'pgpasswd', 'section': 'POSTGRESQL', },
        {'name': 'pgrole', 'section': 'POSTGRESQL', },
        {'name': 'pgschema', 'section': 'POSTGRESQL', },
        {'name': 'loglevel', 'section': 'DEBUG', },
        {'name': 'logfile', 'default': 'socksrvr.log', 'section': 'DEBUG', },
        {'name': 'srvrhost', 'section': 'SOCKSRVR', },
        {'name': 'srvrport', 'section': 'SOCKSRVR', },
        # {'name': 'loglevel', 'default': 'DEBUG', 'section': 'DEBUG', },
        # {'name': 'duration', 'section': 'DEFAULT', },
        # {'name': 'freq', 'section': 'DEFAULT', },
        # {'name': 'pcterr', 'section': 'DEFAULT', },
    ],
    filename="stresstest.conf",
)
defconnection = {}
defconnection["pg_hostname"]    = cfg.options['pghost']
defconnection["pg_database"]    = cfg.options['pgdb']
defconnection["pg_user"]        = cfg.options['pguser']
defconnection["pg_passwd"]      = cfg.options['pgpasswd']
defconnection["pg_role"]        = cfg.options['pgrole']
defconnection["pg_schema"]      = cfg.options['pgschema']
loglevel    = cfg.options['loglevel']
logfile     = cfg.options['logfile']
srvrhost    = cfg.options['srvrhost']
srvrport    = int(cfg.options['srvrport'])
del cfg

print "Started for:", defconnection
print "Logfile:", logfile
print "Logging level:", loglevel

# class _Null(object):
#     """ Класс _Null необходим для маскировки при пустом логировании """
#     def __init__(self, *args, **kwargs): pass
#     def __call__(self, *args, **kwargs): return self
#     def __getattribute__(self, name): return self
#     def __setattr__(self, name, value): pass
#     def __delattr__(self, name): pass

import logging
logging_level = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}
logging.basicConfig(
    filename=logfile,
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging_level.get(loglevel, logging.NOTSET)
)
# _log = _Null()  # резервируем переменную модуля для логирования
_log = logging.getLogger("RegSockSrvr")
_log.debug("Started")

try:
    from Registration import getRegWorker
    import appproto
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()


class Error(RuntimeError):
    pass


class RegHandler(SocketServer.StreamRequestHandler):

    reg_worker = getRegWorker(defconnection, 1, 16)

    def handle(self):
        proto = appproto.AppProto(self.request)

        def Garbage(data):
            RegHandler.reg_worker.Garbage(data['timealive'])
            proto.send_SUCCESS()

        def SendMail(data):
            result = RegHandler.reg_worker.SendMail(
                request_id=data['request_id'],
                logname=data['logname'],
                alias=data['alias'],
                authcode=data['authcode'],
            )
            if result < 0:
                raise Error(RegHandler.reg_worker.ErrMsgs[result])
            proto.send_SUCCESS()

        def SaveRequest(data):
            result = RegHandler.reg_worker.SaveRequest(
                logname=data['logname'],
                alias=data['alias'],
                passwd=data['passwd'],
            )
            if result < 0:
                raise Error(RegHandler.reg_worker.ErrMsgs[result])
            proto.send_SUCCESS(result)

        def RegAprove(data):
            result = RegHandler.reg_worker.RegApprove(
                authcode=data['authcode'],
            )
            if result < 0:
                raise Error(RegHandler.reg_worker.ErrMsgs[result])
            proto.send_SUCCESS()

        def Gather(data):
            rows = RegHandler.reg_worker.Gather(
                fields=data['fields'],
                limit=data['limit'],
            )
            if rows is None or len(rows) == 0:
                proto.send_answer(appproto.APP_PROTO_SIG_NO, 0)
            elif proto.recv_signal(appproto.APP_PROTO_SIG_OK):  # Если Клиент готов принимать данные
                proto.send_cmnd(datagramma['cmnd'], rows)

        while True:
            datagramma = proto.recv_cmnd()
            if not datagramma:
                break
            try:
                if datagramma['cmnd'] == 'SaveRequest': SaveRequest(datagramma['data'])
                elif datagramma['cmnd'] == 'RegApprove': RegAprove(datagramma['data'])
                elif datagramma['cmnd'] == 'SendMail': SendMail(datagramma['data'])
                elif datagramma['cmnd'] == 'Garbage': Garbage(datagramma['data'])
                elif datagramma['cmnd'] == 'Gather': Gather(datagramma['data'])
                else:
                    raise Error("Unknown command: %s" % datagramma['cmnd'])
            except Error as e:
                _log.error(e.message)
                proto.send_answer(appproto.APP_PROTO_SIG_ER,
                                  e.message)
        self.request.close()


class RegServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True


_log.info("Socket server started in %s:%d" % (srvrhost, srvrport))
srvr = RegServer((srvrhost, srvrport), RegHandler)
srvr.serve_forever()
sleep(1)
RegHandler.reg_worker.closeDBpool()
_log.info("Socket server finished.")
