#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from time import sleep
import SocketServer
import json

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
        # {'name': 'loglevel', 'default': 'DEBUG', 'section': 'DEBUG', },
        {'name': 'loglevel', 'section': 'DEBUG', },
        {'name': 'logfile', 'default': 'socksrvr.log', 'section': 'DEBUG', },
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
loglevel   = cfg.options['loglevel']
logfile   = cfg.options['logfile']
del cfg

print "Started for:", defconnection
print "Logfile:", logfile
print "Logging level:", loglevel

class _Null(object):
    """ Класс _Null необходим для маскировки при пустом логировании """
    def __init__(self, *args, **kwargs): pass

    def __call__(self, *args, **kwargs): return self

    def __getattribute__(self, name): return self

    def __setattr__(self, name, value): pass

    def __delattr__(self, name): pass

import logging
logging_level = {
    'DEBUG':logging.DEBUG,
    'INFO':logging.INFO,
    'ERROR':logging.ERROR,
    'CRITICAL':logging.CRITICAL,
}
logging.basicConfig(
    filename=logfile,
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging_level.get(loglevel,logging.NOTSET)
)
_log = _Null()  # резервируем переменную модуля для логирования
_log = logging.getLogger("RegSockSrvr")
_log.debug("Started")

try:
    from Registration import getRegWorker
    import appproto
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()


reg_worker = getRegWorker(defconnection, 1, 16)


class RegWorkerError(Exception):
    pass


class RegHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        proto = appproto.AppProto(self.request)
        while True:
            datagramma = proto.recv_cmnd()
            if not datagramma:
                break
            try:
                if datagramma['cmnd'] == 'SaveRequest':
                    result = reg_worker.SaveRequest(
                        logname=datagramma['data']['logname'],
                        alias=datagramma['data']['alias'],
                        passwd=datagramma['data']['passwd'],
                    )
                    if result < 0:
                        proto.send_ERROR(reg_worker.ErrMsgs[result])
                        raise RegWorkerError(reg_worker.ErrMsgs[result])
                    else:
                        proto.send_cmnd(
                            'success',
                            {'answ': 'success', 'data': result, },
                        )
                elif datagramma['cmnd'] == 'RegApprove':
                    result = reg_worker.RegApprove(
                        authcode=datagramma['data']['authcode'],
                    )
                    if result < 0:
                        proto.send_ERROR(reg_worker.ErrMsgs[result])
                        raise RegWorkerError(reg_worker.ErrMsgs[result])
                    else:
                        proto.send_cmnd('success')
                elif datagramma['cmnd'] == 'SendMail':
                    result = reg_worker.SendMail(
                        request_id=datagramma['data']['request_id'],
                        logname=datagramma['data']['logname'],
                        alias=datagramma['data']['alias'],
                        authcode=datagramma['data']['authcode'],
                    )
                    if result < 0:
                        proto.send_ERROR(reg_worker.ErrMsgs[result])
                        raise RegWorkerError(reg_worker.ErrMsgs[result])
                    else:
                        proto.send_cmnd('success')
                elif datagramma['cmnd'] == 'Garbage':
                    reg_worker.Garbage(timealive=datagramma['data']['timealive'],)
                    proto.send_cmnd('success')
                elif datagramma['cmnd'] == 'Gather':
                    rows = reg_worker.Gather(
                        fields=datagramma['data']['fields'],
                        limit=datagramma['data']['limit'],
                    )
                    proto.send_SUCCESS(len(rows))
                    if proto.recv_OK():  # Если Клиент готов принимать данные
                        proto.send_cmnd(datagramma['cmnd'], rows)
                    del rows
                else:
                    proto.send_ERROR("Unknown command: %s" % datagramma['cmnd'])
            except RegWorkerError as e:
                proto.send_ERROR(e.message)
        self.request.close()


class RegServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True


srvr = RegServer(('', 9090), RegHandler)
srvr.serve_forever()

sleep(1)
reg_worker.closeDBpool()
_log.debug("Finished")
