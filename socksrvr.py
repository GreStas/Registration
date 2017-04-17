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
        {'name': 'loglevel', 'default': 'DEBUG', 'section': 'DEBUG', },
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
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()

# wrkr = getRegWorker(defconnection,1,16)

class RegHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        while True:
            data = json.loads(self.request.recv(1024))
            if __debug__: print data
            if data is None:
                break
            if data['cmnd'] == 'upper':
                data['data']['alias'] = data['data']['alias'].upper()
                self.request.send(json.dumps({
                    'answ': 'success',
                    'data': data['data'],
                }))
            elif lst[0] == 'lower':
                data['data']['alias'] = data['data']['alias'].lower()
                self.request.send(json.dumps({
                    'answ': 'success',
                    'data': data['data'],
                }))
            else:
                self.request.send(json.dumps({
                    'answ': 'Error',
                    'mesg': "Unknown command: %s" % lst[0],
                }))

class RegServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

srvr = RegServer(('', 9090), RegHandler)
srvr.serve_forever()

sleep(1)
# wrkr.closeDBpool()
_log.debug("Finished")
