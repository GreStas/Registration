#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
# from time import sleep
import SocketServer
# Import my modules
from config import Config

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
_log = logging.getLogger("RegSockSrvr")
_log.info("Started")

from Registration import getRegWorker
from regsrvrproto import RegServerProto, Error as RegProtoError


class Error(RuntimeError):
    pass


regworker = getRegWorker(defconnection, 1, 16)


class RegHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        try:
            RegServerProto(regworker, self.request)
        except RegProtoError as e:
            _log.error("Registration error: '%s'" % e.message)
        except RuntimeError as e:
            _log.error("Runtime Error: '%s'" % e.message)


class RegServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True


print "Started for:", defconnection
print "Logfile:", logfile
print "Logging level:", loglevel

_log.info("Socket server started in %s:%d" % (srvrhost, srvrport))
srvr = RegServer((srvrhost, srvrport), RegHandler)
srvr.serve_forever()
# sleep(1)
_log.info("Socket server finished.")
