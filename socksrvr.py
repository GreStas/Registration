#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import SocketServer
from config import Config
from registration import getRegWorker
from registration.regsrvrproto import RegServerProto, Error as RegProtoError

cfg = Config(
        [(("", "--inifile"), {'action': 'store', 'type': 'string', 'dest': 'inifile', 'default': 'stresstest.ini'}),
         (('', '--pghost'), {'action': 'store', 'type': 'string', 'dest': 'pghost'}),
         (('', '--pgdb'), {'action': 'store', 'type': 'string', 'dest': 'pgdb'}),
         (('', '--pguser'), {'action': 'store', 'type': 'string', 'dest': 'pguser'}),
         (('', '--pgpasswd'), {'action': 'store', 'type': 'string', 'dest': 'pgpasswd'}),
         (('', '--pgschema'), {'action': 'store', 'type': 'string', 'dest': 'pgschema'}),
         (('', '--loglevel'), {'action': 'store', 'type': 'string', 'dest': 'loglevel'}),
         (('', '--logfile'), {'action': 'store', 'type': 'string', 'dest': 'logfile'}),
         (('', '--srvrhost'), {'action': 'store', 'type': 'string', 'dest': 'srvrhost'}),
         (('', '--srvrport'), {'action': 'store', 'type': 'string', 'dest': 'srvrport'}),
         (("", "--dbpoolmin"), {'action': 'store', 'type': 'string', 'dest': "dbpoolmin"}),
         (("", "--dbpoolmax"), {'action': 'store', 'type': 'string', 'dest': "dbpoolmax"}),
         (("", "--pooltype"), {'action': 'store', 'type': 'string', 'dest': "pooltype"}),
         # (("-", "--"), {'action': 'store', 'type': 'string', 'dest': "", 'default':''}),
         ],
        prefer_opt=True,
        version='0.0.0.1',
    )
inifile = cfg.get('inifile')
print "INI-file:", inifile
if inifile is not None:
    cfg.load_conf(inifile)

defconnection = {
    "pg_hostname": cfg.get('pghost', 'POSTGRESQL', default='localhost'),
    "pg_database": cfg.get('pgdb', 'POSTGRESQL'),
    "pg_user": cfg.get('pguser', 'POSTGRESQL'),
    "pg_passwd": cfg.get('pgpasswd', 'POSTGRESQL'),
    "pg_role": cfg.get('pgrole', 'POSTGRESQL'),
    "pg_schema": cfg.get('pgschema', 'POSTGRESQL'),
}
loglevel = cfg.get('loglevel', section='DEBUG', default='CRITICAL')
logfile = cfg.get('logfile', section='DEBUG', default='logs/socksrvr.log')
srvrhost = cfg.get('srvrhost', 'SOCKSRVR', default='localhost')
srvrport = int(cfg.get('srvrport', 'SOCKSRVR', default='9090'))
dbpoolmin = int(cfg.get('dbpoolmin', 'SOCKSRVR', default='1'))
dbpoolmax = int(cfg.get('dbpoolmax', 'SOCKSRVR', default='16'))
pooltype = cfg.get('pooltype', section='SOCKSRVR', default='mt')
del cfg

print "Started for:", defconnection
print "on %s:%s" % (srvrhost, srvrport)
print "DBpool(Min,Max) = %s(%d,%d)" % (pooltype, dbpoolmin, dbpoolmax)
print "Logfile:", logfile
print "Logging level:", loglevel

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

regworker = getRegWorker(defconnection, dbpoolmin, dbpoolmax, pooltype)


class Error(RuntimeError):
    pass


class RegHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        try:
            RegServerProto(regworker, self.request)
        except RegProtoError as e:
            _log.error("Registration error: '%s'" % e.message)
        except RuntimeError as e:
            _log.error("Runtime Error: '%s'" % e.message)

if pooltype == 'mt':
    SockSrvrClass = SocketServer.ThreadingTCPServer
elif pooltype == 'mp':
    SockSrvrClass = SocketServer.ForkingTCPServer


class RegServer(SockSrvrClass):
    allow_reuse_address = True
    # daemon_threads = True
    # request_queue_size = 5

srvr = RegServer((srvrhost, srvrport), RegHandler)

# def dedicate_srvr():
if __name__ == '__main__':
    _log.info("Socket server started in %s:%d" % (srvrhost, srvrport))
    srvr.serve_forever()
    # sleep(1)
    _log.info("Socket server finished.")