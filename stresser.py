#!/usr/bin/python
# -*- coding: utf-8 -*-

from time import sleep
import logging
import multiprocessing
import signal
from config import Config


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
logfile = cfg.get('logfile', section='DEBUG', default='logs/stresser.log')
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
_log = logging.getLogger("stresser")
_log.info("Started")

def controller(name, ctrl_evt):
    _log.info("Started #%s" % name)
    while True:
        _log.debug("Begin #%s iteration" % name)
        sleep(10)
        _log.debug("End #%s iteration" % name)
        if not ctrl_evt.is_set():
            break
    _log.info("Finished #%s" % name)


prc_count = multiprocessing.cpu_count()
print "Create %d processes" % prc_count
p_list = []
for i in range(prc_count):
    is_working = multiprocessing.Event()
    is_working.clear()
    prc = multiprocessing.Process(target=controller, args=(str(i), is_working,))
    prc.daemon = True
    p_list.append({
        'is_working': is_working,
        'prc': prc,
    })
    prc.start()

def alrm_handler(signum, frame):
    _log.info("SIGALRM")
    for descriptor in p_list:
        descriptor['is_working'].clear()

def term_handler(signum, frame):
    _log.info("SIGTERM")
    for descriptor in p_list:
        descriptor['prc'].terminate()

signal.signal(signal.SIGALRM, alrm_handler)
signal.signal(signal.SIGTERM, term_handler)

print "Send is_working event"
for descriptor in p_list:
    descriptor['is_working'].set()
    sleep(1)


print "Join all processes"
for descriptor in p_list:
    descriptor['prc'].join()

print "Finished."
