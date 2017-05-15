#!/usr/bin/python
# -*- coding: utf-8 -*-
#

import threading
from time import sleep
import datetime
from faker import Factory
from faker.config import AVAILABLE_LOCALES
import logging
from config import Config
import Registration
from dbpool.pgdbpool import PGDBPoolMP, PGDBPoolMT


cfg = Config(
        [(("", "--inifile"), {'action': 'store', 'type': 'string', 'dest': 'inifile', 'default': 'stresstest.ini'}),
         (('', '--pghost'), {'action': 'store', 'type': 'string', 'dest': 'pghost'}),
         (('', '--pgdb'), {'action': 'store', 'type': 'string', 'dest': 'pgdb'}),
         (('', '--pguser'), {'action': 'store', 'type': 'string', 'dest': 'pguser'}),
         (('', '--pgpasswd'), {'action': 'store', 'type': 'string', 'dest': 'pgpasswd'}),
         (('', '--pgschema'), {'action': 'store', 'type': 'string', 'dest': 'pgschema'}),
         (("", "--dbpoolmin"), {'action': 'store', 'type': 'string', 'dest': "dbpoolmin"}),
         (("", "--dbpoolmax"), {'action': 'store', 'type': 'string', 'dest': "dbpoolmax"}),
         (("", "--pooltype"), {'action': 'store', 'type': 'string', 'dest': "pooltype"}),
         (('', '--loglevel'), {'action': 'store', 'type': 'string', 'dest': 'loglevel'}),
         (('', '--logfile'), {'action': 'store', 'type': 'string', 'dest': 'logfile'}),
         (("-i", "--iters"), {'action': 'store', 'type': 'string', 'dest': "iterations"}),
         (("", "--maxqueue"), {'action': 'store', 'type': 'string', 'dest': "maxqueue"}),
         # (("-", "--"), {'action': 'store', 'type': 'string', 'dest': "", 'default': ''}),
         ],
        prefer_opt=True,
        version='0.0.0.1',
    )
inifile = cfg.get('inifile')
print "INI-file:", inifile
if inifile is not None:
    cfg.load_conf(inifile)

defconnection = {}
defconnection["pg_hostname"]    = cfg.get('pghost', 'POSTGRESQL', default='localhost')
defconnection["pg_database"]    = cfg.get('pgdb', 'POSTGRESQL')
defconnection["pg_user"]        = cfg.get('pguser', 'POSTGRESQL')
defconnection["pg_passwd"]      = cfg.get('pgpasswd', 'POSTGRESQL')
defconnection["pg_role"]        = cfg.get('pgrole', 'POSTGRESQL')
defconnection["pg_schema"]      = cfg.get('pgschema', 'POSTGRESQL')
dbpoolmin   = int(cfg.get('dbpoolmin', 'SOCKSRVR', default='1'))
dbpoolmax   = int(cfg.get('dbpoolmax', 'SOCKSRVR', default='16'))
pooltype    = cfg.get('pooltype', section='SOCKSRVR', default='mt')
loglevel    = cfg.get('loglevel', section='DEBUG', default='CRITICAL')
logfile     = cfg.get('logfile', section='DEBUG', default='logs/stress_threads.log')
iterations  = int(cfg.get('iterations', default='1'))
maxqueue    = int(cfg.get('maxqueue', default='100'))
del cfg

if pooltype == 'mt':
    dbpool = PGDBPoolMT(defconnection, dbpoolmin, dbpoolmax)
elif pooltype == 'mp':
    dbpool = PGDBPoolMP(defconnection, dbpoolmin, dbpoolmax)

print "Started for: %s:%"
print "Logfile:", logfile
print "Logging level:", loglevel
print "Iterations:", iterations
print "maxqueue:", maxqueue

logging_level = {'DEBUG': logging.DEBUG,
                 'INFO': logging.INFO,
                 'ERROR': logging.ERROR,
                 'CRITICAL': logging.CRITICAL, }
logging.basicConfig(
    filename=logfile,
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging_level.get(loglevel, logging.NOTSET)
)
_log = logging.getLogger("StressRegistration")
_log.info("Started")


class RegWorker(Registration.RegWorker):
    def get_authcode(self, request_id):
        data = self.gather(
            fields=[('authcode', None),
                    ('id', "=%d" % request_id)],
            limit=1,
        )
        size = len(data)
        if size == 0:
            raise RuntimeError("No data found")
        elif size > 1:
            raise RuntimeError("Too many rows")
        return data[0][0]   # нам нужно только первое поле

    def get_not_sent(self, cnt=None):
        return self.gather(
            fields=[('id', None),
                    ('logname', None),
                    ('alias', None),
                    ('authcode', None),
                    ('status', "='requested'")],
            limit=cnt
        )


class Stress(threading.Thread):

    prc_counts = 0
    lock = threading.Lock()

    @staticmethod
    def prc_inc():
        with Stress.lock:
            Stress.prc_counts += 1

    @staticmethod
    def prc_dec():
        with Stress.lock:
            Stress.prc_counts -= 1

    def __init__(self, fake):
        threading.Thread.__init__(self)
        self.daemon = True
        self.fake = fake

    def run(self):
        if __debug__:
            _log.debug("(%d)Started thread, counter=%d." % (self.ident, Stress.prc_counts))
        Stress.prc_inc()
        client = RegWorker(dbpool)
        try:
            logname = self.fake.email()
            alias = self.fake.name()
            passwd = self.fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True)
            if __debug__:
                _log.debug("(%d)Call SaveRequest(%s,%s)" % (self.ident, logname, alias))
            request_id = client.save_request(
                logname,
                alias,
                passwd,
            )
            if __debug__:
                _log.debug('(%d)Request_id=%d' % (self.ident, request_id))
        except Registration.Error as e:
            _log.error("(%d)SaveRequest is unsuccessfull: %s" % (self.ident, e.message))
            Stress.prc_dec()
            return
        try:
            authcode = client.get_authcode(request_id)
            if __debug__:
                _log.debug('(%d)Authcode(%d)=%s' % (self.ident, request_id, authcode))
        except RuntimeError as e:
            _log.error("(%d)GetAuthcode is unsuccessfull: %s" % (self.ident, e.message))
            Stress.prc_dec()
            return
        try:
            client.approve(authcode)
            if __debug__:
                _log.debug('(%d)Approve authcode(%d)=%s successfully' % (self.ident, request_id, authcode))
        except Registration.Error as e:
            _log.error("(%d)RegApprove is unsuccessfull: %s" % (self.ident, e.message))
            Stress.prc_dec()
            return
        Stress.prc_dec()
        if __debug__:
            _log.debug("(%d)Finished thread, counter=%d." % (self.ident, Stress.prc_counts))
        return

print "Started"
print "Logfile:", logfile
print "Logging level:", loglevel

fakers = [Factory.create(lcl) for lcl in AVAILABLE_LOCALES]
fakers_max = len(fakers) - 1
prc_count = 0

print "Main cicle started at ", datetime.datetime.now()
for i in xrange(iterations):
    if Stress.prc_counts >= maxqueue:
        sleep(1)
    t = Stress(fakers[i % fakers_max])
    t.start()
print "Main cicle finished at ", datetime.datetime.now()
while Stress.prc_counts > 0:
    print Stress.prc_counts
    sleep(1)
print "Waiting finished at ", datetime.datetime.now()

_log.info("Finished.")
# raw_input("Press Enter to finish...")