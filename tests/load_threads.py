#!/usr/bin/python
# -*- coding: utf-8 -*-
#

# from random import randint
# import sys
import datetime
import logging
import threading
from time import sleep

from faker import Factory
from faker.config import AVAILABLE_LOCALES

from config import Config

cfg = Config(
        [(("", "--inifile"), {'action': 'store', 'type': 'string', 'dest': 'inifile', 'default': 'stresstest.ini'}),
         (('', '--loglevel'), {'action': 'store', 'type': 'string', 'dest': 'loglevel'}),
         (('', '--logfile'), {'action': 'store', 'type': 'string', 'dest': 'logfile'}),
         (('', '--srvrhost'), {'action': 'store', 'type': 'string', 'dest': 'srvrhost'}),
         (('', '--srvrport'), {'action': 'store', 'type': 'string', 'dest': 'srvrport'}),
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

loglevel    = cfg.get('loglevel', section='DEBUG', default='CRITICAL')
logfile     = cfg.get('logfile', section='DEBUG', default='logs/stress_threads.log')
srvrhost    = cfg.get('srvrhost', 'REGCLIENT', default='localhost')
srvrport    = int(cfg.get('srvrport', 'REGCLIENT', default='9090'))
iterations  = int(cfg.get('iterations', default='1'))
maxqueue    = int(cfg.get('maxqueue', default='100'))
del cfg

print "Started for: %s:%s" % (srvrhost, srvrport)
print "Logfile:", logfile
print "Logging level:", loglevel
print "Iterations:", iterations

logging_level = {'DEBUG': logging.DEBUG,
                 'INFO': logging.INFO,
                 'ERROR': logging.ERROR,
                 'CRITICAL': logging.CRITICAL, }
logging.basicConfig(
    filename=logfile,
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging_level.get(loglevel, logging.NOTSET)
)
_log = logging.getLogger("TestSockSrvr")
_log.info("Started")


from registration.regclntproto import RegClientProto, Error as RegProtoError


class RegClient(RegClientProto):
    def get_authcode(self, request_id):
        data = self.gather(
            fields=[('authcode', None),
                    ('id', "=%d" % request_id)],
            limit=1,
        )
        size = len(data)
        if size == 0:
            raise RegProtoError("No data found")
        elif size > 1:
            raise RegProtoError("Too many rows")
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

def gen_request(fake):
    logname = fake.email()
    alias = fake.name()
    passwd = fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True)
    with cmnds_lock:
        cmnds.append((save_request, (logname, alias, passwd), {}))

def save_request(*args, **kwargs):
    try:
        client = RegClient(srvrhost, srvrport)
        request_id = client.save_request(*args, **kwargs)
        if __debug__:
            _log.debug('Request_id=%d' % request_id)
        with cmnds_lock:
            cmnds.append((get_authcode, (request_id), {}))
    except RegProtoError as e:
        _log.error("SaveRequest is unsuccessfull: %s" % e.message)

def get_authcode(request_id):
    try:
        client = RegClient(srvrhost, srvrport)
        authcode = client.get_authcode(request_id)
        if __debug__:
            _log.debug('Authcode(%d)=%s' % (request_id, authcode))
    except RegProtoError as e:
        _log.error("GetAuthcode is unsuccessfull: %s" % e.message)

    try:
        client = RegClient(srvrhost, srvrport)
        with cmnds_lock:
            cmnds.append((get_authcode, (request_id), {}))
    except RegProtoError as e:

def approve(authcode):
    pass

print "Started for server %s:%s" % (srvrhost, srvrport)
print "Logfile:", logfile
print "Logging level:", loglevel

fakers = [Factory.create(lcl) for lcl in AVAILABLE_LOCALES]
fakers_max = len(fakers) - 1
cmnds = []
cmnds_lock = threading.Lock()

print "Main cicle started at ", datetime.datetime.now()
for i in xrange(iterations):
    if len(cmnds) >= maxqueue:
        sleep(1)
    fake = fakers[i % fakers_max]
    with cmnds_lock:
        cmnds.append((gen_request, (fake), {}))
print "Main cicle finished at ", datetime.datetime.now()

_log.info("Finished.")
