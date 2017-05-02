#!/usr/bin/python
# -*- coding: utf-8 -*-
#

# from time import sleep
# from random import randint
# import sys
import datetime
from faker import Factory
from faker.config import AVAILABLE_LOCALES
import logging
from config import Config

cfg = Config(
        [(("", "--inifile"), {'action': 'store', 'type': 'string', 'dest': 'inifile', 'default': 'stresstest.ini'}),
         (('', '--loglevel'), {'action': 'store', 'type': 'string', 'dest': 'loglevel'}),
         (('', '--logfile'), {'action': 'store', 'type': 'string', 'dest': 'logfile'}),
         (('', '--srvrhost'), {'action': 'store', 'type': 'string', 'dest': 'srvrhost'}),
         (('', '--srvrport'), {'action': 'store', 'type': 'string', 'dest': 'srvrport'}),
         (("-i", "--iters"), {'action': 'store', 'type': 'string', 'dest': "iterations"}),
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
logfile     = cfg.get('logfile', section='DEBUG', default='stresstest.log')
srvrhost    = cfg.get('srvrhost', 'REGCLIENT', default='localhost')
srvrport    = int(cfg.get('srvrport', 'REGCLIENT', default='9090'))
iterations  = int(cfg.get('iterations', default='1'))
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


from regclntproto import RegClientProto, Error as RegProtoError


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


print "Started for server %s:%s" % (srvrhost, srvrport)
print "Logfile:", logfile
print "Logging level:", loglevel


cmnds = []


_log.info("Finished.")
