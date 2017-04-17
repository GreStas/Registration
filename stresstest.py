#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Using:
#   "Сокеты в Python для начинающих" URL=https://habrahabr.ru/post/149077/
#

# Import standart modules
import sys
from time import sleep
from random import randint
import json
from future_builtins import ascii
import socket
import datetime

# Import 3rd-parties modules
try:
    from faker import Factory
except ImportError, info:
    print "Import Error:", info
    sys.exit()

# Import my modules
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
defconnection = {
    "pg_hostname":  cfg.options['pghost'],
    "pg_database":  cfg.options['pgdb'],
    "pg_user":      cfg.options['pguser'],
    "pg_passwd":    cfg.options['pgpasswd'],
    "pg_role":      cfg.options['pgrole'],
    "pg_schema":    cfg.options['pgschema'],
}
loglevel   = cfg.options['loglevel']
logfile   = cfg.options['logfile']
del cfg

# print "Started for:", defconnection
# print "Logfile:", logfile
# print "Logging level:", loglevel
# print "time_duration=", time_duration
# print "q_per_minutes=", q_per_minutes
# print "pct_userexists=", pct_userexists

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
_log = logging.getLogger("RegStressTest")
_log.debug("Started")

# wrkr = getRegWorker(defconnection,1,16)

fakers = []
# fakers.append(Factory.create("en_US"))
fakers.append(Factory.create("ru_RU"))
fakers_max = len(fakers) - 1

sock = socket.socket()
sock.connect(('', 9090))

print datetime.datetime.now()
for ii in xrange(10000):
    fake = fakers[randint(0,fakers_max)]
    logname = fake.email()
    passwd = fake.password(
        length=10,
        special_chars=True,
        digits=True,
        upper_case=True,
        lower_case=True
    )
    alias = fake.name()
    if __debug__: print '\n<--- Generated:\n  %s\n  %s\n  %s\n' % (logname, passwd, alias)
    data = {
        'logname': logname,
        'passwd': passwd,
        'alias': alias,
    }
    send_data = json.dumps({
        'cmnd': 'upper',
        'data': data,
    })
    if __debug__: print 'Sent:\n',send_data, '\n'
    sock.send(send_data)

    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        print "%s: '%s'" % (recieve_data['answ'], recieve_data['mesg'])
        continue

    data = recieve_data['data']
    if __debug__: print 'Recieved:\n', data, '\n'
    encode_data = {}
    for (key,value) in data.items():
        ekey = key.encode('utf-8')
        evalue = value.encode('utf-8')
        if __debug__: print '(ekey:evalue) = (%s:%s)' % (ekey, evalue)
        encode_data[ekey] = evalue
    if __debug__:
        print '\nEncoded:\n'
        for key in encode_data:
            print key, encode_data[key]
    if __debug__: print '\nFinished --->\n'
    # s = raw_input('Press any key to continue...')

sock.send(json.dumps(None))
sock.close()

# sleep(1)
# wrkr.closeDBpool()
_log.debug("Finished")
print datetime.datetime.now()