#!/usr/bin/python
# -*- coding: utf-8 -*-

# Import standart modules
import sys
from time import sleep
from random import randint
import json
from future_builtins import ascii

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

for ii in xrange(1):
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
    print 'Generated:\n', logname, passwd, alias, '\n'
    send_data = json.dumps({
        'logname': logname,
        'passwd': passwd,
        'alias': alias,
    })
    print 'Sent:\n',send_data, '\n'
    recieve_data = json.loads(send_data)
    print 'Recieved:\n', recieve_data, '\n'
    encode_data = {}
    for (key,value) in recieve_data.items():
        ekey = key.encode('utf-8')
        evalue = value.encode('utf-8')
        print '(ekey:evalue) = (%s:%s)' % (ekey, evalue)
        encode_data[ekey] = evalue
    print '\nEncoded:\n'
    for key in encode_data:
        print key, encode_data[key]


# sleep(1)
# wrkr.closeDBpool()
_log.debug("Finished")
