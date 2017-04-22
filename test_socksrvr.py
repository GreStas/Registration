#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Полный цикл обработки запроса на регистрацию:
# 0. SendMails
# 1. SaveRequest
# 2. Gather
# 3. RegApprove
# 5. Garbage

# Import standart modules
# from time import sleep
import sys
from random import randint
import datetime

# Import 3rd-parties modules
try:
    from faker import Factory
    from faker.config import AVAILABLE_LOCALES
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
        {'name': 'loglevel', 'default': 'DEBUG', 'section': 'DEBUG', },
        {'name': 'logfile', 'default': 'socksrvr.log', 'section': 'DEBUG', },
        {'name': 'srvrhost', 'section': 'REGCLIENT', },
        {'name': 'srvrport', 'section': 'REGCLIENT', },
    ],
    filename="stresstest.conf",
)
loglevel = cfg.options['loglevel']
logfile = cfg.options['logfile']
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
_log = logging.getLogger("TestSockSrvr")
_log.debug("Started")

# Import my modules
try:
    import regclient
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()

###
#   MAIN
###

client = regclient.RegClient(srvrhost, srvrport)
print datetime.datetime.now()
rows = client.get_not_sent(1)
print "Getting %d rows from Registration for send_mail()" % len(rows)
for row in rows:
    if __debug__: print row
    request_id, logname, alias, authcode, status = row
    try:
        client.SendMail(request_id, logname, alias, authcode)
    except regclient.Error as e:
        _log.error("SendMail is unsuccessfull: %s" % e.message)
        continue
print datetime.datetime.now()

fakers = [Factory.create(lcl) for lcl in AVAILABLE_LOCALES]
# fakers.append(Factory.create("en_US"))
# fakers.append(Factory.create("ru_RU"))
fakers_max = len(fakers) - 1


print "Main cicle started at ", datetime.datetime.now()
for ii in xrange(len(fakers)):
    if __debug__: print '\n<--- Started ---\n'

    try:
        request_id = client.SaveRequest(fakers[randint(0, fakers_max)])
    except regclient.Error as e:
        _log.error("SaveRequest is unsuccessfull: %s" % e.message)
        continue
    if __debug__: print "SaveRequest is successfull"

    # try:
    #     authcode = client.get_authcode(request_id)
    # except regclient.Error as e:
    #     _log.error("GetAuthcode is unsuccessfull: %s" % e.message)
    #     continue
    # if __debug__: print "GetAuthcode is successfull"
    #
    # try:
    #     client.RegApprove(authcode)
    # except regclient.Error as e:
    #     _log.error("RegApprove is unsuccessfull: %s" % e.message)
    #     continue
    # if __debug__: print "RegApprove is successfull"

    if __debug__: print '\n--- Finished --->\n'
    s = raw_input('Press any key to continue...')
print "Main cicle finished at ", datetime.datetime.now()
_log.debug("Finished.")
sys.exit(0)

print datetime.datetime.now()
client.Garbage(60*60*24*10)
print datetime.datetime.now()

_log.debug("Finished.")
