#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Полный цикл обработки запроса на регистрацию:
# 0. SendMails
# 1. SaveRequest
# 2. Gather
# 3. RegApprove
# 5. Garbage


# from time import sleep
# from random import randint
import sys
import datetime
from faker import Factory
from faker.config import AVAILABLE_LOCALES
import logging

from config import Config
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
        data = self.Gather(
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
        return self.Gather(
            fields=[('id', None),
                    ('logname', None),
                    ('alias', None),
                    ('authcode', None),
                    ('status', "='requested'")],
            limit=cnt
        )



###
#   MAIN
###



client = RegClient(srvrhost, srvrport)

fake = Factory.create("ru_RU")
request_id = client.SaveRequest(
    fake.email(),
    fake.name(),
    fake.password(length=10,
                  special_chars=True,
                  digits=True,
                  upper_case=True,
                  lower_case=True),
)
print "Test SaveRequest:", request_id

client = RegClient(srvrhost, srvrport)
authcode = client.get_authcode(request_id)
print 'Authcode(%d)=%s' % (request_id, authcode)

client = RegClient(srvrhost, srvrport)
client.RegApprove(authcode)
print 'Approve authcode(%d)=%s successfully' % (request_id, authcode)

client = RegClient(srvrhost, srvrport)
print datetime.datetime.now()
rows = client.get_not_sent()
print "Getting %d rows from Registration for send_mail()" % len(rows)
for row in rows:
    request_id, logname, alias, authcode, status = row
    try:
        client = RegClient(srvrhost, srvrport)
        client.SendMail(request_id, logname, alias, authcode)
    except RegClientProto as e:
       _log.error("SendMail is unsuccessfull: %s" % e.message)
       continue

print datetime.datetime.now()
client = RegClient(srvrhost, srvrport)
print client.Garbage(60*60*24*1)
print datetime.datetime.now()

_log.debug("Finished.")
sys.exit(0)



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

print datetime.datetime.now()
client.Garbage(60*60*24*10)
print datetime.datetime.now()

_log.debug("Finished.")
