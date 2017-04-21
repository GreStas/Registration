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
    import appproto
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()

cfg = Config(
    [
        {'name': 'loglevel', 'default': 'DEBUG', 'section': 'DEBUG', },
        {'name': 'logfile', 'default': 'socksrvr.log', 'section': 'DEBUG', },
    ],
    filename="stresstest.conf",
)
loglevel = cfg.options['loglevel']
logfile = cfg.options['logfile']
del cfg


class _Null(object):
    """ Класс _Null необходим для маскировки при пустом логировании """
    def __init__(self, *args, **kwargs): pass

    def __call__(self, *args, **kwargs): return self

    def __getattribute__(self, name): return self

    def __setattr__(self, name, value): pass

    def __delattr__(self, name): pass


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
    level=logging_level.get(loglevel,logging.NOTSET)
)
_log = _Null()  # резервируем переменную модуля для логирования
_log = logging.getLogger("TestSockSrvr")
_log.debug("Started")


class RegError(RuntimeError):
    pass



    class RegClient(object):
        def __init__(self, host, port):
            self._sock = socket.socket()
            self._sock.connect(host, port)
            self._proto = appproto.AppProto(self._sock)

        def save_request(self, fake):
            self._proto.send_cmnd(
                'SaveRequest',
                {'logname': fake.email(),
                 'passwd': fake.password(length=10,
                                         special_chars=True,
                                         digits=True,
                                         upper_case=True,
                                         lower_case=True),
                 'alias': fake.name(),},
            )
            try:
                data = self._proto.recv_answer()
            except appproto.Error as e:
                _log.error(e.message)
                raise RegError(e.message)
            return data

def get_authcode(request_id):
    # Prepare and send data
    data = {
        'fields': [
            ('authcode', None),
            ('id', "=%d" % request_id),
        ],
        'limit': 1,
    }
    send_data = json.dumps({'cmnd': 'Gather', 'data': data,})
    sock.send(send_data)
    #
    # Recieve Header of data
    raw_data = sock.recv(1024)
    recieve_data = json.loads(raw_data)
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])
    sock.send('OK')
    size = recieve_data['size']
    #
    # Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(size)) # это будет список из одного элемента - кортежа из двух полей
    return recieve_data[0][0]   # нам нужно только первое поле

def reg_approve(authcode):
    #
    # Prepare and send data
    data = {
        'authcode': authcode,
    }
    send_data = json.dumps({'cmnd': 'RegApprove', 'data': data,})
    sock.send(send_data)
    #
    #  Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])

def reg_garbage(timealive):
    #
    # Prepare and send data
    data = {
        'timealive': timealive,
    }
    send_data = json.dumps({'cmnd': 'Garbage', 'data': data,})
    sock.send(send_data)
    #
    #  Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])

def get_not_send():
    # Prepare and send data
    data = {
        'fields': [
            ('id', None),
            ('logname', None),
            ('alias', None),
            ('authcode', None),
            ('status', "='requested'")
        ],
        'limit': 0,
    }
    send_data = json.dumps({'cmnd': 'Gather', 'data': data,})
    sock.send(send_data)
    #
    # Recieve Header of data
    raw_data = sock.recv(1024)
    recieve_data = json.loads(raw_data)
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])
    sock.send('OK')
    size = recieve_data['size']
    if __debug__: print "Size is %d" % size
    #
    #  Recieve data and prepare to return
    json_data = ''
    while size > 0:
        raw_data = sock.recv(1024)
        json_data += raw_data
        size -= len(raw_data)
    if __debug__: print "len(json_data)=%d" % len(json_data)
    recieve_data = json.loads(json_data)
    return recieve_data

def send_mail(request_id, logname, alias, authcode):
    #
    # Prepare and send data
    data = {
        'request_id': request_id,
        'logname': logname,
        'alias': alias,
        'authcode': authcode
    }
    send_data = json.dumps({
        'cmnd': 'SendMail',
        'data': data,
    })
    sock.send(send_data)
    #
    # Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])

###
#   MAIN
###

sock = socket.socket()
sock.connect(('', 9090))

print datetime.datetime.now()
rows = get_not_send()
print "Getting %d rows from Registration for send_mail()" % len(rows)
for request_id, logname, alias, authcode, status in rows:
    try:
        send_mail(request_id, logname, alias, authcode)
    except RegError as e:
        _log.error("SendMail is unsuccessfull: %s" % e.message)
        continue
print datetime.datetime.now()

fakers = []
fakers.append(Factory.create("ru_RU"))
fakers_max = len(fakers) - 1


print "Main cicle started at ", datetime.datetime.now()
for ii in xrange(10000):
    if __debug__: print '\n<--- Started ---\n'

    try:
        request_id = save_request(fakers[randint(0,fakers_max)])
    except RegError as e:
        _log.error("SaveRequest is unsuccessfull: %s" % e.message)
        continue
    if __debug__: print "SaveRequest is successfull"

    try:
        authcode = get_authcode(request_id)
    except RegError as e:
        _log.error("GetAuthcode is unsuccessfull: %s" % e.message)
        continue
    if __debug__: print "GetAuthcode is successfull"

    try:
        reg_approve(authcode)
    except RegError as e:
        _log.error("RegApprove is unsuccessfull: %s" % e.message)
        continue
    if __debug__: print "RegApprove is successfull"

    if __debug__: print '\n--- Finished --->\n'
    # s = raw_input('Press any key to continue...')
print "Main cicle finished at ", datetime.datetime.now()

print datetime.datetime.now()
reg_garbage(60*60*24*10)
print datetime.datetime.now()

sock.send(json.dumps(None))
sock.close()

_log.debug("Finished.")
