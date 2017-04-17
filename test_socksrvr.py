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


def save_request(fake):
    #
    # Generate data
    logname = fake.email()
    passwd = fake.password(
        length=10,
        special_chars=True,
        digits=True,
        upper_case=True,
        lower_case=True
    )
    alias = fake.name()
    if __debug__: _log.debug("Generated: '%s',  '%s', '%s'." % (logname, passwd, alias))
    #
    # Prepare and send data
    data = {
        'logname': logname,
        'passwd': passwd,
        'alias': alias,
    }
    send_data = json.dumps({
        'cmnd': 'SaveRequest',
        'data': data,
    })
    sock.send(send_data)
    #
    # Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])
    data = recieve_data['data']
    return data['request_id']

def get_authcode(request_id):
    # Prepare and send data
    data = {
        'fields': [
            {'authcode': None},
            {'request_id': "=%d" % request_id},
        ],
        'limit': 1,
    }
    send_data = json.dumps({'cmnd': 'Gather', 'data': data,})
    sock.send(send_data)
    #
    # Recieve data and prepare to return
    recieve_data = json.loads(sock.recv(1024))
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])
    data = recieve_data['data'] # это будет список из одного элемента котрежа из двух полей
    return data[0][0]

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
    #
    # Prepare and send data
    # request_id, logname, alias, authcode
    data = {
        'fields': [
            {'request_id': None},
            {'logname': None},
            {'alias': None},
            {'authcode': None},
            {'status': "='requested'"}
        ],
        'limit': None,
    }
    send_data = json.dumps({'cmnd': 'Gather', 'data': data,})
    sock.send(send_data)
    #
    # Recieve data and prepare to return
    json_data = None
    while True:
        raw_data = sock.recv(1024)
        if not raw_data:
            del raw_data
            break
        else:
            json_data += raw_data
    recieve_data = json.loads(json_data)
    del json_data
    if recieve_data['answ'] == 'Error':
        _log.error("%s: '%s'" % (recieve_data['answ'], recieve_data['mesg']))
        raise RegError(recieve_data['mesg'])
    return recieve_data['data']

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
for request_id, logname, alias, authcode, status in get_not_send():
    try:
        send_mail(request_id, logname, alias, authcode)
    except RegError as e:
        _log.error("SendMail is unsuccessfull: %s" % e.message)
        continue

print datetime.datetime.now()
sock.send(json.dumps(None))
sock.close()
# sys.exit(0)

fakers = []
fakers.append(Factory.create("ru_RU"))
fakers_max = len(fakers) - 1

sock = socket.socket()
sock.connect(('', 9090))

print datetime.datetime.now()
for ii in xrange(1):
    if __debug__: print '\n<--- Started ---\n'

    try:
        request_id = save_request(fakers[randint(0,fakers_max)])
    except RegError as e:
        _log.error("SaveRequest is unsuccessfull: %s" % e.message)
        continue

    try:
        authcode = get_authcode(request_id)
    except RegError as e:
        _log.error("GetAuthcode is unsuccessfull: %s" % e.message)
        continue

    try:
        reg_approve(authcode)
    except RegError as e:
        _log.error("RegApprove is unsuccessfull: %s" % e.message)
        continue

    reg_garbage(3600)

    if __debug__: print '\n--- Finished --->\n'
    # s = raw_input('Press any key to continue...')
print datetime.datetime.now()

sock.send(json.dumps(None))
sock.close()

_log.debug("Finished.")
