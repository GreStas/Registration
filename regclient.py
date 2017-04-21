#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import socket

import logging
logging.basicConfig(
    filename="Registration.log",
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging.DEBUG)
_log = logging.getLogger("RegClient")
_log.debug("Started")


try:
    import appproto
except ImportError, info:
    print "Import user's modules error:", info
    sys.exit()

class Error(RuntimeError):
    pass


class RegClient(object):
    def __init__(self, host, port):
        self._sock = socket.socket()
        self._sock.connect(host, port)
        self._proto = appproto.AppProto(self._sock)

    def get_authcode(self, request_id):
        try:
            self._proto.send_cmnd(
                'Gather',
                {'fields': [('authcode', None),
                            ('id', "=%d" % request_id),],
                 'limit': 1,}
            )
            cnt = self._proto.recv_answer()
            if cnt == 0:
                raise Error("No data found")
            elif cnt > 1:
                raise Error("Too many rows")
            self._proto.send_OK()
            data = self._proto.recv_cmnd()
        except appproto.Error as e:
            raise Error(e.message)
        return data[0][0]   # нам нужно только первое поле

    def get_not_send(self):
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
            raise Error(recieve_data['mesg'])
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

    def SaveRequest(self, fake):
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
            raise Error(e.message)
        return data

    def RegApprove(self, authcode):
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
            raise Error(recieve_data['mesg'])

    def Garbage(self, timealive):
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
            raise Error(recieve_data['mesg'])


    def SendMail(self, request_id, logname, alias, authcode):
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
            raise Error(recieve_data['mesg'])
