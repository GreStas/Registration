#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import socket

import logging
logging.basicConfig(
    filename="regclient.log",
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
        self._sock.connect((host, port))
        self._proto = appproto.AppProto(self._sock)

    def Gather(self, fields, limit=1):
        try:
            self._proto.send_cmnd(
                'Gather',
                {'fields': fields,
                 'limit': limit}
            )
            if self._proto.recv_answer()['answ'] == appproto.APP_PROTO_SIG_NO:
                return []
            self._proto.send_signal(appproto.APP_PROTO_SIG_OK)
            return self._proto.recv_cmnd()['data']
        except appproto.Error as e:
            raise Error(e.message)

    def get_authcode(self, request_id):
        data = self.Gather(
            fields=[('authcode', None),
                    ('id', "=%d" % request_id)],
            limit=1,
        )
        size = len(data)
        if size == 0:
            raise Error("No data found")
        elif size > 1:
            raise Error("Too many rows")
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

    def SaveRequest(self, fake):
        """ Return request_id or raise exception Error"""
        self._proto.send_cmnd(
            'SaveRequest',
            {'logname': fake.email(),
             'passwd': fake.password(length=10,
                                     special_chars=True,
                                     digits=True,
                                     upper_case=True,
                                     lower_case=True),
             'alias': fake.name()},
        )
        try:
            request_id = self._proto.recv_answer()['mesg']
        except appproto.Error as e:
            raise Error(e.message)
        return request_id

    def RegApprove(self, authcode):
        self._proto.send_cmnd('RegApprove',
                              {'authcode': authcode})
        try:
            self._proto.recv_answer()
        except appproto.Error as e:
            _log.error(e.message)
            raise Error(e.message)

    def Garbage(self, timealive):
        self._proto.send_cmnd('Garbage',
                              {'timealive': timealive})
        try:
            self._proto.recv_answer()
        except appproto.Error as e:
            _log.error(e.message)
            raise Error(e.message)

    def SendMail(self, request_id, logname, alias, authcode):
        self._proto.send_cmnd(
            'SendMail',
            {'request_id': request_id,
             'logname': logname,
             'alias': alias,
             'authcode': authcode}
        )
        try:
            self._proto.recv_answer()
        except appproto.Error as e:
            _log.error(e.message)
            raise Error(e.message)
