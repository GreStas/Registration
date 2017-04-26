#!/usr/bin/python
# -*- coding: utf-8 -*-


import logging
import json
import socket
from regproto import *
import appproto
# под именем AppProto должен быть класс, реализующий обмен через сокет


_log = logging.getLogger("RegSrvrProto")
_log.info("Started")


class Error(RuntimeError):
    pass


class RegClientProto(object):
    def __init__(self, host, port):
        self._sock = socket.socket()
        self._sock.connect((host, port))
        self._proto = appproto.AppProto(self._sock)

    def SaveRequest(self, logname, alias, passwd):
        self._proto.send_head(HEAD_CMND,
                              MESG_SAVEREQUEST,
                              {'logname': logname,
                               'alias': alias,
                               'passwd': passwd,})
        header = self._proto.recv_head()
        if not header:
            raise Error("SaveRequest don't return anything")
        elif header['head'] == HEAD_ANSW:
            return header['data']
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        else:
            raise Error("Invalid header")

    def Gather(self, fields, limit=1):
        if __debug__: _log.debug("1 send_head")
        self._proto.send_head(HEAD_CMND,
                              MESG_GATHER,
                              {'fields': fields,
                               'limit': limit})
        if __debug__: _log.debug("4 recv_head")
        header = self._proto.recv_head()
        if header is None:
            raise Error("Gather didn't return anything")
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        elif header['head'] == HEAD_ANSW and header['mesg'] == MESG_GATHER:
            if header['data'] is not None:
                if __debug__: _log.debug("5 send_head")
                self._proto.send_head(HEAD_CMND,
                                      MESG_GATHER,
                                      True)
                if __debug__: _log.debug("8 recv_rawdata")
                json_data = self._proto.recv_rawdata(header['data'])
                return json.loads(json_data)
            else:
                return None
        else:
            raise Error("Invalid header")

    def SendMail(self, request_id, logname, alias, authcode):
        self._proto.send_head(HEAD_CMND,
                              MESG_SENDMAIL,
                              {'request_id': request_id,
                               'logname': logname,
                               'alias': alias,
                               'authcode': authcode, })
        header = self._proto.recv_head()
        if not header:
            raise Error("SendMail returned nothing")
        elif header['head'] == HEAD_ANSW:
            return
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        else:
            raise Error("Invalid header")

    def RegApprove(self, authcode):
        self._proto.send_head(HEAD_CMND,
                              MESG_REGAPPROVE,
                              {'authcode': authcode, })
        header = self._proto.recv_head()
        if not header:
            raise Error("RegApprove returned nothing")
        elif header['head'] == HEAD_ANSW:
            return
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        else:
            raise Error("Invalid header")

    def Garbage(self, timealive):
        self._proto.send_head(HEAD_CMND,
                              MESG_GARBAGE,
                              {'timealive': timealive, })
        header = self._proto.recv_head()
        if not header:
            raise Error("Garbage returned nothing")
        elif header['head'] == HEAD_ANSW:
            return
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        else:
            raise Error("Invalid header")
