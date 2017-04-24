#!/usr/bin/python
# -*- coding: utf-8 -*-
""" Registration Interface module

Описание возможных комбинаций см. в описании прикладных модулей.
    'cmnd' - передаётся команда с данными
    'answ' - передаётся ответ на команду с данными
    'errr' - передаётся информация об ошибке
==========================
head : mesg        : data
=====+=============+======
cmnd : SaveRequest : {'logname': '', 'alias': '', 'passwd': ''}
cmnd : SendMail    : {'request_id': int, 'logname': '', 'alias': '', 'authcode': ''}
cmnd : RegApprove  : {'authcode': ''}
cmnd : Garbage     : {'timealive': int}
cmnd : Gather      : {'fields': list of tuples, limit: int|None }
-----+-------------+------
answ : SaveRequest : {'request_id': int}
answ : SendMail    : {'errcode': int}
answ : RegApprove  : {'errcode': int}
answ : Garbage     : {'errcode': int}
answ : Gather      : {'count': int}
-----+-------------+------
errr : сообщение   : None
==========================
"""

import logging
logging.basicConfig(
    filename="reginterface.log",
    format="%(asctime)s pid[%(process)d].%(name)s.%(funcName)s(%(lineno)d):%(levelname)s:%(message)s",
    level=logging.DEBUG)
_log = logging.getLogger("RegInterface")
_log.debug("Started")

import socket
import json
# под именем AppProto должен быть класс, реализующий обмен через сокет
import appproto

HEAD_CMND = 1
HEAD_ANSW = 2
HEAD_ERRR = 3

MESG_COMMON = 0
MESG_SAVEREQUEST = 1
MESG_SENDMAIL = 2
MESG_REGAPPROVE = 3
MESG_GARBAGE = 4
MESG_GATHER = 5

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
        print header
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
        print header
        if not header:
            raise Error("Garbage returned nothing")
        elif header['head'] == HEAD_ANSW:
            return
        elif header['head'] == HEAD_ERRR:
            raise Error(header['data'])
        else:
            raise Error("Invalid header")


class RegServerProto(object):
    def __init__(self, reg_worker, sock):
        self._proto = appproto.AppProto(sock)
        self._reg_worker = reg_worker
        header = self._proto.recv_head()
        try:
            if not header:
                raise Error("Header is empty")
            elif header['head'] != HEAD_CMND:
                _log.error("'%s' is not a command" % header['head'])
                raise Error("'%s' is not a command" % header['head'])
            elif header['mesg'] == MESG_SAVEREQUEST: self.SaveRequest(header)
            elif header['mesg'] == MESG_REGAPPROVE: self.RegApprove(header)
            elif header['mesg'] == MESG_SENDMAIL: self.SendMail(header)
            elif header['mesg'] == MESG_GARBAGE: self.Garbage(header)
            elif header['mesg'] == MESG_GATHER: self.Gather(header)
            else:
                _log.error("Server's got Unknown command: %s" % header['mesg'])
                self._proto.send_head(HEAD_ERRR, MESG_COMMON, "Unknown command: %s" % header['mesg'])
        except KeyError as e:
            _log.error("Server's got Invalid header: %s" % e.message)
            self._proto.send_head(HEAD_ERRR, MESG_COMMON, "Invalid header: %s" % e.message)

    def Garbage(self, header):
        result = self._reg_worker.Garbage(
            timealive=header['data']['timealive'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_GARBAGE, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_GARBAGE, self._reg_worker.ErrMsgs[result])

    def RegApprove(self, header):
        result = self._reg_worker.RegApprove(
            authcode=header['data']['authcode'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_REGAPPROVE, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_REGAPPROVE, self._reg_worker.ErrMsgs[result])

    def SendMail(self, header):
        result = self._reg_worker.SendMail(
            request_id=header['data']['request_id'],
            logname=header['data']['logname'],
            alias=header['data']['alias'],
            authcode=header['data']['authcode'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_SENDMAIL, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_SENDMAIL, self._reg_worker.ErrMsgs[result])

    def Gather(self, header):
        if __debug__: _log.debug("2 recv_head")
        rows = self._reg_worker.Gather(fields=header['data']['fields'], limit=header['data']['limit'])
        if rows is None:
            if __debug__: _log.debug("3 send_head")
            self._proto.send_head(HEAD_ANSW, MESG_GATHER, None)
        else:
            json_data = json.dumps(rows)
            if __debug__: _log.debug("3 send_head")
            self._proto.send_head(HEAD_ANSW, MESG_GATHER, len(json_data))
            if __debug__: _log.debug("6 recv_head")
            answer = self._proto.recv_head()
            if answer['data']:
                if __debug__: _log.debug("7 send_rawdata")
                self._proto.send_rawdata(json_data)

    def SaveRequest(self, header):
        result = self._reg_worker.SaveRequest(
            logname=header['data']['logname'],
            alias=header['data']['alias'],
            passwd=header['data']['passwd'],
        )
        if result > 0:
            self._proto.send_head(HEAD_ANSW, MESG_SAVEREQUEST, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_SAVEREQUEST, self._reg_worker.ErrMsgs[result])