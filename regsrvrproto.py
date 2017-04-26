#!/usr/bin/python
# -*- coding: utf-8 -*-


import logging
import json
from regproto import *
import appproto
# под именем AppProto должен быть класс, реализующий обмен через сокет


_log = logging.getLogger("RegSrvrProto")
_log.info("Started")


class Error(RuntimeError):
    pass


class RegServerProto(object):
    def __init__(self, reg_worker, sock):
        self._proto = appproto.AppProto(sock)
        self._reg_worker = reg_worker
        header = self._proto.recv_head()
        switch_mesg = {MESG_SAVEREQUEST: self.SaveRequest,
                       MESG_REGAPPROVE: self.RegApprove,
                       MESG_SENDMAIL: self.SendMail,
                       MESG_GARBAGE: self.Garbage,
                       MESG_GATHER: self.Gather, }
        try:
            if not header:
                raise Error("Header is empty")
            elif header['head'] != HEAD_CMND:
                _log.error("'%s' is not a command" % header['head'])
                raise Error("'%s' is not a command" % header['head'])
            elif header['mesg'] in switch_mesg:
                switch_mesg[header['mesg']](header)
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