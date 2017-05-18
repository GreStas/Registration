#!/usr/bin/python
# -*- coding: utf-8 -*-


import logging
import json
from regproto import *
import appproto
# под именем AppProto должен быть класс, реализующий обмен через сокет


# _log = logging.getLogger("RegSrvrProto")
# _log.info("Started")


class Error(RuntimeError):
    pass


class RegServerProto(object):
    def __init__(self, reg_worker, sock):
        self._proto = appproto.AppProto(sock)
        self._reg_worker = reg_worker
        header = self._proto.recv_head()
        switch_mesg = {MESG_SAVEREQUEST: self.save_request,
                       MESG_REGAPPROVE: self.approve,
                       MESG_SENDMAIL: self.sendmail,
                       MESG_GARBAGE: self.garbage,
                       MESG_GATHER: self.gather, }
        self._log = logging.getLogger("%s[%s]" % (self.__class__.__name__, self.__hash__()))
        try:
            if not header:
                raise Error("Header is empty")
            elif header['head'] != HEAD_CMND:
                self._log.error("'%s' is not a command" % header['head'])
                raise Error("'%s' is not a command" % header['head'])
            elif header['mesg'] in switch_mesg:
                switch_mesg[header['mesg']](header)
            else:
                self._log.error("Server's got Unknown command: %s" % header['mesg'])
                self._proto.send_head(HEAD_ERRR, MESG_COMMON, "Unknown command: %s" % header['mesg'])
        except KeyError as e:
            self._log.error("Server's got Invalid header: %s" % e.message)
            self._proto.send_head(HEAD_ERRR, MESG_COMMON, "Invalid header: %s" % e.message)

    def garbage(self, header):
        result = self._reg_worker.garbage(
            timealive=header['data']['timealive'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_GARBAGE, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_GARBAGE, self._reg_worker.ErrMsgs[result])

    def approve(self, header):
        result = self._reg_worker.approve(
            authcode=header['data']['authcode'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_REGAPPROVE, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_REGAPPROVE, self._reg_worker.ErrMsgs[result])

    def sendmail(self, header):
        result = self._reg_worker.sendmail(
            request_id=header['data']['request_id'],
            logname=header['data']['logname'],
            alias=header['data']['alias'],
            authcode=header['data']['authcode'],
        )
        if result == 0:
            self._proto.send_head(HEAD_ANSW, MESG_SENDMAIL, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_SENDMAIL, self._reg_worker.ErrMsgs[result])

    def gather(self, header):
        # if __debug__: _log.debug("2 recv_head")
        rows = self._reg_worker.gather(fields=header['data']['fields'], limit=header['data']['limit'])
        if rows is None:
            # if __debug__: _log.debug("3 send_head")
            self._proto.send_head(HEAD_ANSW, MESG_GATHER, None)
        else:
            json_data = json.dumps(rows)
            # if __debug__: _log.debug("3 send_head")
            self._proto.send_head(HEAD_ANSW, MESG_GATHER, len(json_data))
            # if __debug__: _log.debug("6 recv_head")
            answer = self._proto.recv_head()
            if answer['data']:
                # if __debug__: _log.debug("7 send_rawdata")
                self._proto.send_rawdata(json_data)

    def save_request(self, header):
        if __debug__:
            self._log.debug("Call _reg_worker('%s','%s')" % (header['data']['logname'], header['data']['alias']))
        result = self._reg_worker.save_request(
            logname=header['data']['logname'],
            alias=header['data']['alias'],
            passwd=header['data']['passwd'],
        )
        if __debug__:
            self._log.debug("_reg_worker('%s','%s') returned %s"
                            % (header['data']['logname'], header['data']['alias'], str(result)))
        if result > 0:
            self._proto.send_head(HEAD_ANSW, MESG_SAVEREQUEST, result)
        else:
            self._proto.send_head(HEAD_ERRR, MESG_SAVEREQUEST, self._reg_worker.ErrMsgs[result])
