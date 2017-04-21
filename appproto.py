#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from threading import RLock


class Error(RuntimeError):
    pass

APP_PROTO_OK = 'OK'
APP_PROTO_NO = 'NO'
APP_PROTO_ERROR = 'ERROR'


class AppProto(object):
    """ Использует уже установленное соединение self.sock:socket.socket для отправки и приёма JSON"""

    def __init__(self, sock):
        self.sock = sock
        self._send_rlock = RLock()
        self._recv_rlock = RLock()

    def send_OK(self):
        with self._send_rlock:
            self.sock.send(APP_PROTO_OK)

    def send_NO(self):
        with self._send_rlock:
            self.sock.send(APP_PROTO_NO)

    def _send_answer(self, answ, mesg):
        with self._send_rlock:
            self.sock.send(json.dumps({
                'answ': answ,
                'mesg': mesg,
            }))

    def send_ERROR(self, mesg):
        self._send_answer(APP_PROTO_ERROR, mesg)

    def send_SUCCESS(self, mesg=None):
        self._send_answer(APP_PROTO_OK, mesg)

    def send_cmnd(self, cmnd, data=None):
        with self._send_rlock:
            if data is None:
                size = 0
            else:
                json_data = json.dumps(data)
                size = len(json_data)
            self.sock.send(json.dumps({'cmnd': cmnd, 'size': size}))
            if size == 0:
                return
            if self.recv_OK():
                self.sock.send(json_data)
            else:
                raise Error("Not recieve command header delivery")

    def recv_OK(self):
        with self._recv_rlock:
            try:
                return self.sock.recv(2) == APP_PROTO_OK
            except IOError:
                return False

    def recv_answer(self, bufsize=1024):
        """ На выходе mesg или исключение Error"""
        with self._recv_rlock:
            data = json.loads(self.sock.recv(bufsize))
            if data['answ'] == APP_PROTO_ERROR:
                raise Error(data['mesg'])
            elif data['answ'] == APP_PROTO_OK:
                return data['mesg']

    def recv_cmnd(self, bufsize=1024):
        """ На выходе data или None или исключение Error"""
        with self._recv_rlock:
            raw_data = self.sock.recv(bufsize)
            if not raw_data:
                return None
            header = json.loads(raw_data)
            try:
                cmnd = header['cmnd']
                size = header['size']
            except KeyError as e:
                mesg = "Invalid header:'%s'" % e.message
                self.send_ERROR(mesg)
                raise Error(mesg)
            self.send_OK()
            json_data = ''
            while size > 0:
                raw_data = self.sock.recv(bufsize)
                json_data += raw_data
                size -= len(raw_data)
            return {'cmnd':cmnd, 'data':json.loads(json_data)}
