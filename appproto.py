#!/usr/bin/python
# -*- coding: utf-8 -*-
""" AppProto protocol specification

Обмен всегда однофазный:
1. Инициатор высылает все данные одним пакетом
2. Получатель всегда отвечает одним пакетом
3. Пакеты бывают двух типов
3.1. rawdate - бинарная строка
3.2. head - структурированный пакет неопределённого размера менее, чем bufsize (задан в конструкторе).
    Пакеты всегда состоят из словаря, упакованного JSON. Структура словаря:
        'head': строка из 4 символов
        'mesg': строка, параметр уточняющий заголовок
        'data': свободная структура, котрая будет интерпретироваться самим приложением
"""

import json
from threading import RLock
import logging
_log = logging.getLogger("AppProto")


class Error(RuntimeError):
    pass


class AppProto(object):
    """ Использует уже установленное соединение self.sock:socket.socket для отправки и приёма JSON"""

    def __init__(self, sock, bufsize=1024):
        self.sock = sock
        self.bufsize = bufsize
        self._send_rlock = RLock()
        self._recv_rlock = RLock()

    def send_head(self, head, mesg, data):
        with self._send_rlock:
            self.sock.sendall(json.dumps({'head': head, 'mesg': mesg, 'data': data}))

    def recv_head(self):
        with self._recv_rlock:
            raw_data = self.sock.recv(self.bufsize)
            return json.loads(raw_data) if raw_data else None

    def send_rawdata(self, rawdata):
        with self._send_rlock:
            self.sock.sendall(rawdata)

    def recv_rawdata(self, size):
        _size = size
        with self._recv_rlock:
            raw_data = ''
            while _size > 0:
                data = self.sock.recv(self.bufsize)
                raw_data += data
                _size -= len(data)
            return raw_data


APP_PROTO_SIG_LEN = 2
APP_PROTO_SIG_OK = 'OK'
APP_PROTO_SIG_NO = 'NO'
APP_PROTO_SIG_ER = 'ER'
APP_PROTO_SIGNALS = {APP_PROTO_SIG_OK, APP_PROTO_SIG_NO, APP_PROTO_SIG_ER}


class AppProto1(object):
    def __init__(self, sock):
        self.sock = sock
        self._send_rlock = RLock()
        self._recv_rlock = RLock()

    def send_signal(self, signal):
        if __debug__: _log.debug(signal)
        if signal not in APP_PROTO_SIGNALS:
            raise Error("Unknown signal '%s'" % signal)
        with self._send_rlock:
            self.sock.send(signal)

    def recv_signal(self):
        """ Recieve signal sent send_OK or send_NO"""
        with self._recv_rlock:
            signal = self.sock.recv(APP_PROTO_SIG_LEN)
            if __debug__: _log.debug(signal)
            if signal not in APP_PROTO_SIGNALS:
                raise Error("Unknown signal '%s'" % signal)
            return signal

    def send_answer(self, answ, mesg):
        if __debug__: _log.debug("('%s','%s')" % (answ, mesg))
        with self._send_rlock:
            self.sock.send(json.dumps(
                {'answ': answ,
                 'mesg': mesg}
            ))

    def recv_answer(self, bufsize=1024):
        """ Return recieved message or raise exception Error"""
        with self._recv_rlock:
            data = json.loads(self.sock.recv(bufsize))
            if __debug__: _log.debug(data)
            if data['answ'] == APP_PROTO_SIG_ER:
                raise Error(data['mesg'])
            return data

    def send_ERROR(self, mesg):
        self.send_answer(APP_PROTO_SIG_ER, mesg)

    def send_SUCCESS(self, mesg=None):
        self.send_answer(APP_PROTO_SIG_OK, mesg)

    def send_cmnd(self, cmnd, data=None):
        if __debug__: _log.debug(cmnd, data)
        with self._send_rlock:
            if data is None:
                size = 0
            else:
                json_data = json.dumps(data)
                size = len(json_data)
            if __debug__: _log.debug({'cmnd': cmnd, 'size': size})
            self.sock.send(json.dumps({'cmnd': cmnd, 'size': size}))
            if size == 0:
                return
            if __debug__: _log.debug("Wait signal...")
            signal = self.recv_signal()
            if __debug__: _log.debug("...", signal)
            if signal == APP_PROTO_SIG_OK:
                self.sock.send(json_data)
            else:
                raise Error("Recieved not delivery signal")

    def recv_cmnd(self, bufsize=1024):
        """ На выходе data или None или исключение Error"""
        with self._recv_rlock:
            raw_data = self.sock.recv(bufsize)
            if __debug__: _log.debug(raw_data)
            if not raw_data:  # Проверка на то, что Клиент не закрыл сокет
                return None
            header = json.loads(raw_data)
            try:
                cmnd = header['cmnd']
                size = header['size']
            except KeyError as e:
                mesg = "Invalid header:'%s'" % e.message
                self.send_ERROR(mesg)
                raise Error(mesg)
            if size == 0:
                return None
            if __debug__: _log.debug("Size=%d, send signal '%s'" %(size, APP_PROTO_SIG_OK))
            self.send_signal(APP_PROTO_SIG_OK)
            json_data = ''
            while size > 0:
                raw_data = self.sock.recv(bufsize)
                json_data += raw_data
                size -= len(raw_data)
            if __debug__: _log.debug({'cmnd': cmnd, 'data': json_data})
            return {'cmnd': cmnd,
                    'data': json.loads(json_data)}
