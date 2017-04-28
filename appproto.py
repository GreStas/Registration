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
