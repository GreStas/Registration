#!/usr/bin/python
# -*- coding: utf-8 -*-
""" Registration Protocol interface module

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

HEAD_CMND = 1
HEAD_ANSW = 2
HEAD_ERRR = 3

MESG_COMMON = 0
MESG_SAVEREQUEST = 1
MESG_SENDMAIL = 2
MESG_REGAPPROVE = 3
MESG_GARBAGE = 4
MESG_GATHER = 5