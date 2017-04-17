# -*- coding: utf-8 -*-
#
#   module  :   config.py
#   version :   0.0.0.1
#   begin   :   08/04/2017
""" module config
    PURPOSE:
        вычислить параметры из файла конфигураций и параметров командной строки
    ACTIONS:
    - вычитать параметры из опций из коммандной строки
    - для незаполненных значений (None) - вычитать из файла конфигурации, ошибки игнорировать
    - если параметр нигде не определён и нет значекния по-умолчанию, то оставить пустым

    ОГРАНИЧЕНИЯ:
    - одноимённые параметры должны совпадать по именам с учётом лидирующих "--" для опций
        , например: параметр("pghost") == опция("--pghost")
    - обрабатываются только опции, начинающиеся с лидирующих "--"
"""

import optparse
import ConfigParser

class Config(object):
    """ class Config
    Usage:
        c = Config(
            options,  # list of dictionaries of options for optparse.OptionParser.add_option(optname, action, dest, type, metavar, section)
            filename = None, # .conf file
            version = '0.0.0.1', # for optparse.OptionParser
            description = 'Config: getParam, getParams, getConfig', # for optparse.OptionParser
        )
        c.options = {'parameter':value, ... }
    """

    def __init__(self,
         options,  # list of dictionaries of options for optparse.OptionParser.add_option(optname, action, dest, type, metavar, section)
         filename = None, # .conf file
         version = '0.0.0.1', # for optparse.OptionParser
         description = 'Config: getParam, getParams, getConfig', # for optparse.OptionParser
    ):
        # получаем параметры из опций коммандной строки
        opts = optparse.OptionParser(version=version, description=description)
        for item in options:
            # сохраняеи параметр section (нужен для разбора .conf), чтобы не мешал разбору
            try: section = item.pop('section')
            except KeyError: section = None
            # изымаем из словаря обязательный параметр - название опции
            optname = item.pop('name')
            #  остаток словаря передаём как есть и создаём опцию
            opts.add_option('--%s' % optname, **item)
            # Устанавливаем значение по-умолчнаию, если оно передано и передаём через словарь
            try:
                kwargs = {item['dest']:item['default']}
                opts.set_defaults(**kwargs)
            except KeyError: pass
            # усекаем словарь и восстанавливаем параметры name и section (нужны для разбора .conf)
            item.clear()
            item['name'] = optname
            if section is not None: item['section'] = section
        # сохраняем результат разбора в аттрибутах экземпляра калсса
        params, args = opts.parse_args()

        self.options = vars(params)

        if filename is None: return
        config = ConfigParser.ConfigParser()
        config.read(filename)
        for option in options:
            if self.options[option['name']] is None and 'section' in option:
                try: self.options[option['name']] = config.get(option['section'], option['name'])
                except configparser.Error: pass


if __name__ == "__main__":
    p1 = Config(
        [
            {
                'name': 'pghost',
                'action': 'store',
                'dest': 'pghost',
                'default': 'deboraws',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'pgdb',
                'action': 'store',
                'dest': 'pgdb',
                'default': 'test_db',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'pguser',
                'action': 'store',
                'dest': 'pguser',
                'default': 'tester',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'pgpasswd',
                'action': 'store',
                'dest': 'pgpasswd',
                'default': 'testing',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'pgrole',
                'action': 'store',
                'dest': 'pgrole',
                'default': 'test_db_dev1_users',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'pgschema',
                'action': 'store',
                'dest': 'pgschema',
                'default': 'dev1',
                'section': 'POSTGRESQL',
            },
            {
                'name': 'duration',
                'action': 'store',
                'type': 'int',
                'dest': 'duration',
                'metavar': 'integer  Duration of the test in minutes',
                'default': 60,
                'section': 'DEFAULT',
            },
            {
                'name': 'freq',
                'action': 'store',
                'dest': 'freq',
                'type': 'int',
                'metavar': 'integer      How often will register of user per minute',
                'default': '600',
                'section': 'DEFAULT',
            },
            {
                'name': 'pcterr',
                'action': 'store',
                'dest': 'pcterr',
                'metavar': 'decimal    How often (0..1) registered user will repeate registration',
                'default': '0.2',
                'section': 'DEFAULT',
            },
            # {
            #     'name': '',
            #     'action': 'store',
            #     'dest': '',
            #     'metavar': '',
            #     'default': '',
            #     'section': '',
            # },
        ],
        filename = "stresstest.conf",
    )
    print p1.options

    p2 = Config(
        [
            {'name': 'pghost', 'section': 'POSTGRESQL',},
            {'name': 'pgdb', 'section': 'POSTGRESQL',},
            {'name': 'pguser', 'section': 'POSTGRESQL',},
            {'name': 'pgpasswd', 'section': 'POSTGRESQL',},
            {'name': 'pgrole', 'section': 'POSTGRESQL',},
            {'name': 'pgschema', 'section': 'POSTGRESQL',},
            {'name': 'duration', 'section': 'DEFAULT',},
            {'name': 'freq', 'section': 'DEFAULT',},
            {'name': 'pcterr', 'section': 'DEFAULT',},
        ],
        filename = "stresstest.conf",
    )
    print p2.options