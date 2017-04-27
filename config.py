# -*- coding: utf-8 -*-
#
#   module  :   config.py
#   version :   0.0.0.2
#   begin   :   08/04/2017


import optparse
import ConfigParser


class Config(object):
    """ вычислить параметры из файла конфигураций и параметров командной строки
    """
    def __init__(
            self,
            options,    # list of dictionaries of options for optparse.OptionParser.add_option
            prefer_opt=True,
            section='DEFAULT',
            **kwargs
    ):
        """ Вычитать в атрибут self.opts параметры коммандной строки
            Обработать в соответсвие с переданным списком параметров
        :param options: *args, **kwargs для ConfigParse.OptionParser.add_option
        :param prefer_opt: определяет приоритет опций над INI или наоборот при выборке параметра
        :param section: секция по-умолчанию при выборке параметра
        :param kwargs: параметры для OptionParser
        """
        self.prefer_opt = prefer_opt
        self.section = section
        if 'version' not in kwargs:
            kwargs['version'] = '0.0.0.2'
        if 'description' not in kwargs:
            kwargs['description'] = 'Config: getParam, getParams, getConfig'
        self._conf = None
        self._opts = optparse.OptionParser(**kwargs)
        # получаем параметры из опций коммандной строки
        for args_, kwargs_ in options:
            self._opts.add_option(*args_, **kwargs_)

        self._params, self.args = self._opts.parse_args()
        print 'vars(params):'
        for var in vars(self._params):
            print var, vars(self._params)[var]

    def load_conf(self, filename):
        """"Загрузка опций из INI-файла"""
        self._conf = ConfigParser.ConfigParser()
        self._conf.read(filename)

    def get(self, name, prefer_opt=None, section=None):
        """ Вычисляет значение параметра name
        :param name:
        :param prefer_opt:
        :param section:
        :return: value of anytype
        """
        section_, prefer_opt_ = section, prefer_opt
        if section_ is None:
            section_ = self.section
        if prefer_opt_ is None:
            prefer_opt_ = self.prefer_opt
        try:
            opt_ = vars(self._params)[name]
        except KeyError:
            opt_ = None
        cfg_ = None
        if self._conf is not None:
            try:
                cfg_ = self._conf.get(section_, name)
            except ConfigParser.NoOptionError:
                pass    # cfg_ was set to None
        if prefer_opt_:
            result1, result2 = opt_, cfg_
        else:
            result1, result2 = cfg_, opt_
        return result2 if result1 is None else result1
