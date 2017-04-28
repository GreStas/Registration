#!/usr/bin/python
# -*- coding: utf-8 -*-

from config import Config

if __name__ == "__main__":
    p1 = Config(
        [(("-i", "--infile"), {'action':"store", "type":"string", "dest":"if", 'default':'infile.txt'}),
         (("-o", "--outfile"), {'action': "store", "type": "string", "dest": "of"}),
         (("", "--loglevel"), {'action': "store", "type": "string", "dest": "loglevel", 'default':'CRITICAL'}),
         ],
        prefer_opt=False,
        version='0.0.0.2',
    )

    print "Inputfile is %s" % p1.get("if")
    print "Outputfile is %s" % p1.get("of")
    print "LogLevel is %s" % p1.get("loglevel")

    p1.load_conf('stresstest.conf')

    print "Inputfile is %s" % p1.get("if")
    print "Outputfile is %s" % p1.get("of")
    print "LogLevel is %s" % p1.get("loglevel",section="DEBUG", prefer_opt=True)

    p1.load_conf('stresstest.conf')
    print "LogLevel is %s" % p1.get("loglevel",section="DEBUG")
