#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
Created on 2011-8-17

@author: feng
'''

import logging,os,sys,time
from printerrorinfo import printErrorInfo

class Log(object):
    """
    classdocs
    """

    @staticmethod
    def initlog(**kwargs):
        try:
            if kwargs["logfile"]:
                logFilePath = kwargs["logfile"]
                
                logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filename=logFilePath,
                        filemode='aw')
                console = logging.StreamHandler()
                console.setLevel(logging.INFO)
                formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
                console.setFormatter(formatter)
                logging.getLogger('').addHandler(console)
        except:
            print printErrorInfo()

    @staticmethod
    def getLogging(name):
        try:
            return logging.getLogger(name)
        except:
            print printErrorInfo()



#syspath = os.path.abspath(os.path.dirname(sys.argv[0]))
#print syspath
syspath = os.path.dirname(__file__)
create_time = time.strftime("%Y%m%d", time.localtime())
logname = 'log_'+create_time+'_spider.log'
#logfile = logpath + str(logname)
Log.initlog(logfile=logname)

