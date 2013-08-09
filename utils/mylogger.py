# -*- coding: utf-8 -*-
'''
Created on 2012-8-7

@author: wangwf
'''
import logging.config
import os

logging.config.fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../conf/logging.conf")) 
