#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Created on 2010-9-28

@author: feng
"""

import sys
import traceback

def printErrorInfo():
    try:
        returnStr = ""
        printStr = "-" * 90
        returnStr += printStr + "\n"
        info = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(info[2]):
            returnStr += str(file) + " line: " + str(lineno) + " in " + str(function) + "\n"
            returnStr += str(text) + "\n"
        returnStr += "%s: %s" % info[:2] + "\n"
        returnStr += printStr
        return returnStr
    except:
        info = sys.exc_info()
        for file, lineno, function, text in traceback.extract_tb(info[2]):
            print file, "line:", lineno, "in", function
            print text
        print "%s: %s" % info[:2]
    