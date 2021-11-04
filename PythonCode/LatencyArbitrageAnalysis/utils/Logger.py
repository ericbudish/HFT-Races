'''
Logger.py

Defines the logger object.
'''

import logging
import sys

class LoggerWriter(object):
    def __init__(self, logger, level = logging.WARNING):
        self.logger = logger
        self.level = level
        
    def write(self, message):
        for line in message.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())
    
    def flush(self):
        pass

def getLogger(logpath, logfile, name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(logpath + logfile)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    sys.stdout = LoggerWriter(logger, logging.WARNING)
    sys.stderr = LoggerWriter(logger, logging.ERROR)
    return logger
    
    
    
    
    
