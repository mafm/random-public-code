"""Simplified interface to syslog via logging module.

Not proud of this, but at least it enforces consistent
formatting?!?!"""

import logging
import logging.handlers

## Severity levels

DEBUG = logging.DEBUG
# Detailed information, typically of interest only when diagnosing
# problems.

INFO = logging.INFO
# Confirmation that things are working as expected.

WARNING = logging.WARNING
# An indication that something unexpected has happened, or indicative
# of some problem in the near future (e/g/ 'disk space low'). The
# software is still working as expected.

ERROR = logging.ERROR
# Due to a more serious problem, the software has not been able to
# perform some function.

CRITICAL = logging.CRITICAL
# A serious error, indicating that the program itself may be unable to
# continue running.

_logger = None

def init(log_name, threshold):
    """Create logger sending messages meeting threshold to syslog.

    Example call: simple_syslog.init("ZfsReplicate", INFO)"""
    global _logger
    global _syslog_handler
    _logger = logging.getLogger(log_name)
    _logger.setLevel(threshold)# Create syslog handler, set level to threshold
    _syslog_handler = logging.handlers.SysLogHandler(address = '/dev/log')
    _syslog_handler.setLevel(threshold)
    formatter = logging.Formatter('%(levelname)s:%(name)s: %(asctime)s %(message)s')
    _syslog_handler.setFormatter(formatter)
    _logger.addHandler(_syslog_handler)

def setLevel(threshold):
    global _logger
    assert _logger
    _logger.setLevel(threshold)
    _syslog_handler.setLevel(threshold)

def debug(msg):
    global _logger
    assert _logger
    _logger.debug(msg)

def info(msg):
    global _logger
    assert _logger
    _logger.info(msg)

def warn(msg):
    global _logger
    assert _logger
    _logger.warn(msg)

def error(msg):
    global _logger
    assert _logger
    _logger.error(msg)

def critical(msg):
    global _logger
    assert _logger
    _logger.critical(msg)

