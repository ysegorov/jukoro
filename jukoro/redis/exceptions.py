# -*- coding: utf-8 -*-


class JukoroRedisException(Exception):
    """ Base class for exceptions """


class AlreadyLocked(JukoroRedisException):
    """ Exception for an unavailable lock """


class QueueError(JukoroRedisException):
    """ Exception for queue error """


class NotRegisteredScript(JukoroRedisException):
    """ Exception raised if Lua script is not registered """
