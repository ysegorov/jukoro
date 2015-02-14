# -*- coding: utf-8 -*-


class JukoroRedisException(Exception):
    """ Base class for exceptions """


class AlreadyLocked(JukoroRedisException):
    """ Exception for an unavailable lock """
