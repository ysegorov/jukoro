# -*- coding: utf-8 -*-


class PgError(Exception):
    """ Base pg exception class """


class PgUriError(PgError):
    """ Pg uri parsing error """


class PgAlreadyRegisteredError(PgError):
    """ Entity class registered error """
