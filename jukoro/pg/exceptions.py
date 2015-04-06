# -*- coding: utf-8 -*-


class PgError(Exception):
    """ Base pg exception class """


class BadUri(PgError):
    """ Pg uri parsing error """


class AlreadyRegistered(PgError):
    """ Entity class registered error """


class PoolClosed(PgError):
    """ Pool closed error """


class ConnectionClosed(PgError):
    """ Connection closed error """


class CursorClosed(PgError):
    """ Cursor closed error """


class DoesNotExist(PgError):
    """ Query returned nothing error """
