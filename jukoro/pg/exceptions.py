# -*- coding: utf-8 -*-


class PgError(Exception):
    """ Base pg exception class """


class PgUriError(PgError):
    """ Pg uri parsing error """


class PgAlreadyRegisteredError(PgError):
    """ Entity class registered error """


class PgPoolClosedError(PgError):
    """ Pool closed error """


class PgConnectionClosedError(PgError):
    """ Connection closed error """


class PgCursorClosedError(PgError):
    """ Cursor closed error """


class PgDoesNotExistError(PgError):
    """ Query returned nothing error """
