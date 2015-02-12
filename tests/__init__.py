# -*- coding: utf-8 -*-

import datetime


FNAME = 'tests.log'


def _now():
    return datetime.datetime.now().isoformat()


def setUp():
    with open(FNAME, 'wt') as f:
        msg = ':: started at {}\n'.format(_now())
        f.write(msg)


def tearDown():
    with open(FNAME, 'at') as f:
        msg = ':: finished at {}\n'.format(_now())
        f.write(msg)
