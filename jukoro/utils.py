# -*- coding: utf-8 -*-

import os
import multiprocessing
import pwd

from jukoro.structures import ObjectDict


def mergedicts(a, b):
    res = a.copy()
    for k in b:
        current = res.get(k, None)
        update = b.get(k)
        if isinstance(current, ObjectDict) or isinstance(update, ObjectDict):
            res[k] = ObjectDict(mergedicts(res.get(k, {}), update))
        elif isinstance(current, dict) and isinstance(update, dict):
            res[k] = mergedicts(res[k], update)
        elif isinstance(update, (list, tuple, set)):
            # TODO members?
            res[k] = type(update)(update)
        else:
            res[k] = update
    if isinstance(a, ObjectDict) or isinstance(b, ObjectDict):
        return ObjectDict(res)
    return res


def os_user():
    return pwd.getpwuid(os.getuid()).pw_name


def cpu_count():
    return multiprocessing.cpu_count() or 1
