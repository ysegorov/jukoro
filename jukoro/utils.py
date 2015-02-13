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
            res[k] = ObjectDict(mergedicts(res.get(k, {}), b.get(k)))
        elif isinstance(current, dict) and isinstance(update, dict):
            res[k] = mergedicts(res[k], b[k])
        else:
            res[k] = b[k]
    return ObjectDict(res) if isinstance(a, ObjectDict) else res


def os_user():
    return pwd.getpwuid(os.getuid()).pw_name


def cpu_count():
    return multiprocessing.cpu_count() or 1
