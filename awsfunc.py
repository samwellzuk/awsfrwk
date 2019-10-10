# -*-coding: utf-8 -*-
# Created by samwell
import copy
from functools import wraps, partial
from .reflect import check_func
from .awscall import call_by_aws


def aws_common(func=None, *, concurrent=None, func_id=None, enabled=True, memory=128, maxretry=1):
    if func is None:
        return partial(aws_common, concurrent=concurrent, func_id=func_id, enabled=enabled, memory=memory,
                       maxretry=maxretry)

    check_func(func, ('task_name', 'task_id', 'debug'), ('_run_in_aws',))
    if func_id is None:
        func_id = func.__name__

    context = {
        'template': 'func.yml',
        'base_name': 'common',
        'func_id': func_id,
        'concurrent': concurrent,
        'event_enabled': enabled,
        'runtime_memory': memory,
        'event_batch': 1,
        'msg_max_retry': maxretry,
    }

    @wraps(func)
    def wrapper(*args, **kwargs):
        return call_by_aws(func, args, kwargs, context=context, backup_cb=None, restore_cb=None, clean_cb=None)

    wrapper.__aws_context__ = copy.deepcopy(context)
    return wrapper


def aws_common_vpc(func=None, *, concurrent=None, func_id=None, enabled=True, memory=128, maxretry=1):
    if func is None:
        return partial(aws_common_vpc, concurrent=concurrent, func_id=func_id, enabled=enabled, memory=memory,
                       maxretry=maxretry)

    check_func(func, ('task_name', 'task_id', 'debug'), ('_run_in_aws',))
    if func_id is None:
        func_id = func.__name__

    context = {
        'template': 'func_vpc.yml',
        'base_name': 'common',
        'func_id': func_id,
        'concurrent': concurrent,
        'event_enabled': enabled,
        'runtime_memory': memory,
        'event_batch': 1,
        'msg_max_retry': maxretry,
    }

    @wraps(func)
    def wrapper(*args, **kwargs):
        return call_by_aws(func, args, kwargs, context=context, backup_cb=None, restore_cb=None, clean_cb=None)

    wrapper.__aws_context__ = copy.deepcopy(context)
    return wrapper

