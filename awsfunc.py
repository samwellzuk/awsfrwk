# -*-coding: utf-8 -*-
# Created by samwell
import copy
from functools import wraps, partial
from .reflect import check_func
from .awscall import call_by_aws

"""
以下函数通过装饰器封装对被装饰函数的并发调用。
通过函数的taskid/funcid等信息可以达到对每个函数并发控制的目的。即：相同 taskid-funcid-awsfunc 作为一个并发调用的单元。
内部会根据这个来自动建立对应的aws lambda函数，aws sqs队列
"""


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

