# -*-coding: utf-8 -*-
# Created by samwell
import logging
from .reflect import check_func, dump_func, load_func
from .awsservice import send_message, check_awsenv, _get_template

_logger = logging.getLogger(__name__)


def call_by_aws(func, args, kwargs, context,
                backup_cb, restore_cb, clean_cb):
    if 'task_name' not in kwargs or not kwargs['task_name']:
        raise RuntimeError('Need task_name parameter')
    # always run at serverless environment, call function directly
    if '_run_in_aws' in kwargs:
        del kwargs['_run_in_aws']
        func_args, func_kwargs = restore_cb(*args, **kwargs) if restore_cb else (args, kwargs)
        try:
            result = func(*func_args, **func_kwargs)
        finally:
            if clean_cb:
                clean_cb(*func_args, **func_kwargs)
        return result
    else:
        bk_args, bk_kwargs = backup_cb(*args, **kwargs) if backup_cb else (args, kwargs)
        # in debug mode , call fucntion directly
        if 'debug' in kwargs and kwargs['debug']:
            rs_args, rs_kwargs = restore_cb(*bk_args, **bk_kwargs) if restore_cb else (bk_args, bk_kwargs)
            try:
                result = func(*rs_args, **rs_kwargs)
            finally:
                if clean_cb:
                    clean_cb(*rs_args, **rs_kwargs)
            return result
        else:
            # need call remote function by send message to sqs
            func_bin, func_content = dump_func(func)
            msg = {
                "args": bk_args,
                "kwargs": bk_kwargs,
                "func_bin": func_bin,
                "func_content": func_content
            }
            context["task_name"] = bk_kwargs['task_name'] if 'task_name' in bk_kwargs else None
            context["task_id"] = bk_kwargs['task_id'] if 'task_id' in bk_kwargs else None
            tmpl = _get_template(**context)
            queue_url, s3_bucket, s3_key_prefix, s3_obj_tags = check_awsenv(tmpl)
            send_message(queue_url, s3_bucket, s3_key_prefix, s3_obj_tags, msg)
            return None
