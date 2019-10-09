# -*-coding: utf-8 -*-
# Created by samwell
import logging
import gc

from .rtctl import init_tmchk, uninit_tmchk, AwsLambdaTimeout
from .awsservice import decode_message, clean_message_attachment
from .reflect import load_func
from .dirmgr import clear_tmp_dir

_logger = logging.getLogger(__name__)


def common_handler(event, context):
    init_tmchk(context)
    try:
        msglist = decode_message(event['Records'])
        for msg in msglist:
            args, kwargs, func_bin, func_content = tuple(msg['args']), msg['kwargs'], \
                                                   msg['func_bin'], msg['func_content']
            func = load_func(func_bin, func_content)
            kwargs['_run_in_aws'] = True
            func(*args, **kwargs)
    # bypass normal exception, so message send to death queue
    except AwsLambdaTimeout:
        _logger.exception('common_handler')
    finally:
        # don't retry when message sent to death queue, so clear attachment
        clean_message_attachment(event['Records'])
        uninit_tmchk()
        clear_tmp_dir()
        gc.collect()


