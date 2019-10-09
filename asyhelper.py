# -*-coding: utf-8 -*-
# Created by samwell
import logging
import threading
import asyncio

_logger = logging.getLogger(__name__)


def _thread_main(loop):
    try:
        loop.run_forever()
    finally:
        # Wait 250 ms for the underlying SSL connections to close, aio http
        loop.run_until_complete(asyncio.sleep(0.250))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def async_call(coro, arg=None):
    """
    run coroutine in sub thread, main thread waiting for sub thread exit.
    when main thread capture exception: AwsLambdaTimeout (which mean aws lambda time out, need exit now)
    then stop sub thread and rasie the exception again.
    :param
        coro: coroutine object
        arg:   argument for coroutine
    :return:
    """
    loop = asyncio.new_event_loop()
    kwargs = {'loop': loop}
    args = tuple() if arg is None else arg
    thread = threading.Thread(target=_thread_main, args=(loop,))
    try:
        thread.start()
        future = asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), loop)
        result = future.result()  # Wait for the result with a timeout
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join()

    return result
