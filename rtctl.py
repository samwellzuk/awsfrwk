# -*-coding: utf-8 -*-
# Created by samwell
import signal


class AwsLambdaTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise AwsLambdaTimeout('AWS Lambda time out!')


def init_tmchk(context):
    # aws lambda maximum time is 300 seconds, so throw exception before maximum timeout, give function chance to do clear work
    tmout = context.get_remaining_time_in_millis() // 1000 - 30
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(tmout)


def uninit_tmchk():
    signal.alarm(0)
