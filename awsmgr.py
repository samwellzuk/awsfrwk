# -*-coding: utf-8 -*-
# Created by samwell

import boto3
import logging
import copy
from botocore.exceptions import ClientError

from .awsservice import check_awsenv, remove_awsenv, _get_template

_logger = logging.getLogger(__name__)


def initialize_awsenv(funclist):
    checkset = set()
    for func, task_name, task_id in funclist:
        if not hasattr(func, '__aws_context__'):
            raise RuntimeError('Function type is wrong')
        context = copy.deepcopy(func.__aws_context__)
        key = '%s.%d.%s' % (task_name, 0 if task_id is None else task_id, context['func_id'])
        if key in checkset:
            continue
        checkset.add(key)
        context['task_name'] = task_name
        context['task_id'] = task_id
        tmpl = _get_template(**context)
        try:
            check_awsenv(tmpl)
        except Exception:
            _logger.exception('initialize_awsenv')


def cleanup_awsenv(funclist, clean_func=False):
    checkset = set()
    for func, task_name, task_id in funclist:
        if not hasattr(func, '__aws_context__'):
            raise RuntimeError('Function type is wrong')
        context = copy.deepcopy(func.__aws_context__)
        key = '%s.%d.%s' % (task_name, 0 if task_id is None else task_id, context['func_id'])
        if key in checkset:
            continue
        checkset.add(key)
        context['task_name'] = task_name
        context['task_id'] = task_id
        tmpl = _get_template(**context)
        try:
            remove_awsenv(tmpl, clean_func)
        except Exception:
            _logger.exception('cleanup_awsenv')


def _get_queue_count(sqscli, queue_name):
    count = 0
    try:
        resp = sqscli.get_queue_url(QueueName=queue_name)
        resp = sqscli.get_queue_attributes(QueueUrl=resp['QueueUrl'],
                                           AttributeNames=[
                                               'ApproximateNumberOfMessages',
                                               'ApproximateNumberOfMessagesNotVisible',
                                               'ApproximateNumberOfMessagesDelayed'])
        count += int(resp['Attributes']['ApproximateNumberOfMessages'])
        count += int(resp['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        count += int(resp['Attributes']['ApproximateNumberOfMessagesDelayed'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'AWS.SimpleQueueService.NonExistentQueue':
            raise
    return count


def get_awsenv_info(funclist):
    sqscli = boto3.client('sqs')
    checkset = set()
    runcount, deadcount = 0, 0
    for func, task_name, task_id in funclist:
        if not hasattr(func, '__aws_context__'):
            raise RuntimeError('Function type is wrong')
        context = copy.deepcopy(func.__aws_context__)
        key = '%s.%d.%s' % (task_name, 0 if task_id is None else task_id, context['func_id'])
        if key in checkset:
            continue
        checkset.add(key)

        context['task_name'] = task_name
        context['task_id'] = task_id
        tmpl = _get_template(**context)

        runcount += _get_queue_count(sqscli, tmpl['sqs_queue']['QueueName'])
        deadcount += _get_queue_count(sqscli, tmpl['sqs_queue_dead_letter']['QueueName'])
    return runcount, deadcount


def _move_queue(src, dst, max=1000, restorcb=None):
    have_msg = True
    mvedlist = []
    recvlist = []
    while True:
        msglist = src.receive_messages(
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=30,
            WaitTimeSeconds=3)
        if len(msglist) == 0:
            have_msg = False
            break
        recvlist.extend(msglist)
        if len(recvlist) >= max:
            break

    for msg in recvlist:
        try:
            if restorcb:
                restorcb(msg)
            if msg.message_attributes:
                response = dst.send_message(MessageBody=msg.body, MessageAttributes=msg.message_attributes)
                if response['MD5OfMessageBody'] != msg.md5_of_body:
                    _logger.error('Send message to sqs , body md5 error!')
                elif response['MD5OfMessageAttributes'] != msg.md5_of_message_attributes:
                    _logger.error('Send message to sqs , attrib md5 error!')
            else:
                response = dst.send_message(MessageBody=msg.body)
                if response['MD5OfMessageBody'] != msg.md5_of_body:
                    _logger.error('Send message to sqs , body md5 error!')
            mvedlist.append(msg)
        except:
            _logger.exception('Send messge [%s]', msg.message_id)

    msgids = [{'Id': msg.message_id, 'ReceiptHandle': msg.receipt_handle} for msg in mvedlist]
    start = 0
    size = 10
    while True:
        batchids = msgids[start:start + size]
        start += size
        if not batchids:
            break
        response = src.delete_messages(Entries=batchids)
        if 'Failed' in response and response['Failed']:
            for info in response['Failed']:
                _logger.error('Remove msg Error: [%s] code: %s msg: %s', info['Id'], info['Code'], info['Message'])
    return have_msg


def move_queue(src_sqs, dest_sqs, max=0):
    sqsres = boto3.resource('sqs')
    src_queue = sqsres.get_queue_by_name(QueueName=src_sqs)
    dest_queue = sqsres.get_queue_by_name(QueueName=dest_sqs)
    cur = max
    while True:
        if max != 0:
            if not _move_queue(src_queue, dest_queue, 1000):
                break
            cur -= 1000
            if cur <= 0:
                break
        else:
            if not _move_queue(src_queue, dest_queue):
                break
