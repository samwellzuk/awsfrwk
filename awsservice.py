# -*-coding: utf-8 -*-
# Created by samwell

from bson.json_util import dumps, loads
import boto3
from botocore.exceptions import ClientError
import hashlib
import base64
import logging
import uuid
from struct import pack
import pkgutil
from string import Template
import yaml
import time

_sqs_limit = 256 * 1024 - 1024

_logger = logging.getLogger(__name__)


def _make_md5_base64(msg):
    binmsg = msg.encode()
    m = hashlib.md5(binmsg)
    checksum = base64.b64encode(m.digest()).decode()
    return binmsg, m.hexdigest(), checksum


def _make_attrib_check(attrib):
    binlist = []
    for k in attrib:
        name = k.encode()
        name_len = len(name)
        t = attrib[k]['DataType'].encode()
        t_len = len(t)
        v = attrib[k]['StringValue'].encode()
        v_len = len(v)
        form = '!l%dsl%dsbl%ds' % (name_len, t_len, v_len)
        bin = pack(form, name_len, name, t_len, t, 1, v_len, v)
        binlist.append(bin)
    return hashlib.md5(b''.join(binlist)).hexdigest()


def send_message(sqs_queue_url, s3_bucket, s3_key_prefix, s3_obj_tags, msg, force_s3=False):
    smsg = dumps(msg)
    binmsg, md5msg, checkmd5 = _make_md5_base64(smsg)
    if force_s3 or len(smsg) >= _sqs_limit:
        sqscli = boto3.client('sqs')
        s3cli = boto3.client('s3')
        objkey = '%s%s' % (s3_key_prefix, uuid.uuid1())
        s3cli.put_object(Bucket=s3_bucket, Key=objkey, Body=binmsg, ContentMD5=checkmd5, Tagging=s3_obj_tags)
        attrib = {
            's3': {
                'DataType': 'String',
                'StringValue': s3_bucket
            },
        }
        attrib_md5 = _make_attrib_check(attrib)
        smsg = objkey
        md5msg = hashlib.md5(smsg.encode()).hexdigest()
        response = sqscli.send_message(QueueUrl=sqs_queue_url, MessageBody=smsg, MessageAttributes=attrib)
        if response['MD5OfMessageBody'] != md5msg:
            _logger.error('Send message to sqs , body md5 error!')
        if response['MD5OfMessageAttributes'] != attrib_md5:
            _logger.error('Send message to sqs , attrib md5 error!')
        return response['MessageId']
    else:
        sqscli = boto3.client('sqs')
        response = sqscli.send_message(QueueUrl=sqs_queue_url, MessageBody=smsg)
        if response['MD5OfMessageBody'] != md5msg:
            _logger.error('Send message to sqs , body md5 error!')
        return response['MessageId']


def decode_message(recordlist):
    msglist = []
    for record in recordlist:
        if 's3' in record['messageAttributes']:
            s3cli = boto3.client('s3')
            response = s3cli.get_object(Bucket=record['messageAttributes']['s3']['stringValue'], Key=record['body'])
            binbody = response['Body'].read()
            msg = loads(binbody.decode())
        else:
            msg = loads(record['body'])
        msglist.append(msg)
    return msglist


def clean_message_attachment(recordlist):
    bucket_objs = {}
    for record in recordlist:
        if 's3' in record['messageAttributes']:
            bucket = record['messageAttributes']['s3']['stringValue']
            objkey = record['body']
            if bucket not in bucket_objs:
                bucket_objs[bucket] = [{'Key': objkey}]
            else:
                bucket_objs[bucket].append({'Key': objkey})
    s3cli = boto3.client('s3')
    for bucket in bucket_objs:
        resp = s3cli.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': bucket_objs[bucket],
                'Quiet': True
            })
        if 'Errors' in resp:
            for errinfo in resp['Errors']:
                _logger.error('S3 DELETE: %s ,code=%s,msg=%s', errinfo['Key'], errinfo['Code'], errinfo['Message'])


_cached_templates = {}


def _get_template(template, base_name, task_name, task_id, func_id, concurrent, event_enabled, event_batch,
                  runtime_memory, msg_max_retry):
    if template not in _cached_templates:
        data = pkgutil.get_data(__package__, template)
        _cached_templates[template] = Template(data.decode('utf-8'))
    stscli = boto3.client('sts')
    aws_account = stscli.get_caller_identity().get('Account')
    aws_region = boto3._get_default_session().region_name
    kwargs = {
        'aws_account': aws_account,
        'aws_region': aws_region,
        'base_name': base_name,
        'task_name': task_name,
        'task_id': "0" if task_id is None else task_id,
        'func_id': func_id,
        'concurrent': concurrent,
        'event_enabled': "true" if event_enabled else "false",
        'event_batch': event_batch,
        'runtime_memory': runtime_memory,
        'msg_max_retry': msg_max_retry,
    }
    data = _cached_templates[template].substitute(kwargs)
    return yaml.load(data)


def creat_awsenv(tmpl):
    sqscli = boto3.client('sqs')

    resp = sqscli.create_queue(**tmpl['sqs_queue_dead_letter'])
    queue_dl_url = resp['QueueUrl']
    sqscli.tag_queue(QueueUrl=queue_dl_url, Tags=tmpl['sqs_queue_tags'])

    resp = sqscli.create_queue(**tmpl['sqs_queue'])
    queue_url = resp['QueueUrl']
    sqscli.tag_queue(QueueUrl=queue_url, Tags=tmpl['sqs_queue_tags'])

    lambcli = boto3.client('lambda')
    try:
        lambcli.create_function(**tmpl['lambda_func'])
        if tmpl['lambda_func_concurrency']['ReservedConcurrentExecutions'] is None:
            lambcli.delete_function_concurrency(FunctionName=tmpl['lambda_func']['FunctionName'])
        else:
            lambcli.put_function_concurrency(**tmpl['lambda_func_concurrency'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceConflictException':
            raise
    try:
        lambcli.create_event_source_mapping(**tmpl['event_mapping'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceConflictException':
            raise
        reupdate = True
    else:
        reupdate = False
    if reupdate:
        try:
            resp = lambcli.list_event_source_mappings(EventSourceArn=tmpl['event_mapping']['EventSourceArn'],
                                                      FunctionName=tmpl['event_mapping']['FunctionName'])
            lambcli.update_event_source_mapping(
                UUID=resp['EventSourceMappings'][0]['UUID'],
                FunctionName=tmpl['event_mapping']['FunctionName'],
                Enabled=tmpl['event_mapping']['Enabled'],
                BatchSize=tmpl['event_mapping']['BatchSize']
            )
        except ClientError:
            pass
    return queue_url


def remove_awsenv(tmpl, clean_func):
    lambcli = boto3.client('lambda')

    resp = lambcli.list_event_source_mappings(
        EventSourceArn=tmpl['event_mapping']['EventSourceArn'],
        FunctionName=tmpl['event_mapping']['FunctionName'],
    )
    if resp['EventSourceMappings']:
        for mapping in resp['EventSourceMappings']:
            try:
                lambcli.delete_event_source_mapping(UUID=mapping['UUID'])
            except ClientError as e:
                code = e.response['Error']['Code']
                if code != 'ResourceNotFoundException' and code != 'ResourceInUseException':
                    _logger.exception('lambda: delete_event_source_mapping')

    if clean_func:
        try:
            lambcli.delete_function(FunctionName=tmpl['lambda_func']['FunctionName'])
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                _logger.exception('lambda: delete_function: %s', tmpl['lambda_func']['FunctionName'])

    sqscli = boto3.client('sqs')
    try:
        resp = sqscli.get_queue_url(QueueName=tmpl['sqs_queue']['QueueName'])
        sqscli.delete_queue(QueueUrl=resp['QueueUrl'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'AWS.SimpleQueueService.NonExistentQueue':
            _logger.exception('sqs: %s', tmpl['sqs_queue']['QueueName'])

    try:
        resp = sqscli.get_queue_url(QueueName=tmpl['sqs_queue_dead_letter']['QueueName'])
        sqscli.delete_queue(QueueUrl=resp['QueueUrl'])
    except ClientError as e:
        if e.response['Error']['Code'] != 'AWS.SimpleQueueService.NonExistentQueue':
            _logger.exception('sqs: %s', tmpl['sqs_queue_dead_letter']['QueueName'])


def check_awsenv(tmpl):
    sqscli = boto3.client('sqs')
    try:
        resp = sqscli.get_queue_url(QueueName=tmpl['sqs_queue']['QueueName'])
        queue_url = resp['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            queue_url = creat_awsenv(tmpl)
        else:
            raise
    return queue_url, tmpl['s3_bucket'], tmpl['s3_key_prefix'], tmpl['s3_obj_tags']
