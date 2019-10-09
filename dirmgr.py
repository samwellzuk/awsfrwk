# -*-coding: utf-8 -*-
# Created by samwell
import os
import os.path
import shutil
import tempfile

# aws-lambda serverless evironment start to run, it will start a process, and receve message and execute function with the message. then do that again until no message.
# it will use same process receve many messages, so module only load once at start. 
tmp_dir = os.path.join(tempfile.gettempdir(), 'awstmp%d' % os.getpid())
reflect_dir = os.path.join(tmp_dir, 'aws_reflect')
runtime_dir = os.path.join(tmp_dir, 'aws_runtime')
other_dir = os.path.join(tmp_dir, 'aws_other')


def clear_tmp_dir(cleanse=False):
    if os.path.isdir(tmp_dir):
        if cleanse:
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass
        else:
            for pname in os.listdir(path=tmp_dir):
                if pname == 'aws_reflect':
                    continue
                dirname = os.path.join(tmp_dir, pname)
                try:
                    if os.path.isdir(dirname):
                        shutil.rmtree(dirname, ignore_errors=True)
                        os.makedirs(dirname, exist_ok=True)
                    else:
                        os.remove(dirname)
                except Exception:
                    pass


def _init_tmp_dir():
    clear_tmp_dir(True)
    os.makedirs(reflect_dir, exist_ok=True)
    os.makedirs(runtime_dir, exist_ok=True)
    os.makedirs(other_dir, exist_ok=True)


def _uninit_tmp_dir():
    clear_tmp_dir(True)
