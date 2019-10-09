# -*-coding: utf-8 -*-
# Created by samwell

import os
import sys
import inspect
import logging

from importlib import import_module, invalidate_caches
from pathlib import Path
from zipfile import PyZipFile
from tempfile import TemporaryFile, mkstemp

import re
from zipimport import zipimporter

from .dirmgr import reflect_dir, runtime_dir

_logger = logging.getLogger(__name__)
_pk_re = re.compile(r'aws_pk_.*\.zip')


def check_func(func, kwonly_parms, exclude_parms):
    if not inspect.isfunction(func):
        raise RuntimeError('Object [%r]: must be function' % func)
    result = inspect.getfullargspec(func)
    for p in kwonly_parms:
        if p not in result.kwonlyargs:
            raise RuntimeError('Function %s: must have "%s" as keyword only parameter' % (func.__name__, p))
    for p in exclude_parms:
        if p in result.args or p in result.kwonlyargs:
            raise RuntimeError('Function %s: can not have "%s" in parameter' % (func.__name__, p))


def _check_func_from_zip(mod):
    if isinstance(mod.__loader__, zipimporter):
        return True
    if not os.path.isfile(mod.__file__):
        m = _pk_re.search(mod.__file__)
        if m:
            return True
    return False


def _print_module_info(mod):
    _logger.error('__name__: %r' % mod.__name__)
    _logger.error('__file__: %r' % mod.__file__)
    _logger.error('__loader__: %r' % mod.__loader__)
    _logger.error('__package__: %r' % mod.__package__)
    _logger.error('__spec__: %r' % mod.__spec__)


def dump_func(func):
    mod = inspect.getmodule(func)
    mod_parts = []
    mod_parts.append(mod.__package__ if mod.__package__ else '')
    if mod.__name__ == '__main__':
        srcfile = Path(os.path.abspath(inspect.getsourcefile(mod)))
        mod_parts.append(srcfile.stem)
    else:
        mod_parts.append(mod.__name__)
    mod_parts.append(func.__qualname__)
    func_bin = '|'.join(mod_parts)
    if _check_func_from_zip(mod):
        m = _pk_re.search(mod.__file__)
        if not m:
            raise RuntimeError('Modele from zip import, can not find aws package')
        pkfile = mod.__file__[:m.end()]
        with open(pkfile, 'rb') as infile:
            func_content = infile.read()
    elif mod.__package__ and (mod.__package__ == 'awsfrwk' or mod.__package__.startswith('awsfrwk.')):
        # be called in local
        func_content = None
    else:
        # package file and dir
        srcfile = Path(os.path.abspath(inspect.getsourcefile(mod)))
        curdir = Path(os.path.abspath('.'))
        try:
            reldir = srcfile.relative_to(curdir)
        except ValueError:
            raise RuntimeError('Function %s: source file must in current dir' % func.__name__)

        targetpath = curdir.joinpath(reldir.parts[0])

        def _enum_dir(p, zipf):
            for sp in p.iterdir():
                if sp.is_dir():
                    _enum_dir(sp, zipf)
                    continue
                suffix = sp.suffix.lower()
                if suffix != '.py' and suffix != '.pyc' and suffix != '.pyo':
                    relp = sp.relative_to(targetpath.parent)
                    zipf.write(str(sp), str(relp))

        with TemporaryFile(dir=runtime_dir) as tmpf:
            with PyZipFile(tmpf, "w") as zipfp:
                zipfp.writepy(str(targetpath))
                if targetpath.is_dir():
                    _enum_dir(targetpath, zipfp)
            tmpf.seek(0)
            func_content = tmpf.read()
    return func_bin, func_content


def load_func(func_bin, func_content):
    if func_content is not None:
        fname = os.path.join(reflect_dir, 'aws_pk_%s.zip' % func_bin.replace('|', '-'))
        # if find packeage file , mean this call isn't first time. do not insert path to system
        if not os.path.isfile(fname):
            with open(fname, 'wb') as of:
                of.write(func_content)
            sys.path.insert(0, fname)
    func_parts = func_bin.split('|')
    if len(func_parts) != 3:
        raise RuntimeError('func_bin %r format error!' % func_bin)
    package = func_parts[0]
    modname = func_parts[1]
    funcname = func_parts[2]
    mod = import_module(modname, package=package)
    parts = funcname.split('.')
    func = mod
    for p in parts:
        func = getattr(func, p)
    return func

