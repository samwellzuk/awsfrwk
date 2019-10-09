# -*-coding: utf-8 -*-
# Created by samwell
import atexit
from .dirmgr import _init_tmp_dir, _uninit_tmp_dir

# create temp dir when module load, then clear temp dir when process exit.
_init_tmp_dir()
# atexit, last in first out
atexit.register(_uninit_tmp_dir)

