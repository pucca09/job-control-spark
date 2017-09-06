# -*- coding: UTF-8 -*-

import os


def working_root():
    return os.path.join(os.path.dirname(__file__), '../..')

def python_root():
    return os.path.join(working_root(), 'python')

def src_root():
    return os.path.join(working_root(), 'src')

def bin_root():
    return os.path.join(working_root(), 'bin')

def dependency_root():
    return os.path.join(working_root(), 'dependency')

def data_root():
    return os.path.join(working_root(), 'data')

def conf_root():
    return os.path.join(working_root(), 'conf')

def lib_root():
    return os.path.join(working_root(), 'lib')

def scala_root():
    return os.path.join(working_root(), 'java')

def job_root():
    return os.path.join(working_root(), 'jobs')


