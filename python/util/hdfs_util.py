# -*- coding: UTF-8 -*-
import os
import subprocess


def exist(path):
    return 0 == int(os.system('hadoop fs -test -e %s' % path))

def mkdir(path):
    if exist(path):
        print 'the path already exist'
        return True
    else:
        return 0 == int(os.system('hadoop fs -mkdir -p %s' % path))

def get_files_by_dir(path):
    assert exits(path), 'File not Found'
    ls = subprocess.Popen(["hadoop", "fs", "-ls", "%s" % path], stdout=subprocess.PIPE)
    files = list()
    for file_name in ls.stdout:
        files.append(file_name)
    return files


def delete(path):
    if exist(path):
        return 0 == int(os.system('hadoop fs -rm -r %s' % path))
    return True

