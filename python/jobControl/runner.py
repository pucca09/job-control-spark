# -*- coding: UTF-8 -*-

import os
import subprocess
from util import project_dir_manager, hdfs_util
class SparkJobException(Exception):
    pass

class SparkJob(object):
    def __init__(self, **params):
        self.sparkContext = SparkContext(**params)

    def run(self):
        run(self.sparkContext)


class JavaJob(object):
    def __init__(self, **params):
        self.Context = JavaContext(**params)

    def run(self):
        run(self.Context)


class JavaContext(object):
    def __init__(self, **params):
        self.params = None
        self.jar = None
        self.main_class = None
        self.mode = 'java'
        self.__dict__.update(params)


class SparkContext(object):
    def __init__(self, **params):
        self.spark_home = None                          # jobControl home path
        self.executor_memory = None                     # executor内存大小， 默认1G
        self.total_executor_cores = None                # executor使用的总核数 默认1核
        self.master = None                              # master url 默认是本地local[1]
        self.driver_memory = None                       # Driver程序使用内存大小 默认512m
        self.main_class = None                           # Driver类名，包含包名称
        self.jars = []                                  # Driver依赖的第三方jar包
        self.driver = None                              # Driver jar包
        self.params = None                              # Driver依赖参数
        self.files = None                               # 用逗号隔开的要放置在每个executor工作目录的文件列表
        self.mode = 'spark'
        self.input_path = None
        self.output_path = None
        self.__dict__.update(params)


def run(jobContext):
    #assert 'mode' in jobContext.__dict__, "the job_context's mode cannot be null"
    if jobContext.mode == 'spark':
        __run_remote_job(jobContext)
    elif jobContext.mode == 'java':
        __run_java_job(jobContext)


def __run_java_job(jobContext):
    # commandList = list()
    # commandList.append('java -jar')
    # if jobContext.params:
    pass

def __run_remote_job(jobContext):
    if not jobContext.spark_home:
        spark_submit = os.path.join(os.environ.get('SPARK_HOME'), 'bin', 'spark-submit')
    else:
        spark_submit = os.path.join(jobContext.spark_home, 'bin', 'spark-submit')

    commandList = list()

    if not jobContext.master:
        commandList.extend(['--master', 'local[1]'])
    else:
        commandList.extend(['--master', jobContext.master])

    assert jobContext.main_class, 'the Main Class is Null'
    commandList.extend(['--class', jobContext.main_class])
    if not jobContext.executor_memory:
        commandList.extend(['--executor-memory', '1g'])
    else:
        commandList.extend(['--executor-memory', jobContext.executor_memory])

    if not jobContext.total_executor_cores:
        commandList.extend(['--total-executor-cores', '1'])
    else:
        commandList.extend(['--total-executor-cores', jobContext.total_executor_cores])

    if not jobContext.driver_memory:
        commandList.extend(['--driver-memory', '512m'])
    else:
        commandList.extend(['--driver-memory', jobContext.driver_memory])

    if jobContext.jars:
        commandList.extend(['--jars', jobContext.jars])

    if jobContext.files:
        commandList.extend(['--files', jobContext.files])

    assert jobContext.driver, 'the Driver jar is Null'
    commandList.append(os.path.join(project_dir_manager.python_root(), '../jobs/'+jobContext.driver))

    if type(jobContext.input_path) in [list]:
        input_path = jobContext.input_path
    else:
        input_path = [jobContext.input_path]

    for input_dir in input_path:
        if not hdfs_util.exist(input_dir):
            print >> sys.stderr, 'the %s is not existed!' %input_dir
            sys.exit(-1)
    
    output_path = jobContext.output_path
    if hdfs_util.exist(output_path):
        print 'the %s is existed!'  %output_path
        hdfs_util.delete(output_path)

    commandList.extend(input_path)
    commandList.append(output_path)

    if jobContext.params:
        commandList.extend(jobContext.params)
    command = [spark_submit]
    command.extend(commandList)
    print '--------------command-----------------'
    print  ' '.join(command)
    print '--------------------------------------'
    command_str = ' '.join(command)
    #subprocess.call(command_str, shell=True)
    p =  subprocess.Popen(command_str,
                          shell=True,
                          stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE)
    output, error = p.communicate()
    print '-------------communicate---------------'
    print output
    print '---------------------------------------'   
    if p.poll() != 0:
            raise SparkJobException, 'spark job fail'
    return output, error



if __name__ == '__main__':
    print project_dir_manager.working_root()
    # SparkJob(mainClass = 'wtist.rowcount', driver= '/extdisk/wtist/tele/LingFeng/FeatureExtract/RowsCount-1.0-SNAPSHOT.jar', params=['/user/tele/hainan/location/jizhan']).run()











