# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util
#import sys
import os
Usage = """
    run_de_oscillation.py configfile DeOscillation
"""


def run(args):
    #print args

    assert_message.assert_message(len(args) == 3, Usage)
    conf_file = args[1]
    conf = conf_parser.ConfParser(conf_file)

    conf.load(args[2])
    user_type = conf.get('user_type')
    assert_message.assert_message(user_type, 'user type cannot be null')
    assert_message.assert_message(conf.get('month'), 'month cannot be null')
    assert_message.assert_message(conf.get('province'), 'province cannot be null')
    assert_message.assert_message(conf.get('input_dir'), 'input dir cannot be null')
    assert_message.assert_message(conf.get('output_dir'), 'output dir cannot be null')
    #input_dir = os.path.join(conf.get('input_dir'), conf.get('province'), conf.get('month'), '%sDataClean%s' % (conf.get('month'), user_type))
    output_dir = os.path.join(conf.get('output_dir'),conf.get('province'), conf.get('month'))
    cluster = conf.load_to_dict('cluster')

    # Stable Point	
    stable_input = os.path.join(conf.get('input_dir'), conf.get('province'), conf.get('month'), '%sDataClean%s' % (conf.get('month'), user_type),'%sTotal%s.csv' % (conf.get('month'), user_type))
    stable_output = os.path.join(output_dir, '%sStable%s.csv' % (conf.get('month'), user_type))
    stable_params = list()
    stable_params.append(conf.load_to_dict('StablePoint').get('oscillation.stable.point.time.threshold', '15'))
    stable_params.append(stable_input)
    if hdfs_util.exits(stable_output):
        print 'the stable_output is existed!'
        hdfs_util.delete(stable_output)
    stable_params.append(stable_output)
    cluster['params'] = stable_params
    cluster['main_class'] = 'wtist.driver.preprocess.deOscillation.StablePointDriver'
    cluster['driver'] = 'Mobility-1.0.jar'
    #print cluster
    stable_task = runner.SparkJob(**cluster)
    #print stable_task.sparkContext.__dict__
    stable_task.run()




if __name__=='__main__':
    run(sys.argv)
