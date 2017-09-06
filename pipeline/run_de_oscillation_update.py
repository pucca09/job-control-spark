# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util, option_util
import os


def run(args):
    conf_file = args['conf']
    conf = conf_parser.ConfParser(conf_file)
    conf.load('DeOscillation')            # 加载去震荡默认的参数配置模块
    month = args['month']
    if not month:
        if conf.has('month'):
            month = conf.get('month')
        else:
            assert False, 'the month is not setted'
    
    province = args['province']
    if not province:
        if conf.has('province'):
            province = conf.get('province')
        else:
            assert False, 'the province is not setted'
    
    user_type = ''
    if conf.has('user_type'):
        user_type = conf.get('user_type')
    if args['params']:
        user_type = args['params'][0]  # 若user_type 采用传参的话，必须把user_type放在无参数名的第一位
    if not user_type:
        assert False, 'the user_type is not setted'
    
    input_dir = args['input']
    if not input_dir:
        input_dir = os.path.join(conf.get('root_dir'), 'DataClean', province, month, '%s%s' %(month, user_type))
    
    output_dir = args['output']
    if not output_dir:
        output_dir = os.path.join(conf.get('root_dir'), 'DeOscillation', province, month)
    cluster = conf.load_to_dict('cluster')

    # Stable Point
    print 'stable point start'	
    stable_input = os.path.join(input_dir,'%sTotal%s.csv' % (month, user_type))
    print stable_input
    stable_output = os.path.join(output_dir, '%sStable%s.csv' % (month, user_type))
    stable_params = list()
    stable_params.append(conf.load_to_dict('StablePoint').get('oscillation.stable.point.time.threshold', '15'))
    if not hdfs_util.exist(stable_input):
       print >> sys.stderr, 'the stable_input is not existed!'
       sys.exit(-1) 
    stable_params.append(stable_input)
    if hdfs_util.exist(stable_output):
       print 'the stable_output is existed!'
       hdfs_util.delete(stable_output)
    stable_params.append(stable_output)
    cluster['params'] = stable_params
    cluster['main_class'] = conf.load_to_dict('StablePoint').get('main_class')
    cluster['driver'] = conf.load_to_dict('StablePoint').get('driver')
    stable_task = runner.SparkJob(**cluster)
    stable_task.run()
    print 'stable point end'

    # Rule1
    print 'rule1 start'
    rule1_input = stable_output
    rule1_output = os.path.join(output_dir, '%sRule1%s.csv' %(month, user_type))
    rule1_params = list()
    rule1_params.append(conf.load_to_dict('Rule1').get('oscillation.rule1.time.threshold', '2'))
    #rule1_params.append(rule1_input)
    if not hdfs_util.exist(rule1_input):
       print >> sys.stderr, 'the rule1_input is not existed!'
       sys.exit(-1) 
    rule1_params.append(rule1_input)
    if hdfs_util.exist(rule1_output):
       print 'the rule1_output is existed!'
       hdfs_util.delete(rule1_output)
    rule1_params.append(rule1_output)
    cluster['params'] = rule1_params
    cluster['main_class'] = conf.load_to_dict('Rule1').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule1').get('driver')
    rule1_task = runner.SparkJob(**cluster)
    rule1_task.run()
    print 'rule1 end'

    # Rule2
    print 'rule2 start'
    rule2_input = rule1_output
    rule2_output = os.path.join(output_dir, '%sRule2%s.csv' %(month, user_type))
    rule2_params = list()
    rule2_params.append(conf.load_to_dict('Rule2').get('oscillation.rule2.time.threshold', '1'))
    rule2_params.append(conf.load_to_dict('Rule2').get('oscillation.rule2.distance.threshold', '10'))
    #rule2_params.append(rule2_input)
    if not hdfs_util.exist(rule2_input):
       print >> sys.stderr, 'the rule2_input is not existed!'
       sys.exit(-1) 
    rule2_params.append(rule2_input)
    if hdfs_util.exist(rule2_output):
       print 'the rule2_output is existed!'
       hdfs_util.delete(rule2_output)
    rule2_params.append(rule2_output)
    cluster['params'] = rule2_params
    cluster['main_class'] = conf.load_to_dict('Rule2').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule2').get('driver')
    rule2_task = runner.SparkJob(**cluster)
    rule2_task.run()
    print 'rule2 end'
    
    # Rule3
    print 'rule3 start'
    rule3_input = rule2_output
    rule3_output = os.path.join(output_dir, '%sRule3%s.csv' %(month, user_type))
    rule3_params = list()
    rule3_params.append(conf.load_to_dict('Rule3').get('oscillation.rule3.speed.threshold', '250'))
    rule3_params.append(conf.load_to_dict('Rule3').get('oscillation.rule3.distance.threshold', '50'))
    #rule3_params.append(rule3_input)
    if not hdfs_util.exist(rule3_input):
       print >> sys.stderr, 'the rule3_input is not existed!'
       sys.exit(-1) 
    rule3_params.append(rule3_input)
    if hdfs_util.exist(rule3_output):
       print 'the rule3_output is existed!'
       hdfs_util.delete(rule3_output)
    rule3_params.append(rule3_output)
    cluster['params'] = rule3_params
    cluster['main_class'] = conf.load_to_dict('Rule3').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule3').get('driver')
    rule3_task = runner.SparkJob(**cluster)
    rule3_task.run()
    print 'rule3 end'

    
    # Rule4
    print 'rule4 start'
    rule4_input = rule3_output
    rule4_output = os.path.join(output_dir, '%sRule4%s.csv' %(month, user_type))
    rule4_params = list()
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.time.threshold', '60'))
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.count.threshold', '3'))
    rule4_params.append(conf.load_to_dict('Rule4').get('oscillation.rule4.uniq.count.threshold', '2'))
    #rule4_params.append(rule4_input)
    if not hdfs_util.exist(rule4_input):
       print >> sys.stderr, 'the rule4_input is not existed!'
       sys.exit(-1) 
    rule4_params.append(rule4_input)
    if hdfs_util.exist(rule4_output):
       print 'the rule4_output is existed!'
       hdfs_util.delete(rule4_output)
    rule4_params.append(rule4_output)
    cluster['params'] = rule4_params
    cluster['main_class'] = conf.load_to_dict('Rule4').get('main_class')
    cluster['driver'] = conf.load_to_dict('Rule4').get('driver')
    rule4_task = runner.SparkJob(**cluster)
    rule4_task.run()
    print 'rule4 end'



if __name__=='__main__':
    run(option_util.get_option(sys.argv))
